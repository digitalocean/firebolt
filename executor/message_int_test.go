// +build integration

package executor_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.internal.digitalocean.com/observability/firebolt/config"
	"github.internal.digitalocean.com/observability/firebolt/executor"
	"github.internal.digitalocean.com/observability/firebolt/fbcontext"
	"github.internal.digitalocean.com/observability/firebolt/internal"
	"github.internal.digitalocean.com/observability/firebolt/message"
	"github.internal.digitalocean.com/observability/firebolt/util"
)

func sendMessageViaNode(ex *executor.Executor, messageType string, key string, payload string) {
	filterNode := getFilterNode(ex)

	msg := fbcontext.Message{
		MessageType: messageType,
		Key:         key,
		Payload:     []byte(payload),
	}
	_ = filterNode.SendMessage(msg)
}

func sendMessageViaExecutor(ex *executor.Executor, messageType string, key string, payload string) {
	msg := message.Message{
		MessageType: messageType,
		Key:         key,
		Payload:     []byte(payload),
	}
	_ = ex.SendMessage(msg)
}

func getFilterNode(ex *executor.Executor) *internal.FilterNode {
	filterNodeContext := ex.FindNodeByID("filternode")
	filterNode := filterNodeContext.NodeProcessor.(*internal.FilterNode)
	return filterNode
}

func ackMessage(ex *executor.Executor, messageType string, key string, payload string) {
	filterNode := getFilterNode(ex)

	msg := fbcontext.Message{
		MessageType: messageType,
		Key:         key,
		Payload:     []byte(payload),
	}
	filterNode.AckMessage(msg)
}

func newExecutorForMessageTopic(topicName string) (*executor.Executor, error) {
	// read the config file and update the config to use a unique topic name, which isolates this test from any records left by previous test runs
	executor.RegisterBuiltinSourceTypes()
	executor.RegisterBuiltinNodeTypes()
	c, _ := config.Read("testconfig.yaml")
	c.InternalData.Params["messagetopic"] = topicName

	return executor.New(executor.WithConfig(*c))
}

func TestKafkaMessageTransport(t *testing.T) {
	internal.RegisterTestNodeTypes()

	topicName := fmt.Sprintf("fb-message-inttest-%d", time.Now().UnixNano())
	previousExecutor, err := newExecutorForMessageTopic(topicName)
	assert.Nil(t, err)

	// wait a sec for the executor to be ready
	time.Sleep(3 * time.Second)

	emptyMessageChannel()

	sendMessageViaNode(previousExecutor, "IntTestMessage", "0", "foo")
	sendMessageViaNode(previousExecutor, "OtherMessage", "0", "foo")
	sendMessageViaNode(previousExecutor, "IntTestMessage", "1", "foo")
	sendMessageViaNode(previousExecutor, "IntTestMessage", "2", "foo")
	sendMessageViaNode(previousExecutor, "IntTestMessage", "1", "bar")
	sendMessageViaNode(previousExecutor, "IntTestMessage", "1", "baz")
	ackMessage(previousExecutor, "IntTestMessage", "2", "foo") // ack should prevent receipt of this msg in the new executor
	sendMessageViaNode(previousExecutor, "IntTestMessage", "0", "bar")
	sendMessageViaNode(previousExecutor, "NotSubscribedToMe", "0", "bar") // this message should not be received bc the node does not subscribe to the msgtype

	// sleep to give kafka time to autocreate the topic and have a leader for its partitions
	time.Sleep(5 * time.Second)
	previousExecutor.Shutdown()

	// all 7 of the messages (this excludes the with a msgtype that hasn't been subscribed to, and the ACK) should have been received
	assert.Equal(t, 7, len(internal.FilterNodeMessages))

	// need to *empty* the channel now so that no messages received on 'previousExecutor' pollute the results of the 'real' test
	emptyMessageChannel()

	// the real test begins here; in this executor we expect to get only the latest message for each key from the run above
	// and NOT any ack'd messages NOR any messages that 'filternode' is not subscribed to
	ex, err := newExecutorForMessageTopic(topicName)
	assert.Nil(t, err)

	// executor.New will not return until the messaging framework has initialized and consumed all preexisting messages
	// so we can make some assertions right away
	err = util.AwaitCondition(func() bool {
		return len(internal.FilterNodeMessages) == 3
	}, 250*time.Millisecond, 30*time.Second)
	if err != nil {
		fmt.Printf("failed, only found %d\n", len(internal.FilterNodeMessages))
		t.FailNow()
	}

	assert.Equal(t, 3, len(internal.FilterNodeMessages))
	expectedMessagesFound := 0
	for message := range internal.FilterNodeMessages {
		if message.MessageType == "IntTestMessage" && message.Key == "0" {
			assert.Equal(t, "bar", string(message.Payload))
			expectedMessagesFound++
		}
		if message.MessageType == "IntTestMessage" && message.Key == "1" {
			assert.Equal(t, "baz", string(message.Payload))
			expectedMessagesFound++
		}
		if message.MessageType == "OtherMessage" && message.Key == "0" {
			assert.Equal(t, "foo", string(message.Payload))
			expectedMessagesFound++
		}
		fmt.Printf("found message %d %s %s %s\n", expectedMessagesFound, message.MessageType, message.Key, string(message.Payload))
		if len(internal.FilterNodeMessages) == 0 {
			break
		}
	}
	assert.Equal(t, 3, expectedMessagesFound)

	// send two more messages, this time use the executor
	sendMessageViaExecutor(ex, "IntTestMessage", "0", "foo")
	sendMessageViaExecutor(ex, "IntTestMessage", "0", "bar")

	// technically Execute isn't even required to send messages; included only for the sake of making this a complete/realistic test
	go ex.Execute()

	// let the executor run, then shut it down
	time.Sleep(10 * time.Second)
	ex.Shutdown()

	assert.Equal(t, 2, len(internal.FilterNodeMessages))
	received := <-internal.FilterNodeMessages
	assert.Equal(t, "foo", string(received.Payload))
	received = <-internal.FilterNodeMessages
	assert.Equal(t, "bar", string(received.Payload))
}

func TestNoOpMessageTransport(t *testing.T) {
	internal.RegisterTestNodeTypes()

	emptyMessageChannel()

	ex, err := executor.New(executor.WithConfigFile("testconfig-noMessageTransport.yaml"))
	assert.Nil(t, err)

	sendMessageViaNode(ex, "IntTestMessage", "1", "bar")
	sendMessageViaNode(ex, "IntTestMessage", "1", "baz")
	ackMessage(ex, "IntTestMessage", "2", "foo")

	// none of that should do anything, but it also shouldn't cause any errors
	assert.Equal(t, 0, len(internal.FilterNodeMessages))
}

func emptyMessageChannel() {
	for len(internal.FilterNodeMessages) > 0 {
		msg := <-internal.FilterNodeMessages
		fmt.Printf("received: %s %s %s, remaining: %d\n", msg.Key, msg.MessageType, string(msg.Payload), len(internal.FilterNodeMessages))
	}
}
