// +build integration

package kafkaconsumer

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.internal.digitalocean.com/observability/firebolt"

	"github.internal.digitalocean.com/observability/firebolt/config"
	"github.internal.digitalocean.com/observability/firebolt/message"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.internal.digitalocean.com/observability/firebolt/fbcontext"
	"github.internal.digitalocean.com/observability/firebolt/metrics"
	"github.internal.digitalocean.com/observability/firebolt/node/kafkaproducer"
	"github.internal.digitalocean.com/observability/firebolt/util"
)

// kafkaconsumer should sync its partition assignments to the recoveryconsumer (if enabled)
func TestRecoveryConsumerPartitionAssignment(t *testing.T) {
	metrics.Init("recoveryconsumer")

	topicName := fmt.Sprintf("recoveryconsumer-partitionassignment-%d", time.Now().UnixNano())
	println("recoveryconsumer partition assignment integration test using topic " + topicName)

	kc, err := startKafkaConsumerWithMockContext(topicName, 500, 200, t)
	assert.Nil(t, err)

	// assign all 4 partitions (because this is the only consumer, they will all get assigned anyway after several seconds)
	tp1 := kafka.TopicPartition{
		Topic:     &topicName,
		Partition: 0,
		Offset:    kafka.Offset(0),
	}
	tp2 := kafka.TopicPartition{
		Topic:     &topicName,
		Partition: 1,
		Offset:    kafka.Offset(0),
	}
	tp3 := kafka.TopicPartition{
		Topic:     &topicName,
		Partition: 2,
		Offset:    kafka.Offset(0),
	}
	tp4 := kafka.TopicPartition{
		Topic:     &topicName,
		Partition: 3,
		Offset:    kafka.Offset(0),
	}
	err = kc.assignPartitions([]kafka.TopicPartition{tp1, tp2, tp3, tp4})
	assert.Nil(t, err)

	// the recoverytracker doesn't have any recoveryrequests yet, so no partitions should have been assigned in recoveryconsumer
	assert.NotNil(t, kc.recoveryConsumer)
	assert.Nil(t, kc.recoveryConsumer.activePartitionMap)

	// create recoveryrequests for 3 of 4 partitions
	kc.recoveryConsumer.tracker.AddRecoveryRequest(0, 0, 100)
	kc.recoveryConsumer.tracker.AddRecoveryRequest(1, 0, 100)
	kc.recoveryConsumer.tracker.AddRecoveryRequest(3, 0, 100)

	// wait for a sec for recoveryTracker to consume those recoveryrequests
	time.Sleep(25 * time.Second)
	assert.Equal(t, 3, kc.recoveryConsumer.tracker.RecoveryRequestCount())

	// test the assignment now that there are matching recoveryrequests
	err = kc.assignPartitions([]kafka.TopicPartition{tp1, tp2, tp3})
	assert.Nil(t, err)

	// make sure the recoveryconsumer was assigned 0, 1, 3
	assert.NotNil(t, kc.recoveryConsumer)
	assert.NotNil(t, kc.recoveryConsumer.activePartitionMap)
	assert.Equal(t, 3, len(kc.recoveryConsumer.activePartitionMap))
	assert.Equal(t, int32(0), kc.recoveryConsumer.activePartitionMap[0].partition.Partition)
	assert.Equal(t, int32(1), kc.recoveryConsumer.activePartitionMap[1].partition.Partition)
	assert.Equal(t, int32(3), kc.recoveryConsumer.activePartitionMap[3].partition.Partition)

	// revoke assignments
	kc.revokePartitionAssignments()

	// make sure the recoveryconsumer was kept in-sync
	assert.NotNil(t, kc.recoveryConsumer)
	assert.Equal(t, 0, len(kc.recoveryConsumer.activePartitionMap))

	// the shutdown can hang if there are any handles on TopicPartitions in kafkaconsumer
	kc.revokePartitionAssignments()

	err = kc.Shutdown()
	assert.Nil(t, err)
}

// recoveryconsumer should recover limited data after a simulated outage
// this test depends on topic 'irecoveryconsumer-endtoend' which is created in 'inttest/docker-compose.yml'
func TestRecoveryConsumerEndToEnd(t *testing.T) {
	metrics.Init("recoveryconsumer")

	topicName := fmt.Sprintf("recoveryconsumer-endtoend-%d", time.Now().UnixNano())
	println("recoveryconsumer endtoend assignment integration test using topic " + topicName)

	// first we will produce 2000 records to create a recovery state where the kafkaconsumer is 'behind'
	// the kafkaconsumer is set up with 'maxpartitionlag' 60 * 4 partitions, so 240 records will be consumed immediately in "real time" by the main consumer
	// then it should request recovery for all 4 partitions, and with 'parallelrecoverymaxrecords' = 200 it should recover 800 records
	// create a producer for test data
	produceTestRecords(topicName, 2000, t)

	// sleep to give kafka time to autocreate the topic and have a leader for it's partition, also without this sometimes
	// it's too fast and kafka returns 0 for highwatermark, which results in a false-failed test
	time.Sleep(5 * time.Second)

	// start the kafkaconsumer; it should initiate recovery automatically
	kc, err := startKafkaConsumerWithMockContext(topicName, 500, 200, t)
	assert.Nil(t, err)
	assert.Len(t, kc.sendCh, 0)

	// we expect recoveryTracker to get recoveryrequests for all 4 partitions
	_ = util.AwaitCondition(func() bool {
		return kc.recoveryConsumer.tracker.RecoveryRequestCount() == 4
	}, 250*time.Millisecond, 20*time.Second)
	assert.Len(t, kc.recoveryConsumer.tracker.recoveryRequests, 4)

	// give the recoveryconsumer some time to do its thing
	_ = util.AwaitCondition(func() bool {
		return len(kc.sendCh) == 1040
	}, 1*time.Second, 60*time.Second)
	assert.Len(t, kc.sendCh, 1040) // (60 * 4) from kafkaconsumer + (200 * 4) from recoveryconsumer

	err = kc.Shutdown()
	assert.Nil(t, err)
}

// recoveryconsumer should work as expected even under a rate limit
func TestRecoveryConsumerRateLimited(t *testing.T) {
	metrics.Init("recoveryconsumer")

	topicName := fmt.Sprintf("recoveryconsumer-ratelimit-%d", time.Now().UnixNano())

	produceTestRecords(topicName, 1000, t)

	// sleep to give kafka time to autocreate the topic and have a leader for it's partition, also without this sometimes
	// it's too fast and kafka returns 0 for highwatermark, which results in a false-failed test
	time.Sleep(15 * time.Second)

	// start the kafkaconsumer; it should initiate recovery automatically
	kc, err := startKafkaConsumerWithMockContext(topicName, 10, 80, t)
	assert.Nil(t, err)
	assert.Len(t, kc.sendCh, 0)

	// we expect recoveryTracker to get recoveryrequests for all 4 partitions
	_ = util.AwaitCondition(func() bool {
		return kc.recoveryConsumer.tracker.RecoveryRequestCount() == 4
	}, 250*time.Millisecond, 20*time.Second)
	assert.Len(t, kc.recoveryConsumer.tracker.recoveryRequests, 4)

	// give the recoveryconsumer some time to do its thing
	_ = util.AwaitCondition(func() bool {
		return len(kc.sendCh) == 560
	}, 1*time.Second, 40*time.Second)
	assert.Len(t, kc.sendCh, 560) // (60 * 4) from kafkaconsumer + (80 * 4) from recoveryconsumer

	err = kc.Shutdown()
	assert.Nil(t, err)
}

// recoveryconsumer should handle restarts and rebalances during recovery
func TestRecoveryConsumerRestart(t *testing.T) {
	metrics.Init("recoveryconsumer")

	eventTopicName := fmt.Sprintf("recoveryconsumer-restart-%d", time.Now().UnixNano())
	messageTopicName := fmt.Sprintf("recoveryconsumer-messages-%d", time.Now().UnixNano())
	println("recoveryconsumer endtoend assignment integration test using event topic " + eventTopicName)

	// first we will produce 2000 records to create a recovery state where the kafkaconsumer is 'behind'
	// the kafkaconsumer is set up with 'maxpartitionlag' 60 * 4 partitions, so 240 records will be consumed immediately in "real time" by the main consumer
	// then it should request recovery for all 4 partitions, and with 'parallelrecoverymaxrecords' = 200 it should recover 800 records
	// create a producer for test data
	produceTestRecords(eventTopicName, 10000, t)

	// sleep to give kafka time to autocreate the topic and have a leader for it's partition, also without this sometimes
	// it's too fast and kafka returns 0 for highwatermark, which results in a false-failed test
	time.Sleep(15 * time.Second)

	kc, err := startKafkaConsumerWithRealContext(eventTopicName, messageTopicName, 500, 2000, t)
	assert.Nil(t, err)
	assert.Len(t, kc.sendCh, 0)

	// we expect recoveryTracker to get recoveryrequests for all 4 partitions
	_ = util.AwaitCondition(func() bool {
		return kc.recoveryConsumer.tracker.RecoveryRequestCount() == 4
	}, 250*time.Millisecond, 20*time.Second)
	assert.Len(t, kc.recoveryConsumer.tracker.recoveryRequests, 4)

	// stop mid-recovery; any number of records (beyond the 240 that kafkaconsumer will get on its own) is good
	println("waiting for recovery to start, then shutting it down")
	_ = util.AwaitCondition(func() bool {
		return len(kc.sendCh) > 240
	}, 250*time.Millisecond, 20*time.Second)
	_ = kc.Shutdown()
	fmt.Printf("stopped initial consumer with %d records recovered\n", len(kc.sendCh))

	// start a new consumer, which should resume recovery; up to 10 seconds of records may be double-recovered
	previouslyRecovered := len(kc.sendCh)
	kc2, err := startKafkaConsumerWithRealContext(eventTopicName, messageTopicName, 500, 2000, t)
	assert.Nil(t, err)

	// again, we expect recoveryTracker to resume recoveryrequests for all 4 partitions
	_ = util.AwaitCondition(func() bool {
		return kc2.recoveryConsumer.tracker.RecoveryRequestCount() == 4
	}, 250*time.Millisecond, 20*time.Second)
	assert.Len(t, kc2.recoveryConsumer.tracker.recoveryRequests, 4)

	fmt.Printf("previously recovered %d newly recovered %d\n", previouslyRecovered, len(kc2.sendCh))
	// give the recoveryconsumer some time to do its thing
	_ = util.AwaitCondition(func() bool {
		return (previouslyRecovered + len(kc2.sendCh)) >= 8240
	}, 1*time.Second, 60*time.Second)
	assert.True(t, 8240 <= previouslyRecovered+len(kc2.sendCh), "expected the number of records recovered to be > 8240 but it was %d", previouslyRecovered+len(kc2.sendCh)) // (60 * 4) from kafkaconsumer + (2000 * 4) from recoveryconsumer

	err = kc2.Shutdown()
	assert.Nil(t, err)
}

// recoveryconsumer stop recovery when a cancellation message is received
func TestRecoveryCancellation(t *testing.T) {
	metrics.Init("recoveryconsumer")

	eventTopicName := fmt.Sprintf("recoveryconsumer-restart-%d", time.Now().UnixNano())
	messageTopicName := fmt.Sprintf("recoveryconsumer-messages-%d", time.Now().UnixNano())
	println("recoveryconsumer recovery cancellation integration test using event topic " + eventTopicName)

	// first we will produce 10000 records to create a recovery state where the kafkaconsumer is 'behind'
	// the kafkaconsumer is set up with 'maxpartitionlag' 60 * 4 partitions, so 240 records will be consumed immediately in "real time" by the main consumer
	// then it should request recovery for all 4 partitions, and with 'parallelrecoverymaxrecords' = 200 it should recover 800 records
	// create a producer for test data
	produceTestRecords(eventTopicName, 10000, t)

	// sleep to give kafka time to autocreate the topic and have a leader for it's partition, also without this sometimes
	// it's too fast and kafka returns 0 for highwatermark, which results in a false-failed test
	time.Sleep(5 * time.Second)

	kc, err := startKafkaConsumerWithRealContext(eventTopicName, messageTopicName, 500, 2000, t)
	assert.Nil(t, err)
	assert.Len(t, kc.sendCh, 0)

	// we expect recoveryTracker to get recoveryrequests for all 4 partitions
	_ = util.AwaitCondition(func() bool {
		return kc.recoveryConsumer.tracker.RecoveryRequestCount() == 4
	}, 250*time.Millisecond, 20*time.Second)
	assert.Len(t, kc.recoveryConsumer.tracker.recoveryRequests, 4)

	// wait for the same amount of time as in the test above
	println("waiting for recovery to start before sending cancellation message")
	_ = util.AwaitCondition(func() bool {
		return len(kc.sendCh) > 240
	}, 250*time.Millisecond, 20*time.Second)

	// cancel recovery by sending a message
	err = message.GetSender().Send(message.Message{
		MessageType: "recoverycancelall",
	})
	assert.Nil(t, err)

	// wait for the number of messages to stop increasing over 3 seconds
	numRecovered := len(kc.sendCh)
	time.Sleep(3 * time.Second)
	_ = util.AwaitCondition(func() bool {
		fmt.Printf("waiting for recovery to stop, was %d now %d\n", numRecovered, len(kc.sendCh))
		if numRecovered == len(kc.sendCh) {
			return true
		}
		numRecovered = len(kc.sendCh)
		return false
	}, 3*time.Second, 30*time.Second)

	// the total # of records should NOT have been recovered; cancellation should have stopped it short
	assert.True(t, len(kc.sendCh) < 8240, "Less than 8240 records should have been recovered, but %d were", len(kc.sendCh))

	err = kc.Shutdown()
	assert.Nil(t, err)
	fmt.Printf("stopped consumer with %d records recovered\n", len(kc.sendCh))
}

func startMessageSenderAndReceiver(t *testing.T, messageTopicName string) message.Receiver {
	c := config.Config{}
	c.InternalData = &config.InternalDataConfig{}
	c.InternalData.Transport = "kafka"
	c.InternalData.Params = make(map[string]string)
	c.InternalData.Params["brokers"] = "localhost:9092"
	c.InternalData.Params["messagetopic"] = messageTopicName

	message.InitKafkaSender(*c.InternalData)
	receiver, err := message.NewKafkaReceiver(c.InternalData)
	assert.Nil(t, err)

	receiver.Start()

	return receiver
}

func produceTestRecords(topicName string, numRecords int, t *testing.T) {
	kp := &kafkaproducer.KafkaProducer{}
	producerConfig := make(map[string]string)
	producerConfig["brokers"] = "localhost:9092"
	producerConfig["topic"] = topicName
	err := kp.Setup(producerConfig)
	assert.Nil(t, err)
	for i := 0; i < numRecords; i++ {
		kp.Process(&firebolt.Event{
			Payload: []byte(fmt.Sprintf("record number %d", i)),
			Created: time.Now(),
		})
	}
}

// start the kafka consumer with parallelrecoveryenabled and a mock fbcontext
func startKafkaConsumerWithMockContext(topicName string, rps int, recoveryMaxRecords int, t *testing.T) (*KafkaConsumer, error) {
	kc := &KafkaConsumer{}
	consumerConfig := make(map[string]string)
	consumerConfig["brokers"] = "localhost:9092"
	consumerConfig["consumergroup"] = "recoveryconsumer-inttest"
	consumerConfig["topic"] = topicName
	consumerConfig["buffersize"] = "100"
	consumerConfig["maxpartitionlag"] = "60"
	consumerConfig["parallelrecoveryenabled"] = "true"
	consumerConfig["parallelrecoverymaxrecords"] = strconv.Itoa(recoveryMaxRecords)
	consumerConfig["parallelrecoverymaxrate"] = strconv.Itoa(rps)
	recordsCh := make(chan firebolt.Event, 100000)

	mockContext := &fbcontext.MockFBContext{}
	mockContext.On("SendMessage", mock.MatchedBy(expectedMsgMatcher)).Return(nil)

	kc.Init("myid", mockContext)
	err := kc.Setup(consumerConfig, recordsCh)
	assert.Nil(t, err)
	go kc.Start()
	return kc, err
}

// start the kafka consumer with parallelrecoveryenabled
func startKafkaConsumerWithRealContext(topicName string, messageTopicName string, rps int, recoveryMaxRecords int, t *testing.T) (*KafkaConsumer, error) {
	// build a messaging setup
	receiver := startMessageSenderAndReceiver(t, messageTopicName)
	assert.NotNil(t, receiver)
	receiver.SetNotificationFunc(func(msg message.Message) []error {
		println("RECEIVED MESSAGE: " + msg.MessageType)
		return nil
	})
	sendFunc := func(msg fbcontext.Message) error {
		println("SENDING MESSAGE: " + msg.MessageType)
		err := message.GetSender().Send(message.Message{
			MessageType: msg.MessageType,
			Key:         msg.Key,
			Payload:     msg.Payload,
		})
		assert.Nil(t, err)
		return nil
	}
	ackFunc := func(msg fbcontext.Message) error {
		println("ACKING MESSAGE: " + msg.MessageType)
		// not implemented - this test does not require ACKs
		return nil
	}

	kc := &KafkaConsumer{}
	consumerConfig := make(map[string]string)
	consumerConfig["brokers"] = "localhost:9092"
	consumerConfig["consumergroup"] = "recoveryconsumer-inttest"
	consumerConfig["topic"] = topicName
	consumerConfig["buffersize"] = "100"
	consumerConfig["maxpartitionlag"] = "60"
	consumerConfig["parallelrecoveryenabled"] = "true"
	consumerConfig["parallelrecoverymaxrecords"] = strconv.Itoa(recoveryMaxRecords)
	consumerConfig["parallelrecoverymaxrate"] = strconv.Itoa(rps)
	recordsCh := make(chan firebolt.Event, 100000)

	receiver.SetNotificationFunc(func(msg message.Message) []error {
		println("RECEIVED MESSAGE: " + msg.MessageType)
		err := kc.Receive(fbcontext.Message{
			MessageType: msg.MessageType,
			Key:         msg.Key,
			Payload:     msg.Payload,
		})
		assert.Nil(t, err)
		return nil
	})

	context := fbcontext.NewFBContext(func() string { return "yar" })
	context.ConfigureMessaging(sendFunc, ackFunc)

	kc.Init("myid", context)
	err := kc.Setup(consumerConfig, recordsCh)
	assert.Nil(t, err)
	go kc.Start()
	return kc, err
}
