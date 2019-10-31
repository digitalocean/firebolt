package executor

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.internal.digitalocean.com/observability/firebolt/config"
	"github.internal.digitalocean.com/observability/firebolt/fbcontext"
	"github.internal.digitalocean.com/observability/firebolt/message"
	"github.internal.digitalocean.com/observability/firebolt/node"
)

// InitMessaging should set up the configured messaging transport, and it MUST ensure that the executor's fbcontext is
// set, but it MUST NOT actually start receiving messages
func (e *Executor) InitMessaging(c config.Config) error {
	// currently 'kafka' is the only implemented message transport
	if c.InternalData != nil && c.InternalData.Transport == config.InternalDataTransportKafka {
		return e.initMessagingKafka(c)
	}

	log.Warn("executor: valid internaldata transport not found, messaging will not be available")
	e.messageReceiver = &NoOpMessageReceiver{}

	return nil
}

// StartMessaging runs the message receiver and blocks until any pending / non-ack'd messages have been processed.
func (e *Executor) StartMessaging() {
	e.messageReceiver.Start()

	// wait for the consumer to be "caught up" before allowing the executor to start running; this ensures that nodes
	// don't see out-of-date messages (that may not have been compacted yet) from earlier in the topic's history
	deadline := time.Now().Add(60 * time.Second)
	for {
		if e.messageReceiver.Initialized() {
			log.Info("executor: kafka receiver initialized, continuing with startup")
			break
		}
		if time.Now().After(deadline) {
			log.Warn("executor: kafka receiver initialization deadline exceeded, check topic configuration to ensure it is compacting")
			break
		}
		log.Info("executor: waiting for kafka receiver to initialize...")
		time.Sleep(300 * time.Millisecond)
	}
}

func (e *Executor) initMessagingKafka(c config.Config) error {
	message.InitKafkaSender(*c.InternalData)
	receiver, err := message.NewKafkaReceiver(c.InternalData)
	if err != nil {
		return fmt.Errorf("executor: failed to initialize message receiver %s", c.InternalData.Transport)
	}
	receiver.SetNotificationFunc(func(msg message.Message) []error {
		return e.deliverMessage(msg)
	})
	e.messageReceiver = receiver

	e.fbContext.ConfigureMessaging(sendMessage, ackMessage)

	return nil
}

type errorList struct {
	errors []error
}

func (el *errorList) addError(err error) {
	if el.errors == nil {
		el.errors = []error{err}
	} else {
		el.errors = append(el.errors, err)
	}
}

// deliverMessage recursively visits the source and all nodes and delivers the message if the node accepts this message type.
// Because multiple node types may accept this message, this method returns an array of errors.
func (e *Executor) deliverMessage(msg message.Message) []error {
	// translate to a fbcontext message
	ctxMsg := newContextMessage(msg)

	errorList := &errorList{}

	// source
	if e.source.AcceptsMessage(ctxMsg.MessageType) {
		err := e.source.Receive(ctxMsg)
		if err != nil {
			errorList.addError(err)
		}
	}

	// all nodes
	for _, rootNode := range e.rootNodes {
		e.deliverMessageToNode(ctxMsg, rootNode, errorList)
	}
	return errorList.errors
}

func (e *Executor) deliverMessageToNode(msg fbcontext.Message, node *node.Context, errorList *errorList) {
	if node.NodeProcessor.AcceptsMessage(msg.MessageType) {
		err := node.NodeProcessor.Receive(msg)
		if err != nil {
			errorList.addError(err)
		}
	}

	for _, child := range node.Children {
		e.deliverMessageToNode(msg, child, errorList)
	}
}

func newContextMessage(msg message.Message) fbcontext.Message {
	return fbcontext.Message{
		MessageType: msg.MessageType,
		Key:         msg.Key,
		Payload:     msg.Payload,
	}
}

func newMessage(msg fbcontext.Message) message.Message {
	return message.Message{
		MessageType: msg.MessageType,
		Key:         msg.Key,
		Payload:     msg.Payload,
	}
}

func sendMessage(msg fbcontext.Message) error {
	return message.GetSender().Send(newMessage(msg))
}

func ackMessage(msg fbcontext.Message) error {
	return message.GetSender().Ack(newMessage(msg))
}

// NoOpMessageReceiver is a message receiver that does, well, nothing
type NoOpMessageReceiver struct {
}

// Start is a no-op
func (n *NoOpMessageReceiver) Start() {}

// Initialized is a no-op
func (n *NoOpMessageReceiver) Initialized() bool {
	return true
}

// SetNotificationFunc is a no-op
func (n *NoOpMessageReceiver) SetNotificationFunc(notifier message.NotificationFunc) {}

// Shutdown is a no-op
func (n *NoOpMessageReceiver) Shutdown() {}
