package message

import (
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/digitalocean/firebolt/config"
)

// Message is a single message to be delivered to a source or node
type Message struct {
	MessageType string `json:"messagetype"` // e.g. RequestConsumerRecovery or UpdateRateData or CancelConsumerRecovery or RestartSource (for ES recovery?)
	Key         string `json:"key"`         // key for the Kafka message carrying this message; this can be either be a unique business key or synthetic unique key
	Payload     []byte `json:"payload"`     // the payload (optional) delivered to the target along with the message itself
}

// NotificationFunc is the method used to send a new message to all sources / nodes that accept it
type NotificationFunc func(msg Message) []error

// Receiver receives messages using the configured transport
type Receiver interface {
	Start()
	Initialized() bool
	SetNotificationFunc(notifier NotificationFunc)
	Shutdown()
}

// Sender sends messages using the configured transport
type Sender interface {
	Send(msg Message) error
	Ack(msg Message) error
	Shutdown()
}

var senderSingleton Sender
var senderLock sync.Mutex

// InitKafkaSender constructs the singleton instance of a messagesender using the kafka transport
func InitKafkaSender(config config.InternalDataConfig) {
	senderLock.Lock()
	defer senderLock.Unlock()
	senderSingleton = NewKafkaMessageSender(&config)
	log.Info("message: kafka message sender initialization complete")
}

// ShutdownKafkaSender stops the kafka producer and unsets the singleton
func ShutdownKafkaSender() {
	log.Info("message: shutdown kafka messagesender")
	senderLock.Lock()
	defer senderLock.Unlock()
	if senderSingleton != nil {
		senderSingleton.Shutdown()
		senderSingleton = nil
	}
}

// GetSender returns the singleton sender
func GetSender() Sender {
	if senderSingleton == nil {
		log.Error("message: illegal attempt to access messagesender before initialization")
	}
	return senderSingleton
}

//TODO: readme.md docs for developers to get started (mention make targets and installing cover at a min)
//TODO: update docs accordingly for 'messagetopic' param, add docs describing compaction settings and single partition to guarantee ordering
