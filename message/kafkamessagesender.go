package message

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"

	"github.internal.digitalocean.com/observability/firebolt/config"
	"github.internal.digitalocean.com/observability/firebolt/node/kafkaproducer"
)

// KafkaMessageSender is a message Sender for Kafka.
type KafkaMessageSender struct {
	producer *kafkaproducer.KafkaProducer
	topic    string
}

// NewKafkaMessageSender creates and configures a Sender that uses a compact kafka topic as its transport.
func NewKafkaMessageSender(config *config.InternalDataConfig) Sender {
	kp := &kafkaproducer.KafkaProducer{}
	producerConfig := make(map[string]string)
	producerConfig["brokers"] = config.Params["brokers"]
	producerConfig["topic"] = config.Params["messagetopic"]
	err := kp.Setup(producerConfig)
	if err != nil {
		log.WithError(err).Info("kafkamessagesender: failed to setup kafka producer")
	}

	log.Info("kafkamessagesender: created new messagesender")
	return KafkaMessageSender{
		producer: kp,
		topic:    config.Params["messagetopic"],
	}
}

// Send sends the message on the configured kafka topic
func (s KafkaMessageSender) Send(msg Message) error {
	log.WithField("message_type", msg.MessageType).Debug("kafkamessagesender: producing message")
	return s.produceMessage(msg, false)
}

// Ack acknowledges that the message has been received and completed; it will not be delivered again.   Because of the
// inherent raciness of kafka message delivery this cannot be guaranteed, and all message processing within a node
// must be idempotent.
func (s KafkaMessageSender) Ack(msg Message) error {
	//TODO: would it be possible to provide a stronger guarantee if we buffered messages in messageConsumer for say 1 second, and only
	//TODO: deliver them if no ACK arrives in that time?
	return s.produceMessage(msg, true)
}

func (s *KafkaMessageSender) produceMessage(msg Message, ack bool) error {
	wireMsg := &wireMessage{
		Message:     msg,
		Updated:     time.Now(),
		Acknowleged: ack,
	}

	wireMsgBytes, err := json.Marshal(wireMsg)
	if err != nil {
		return fmt.Errorf("kafkamessagesender: failed to marshal wire message [%v]", err)
	}

	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &s.topic, Partition: kafka.PartitionAny},
		Key:            []byte(uniqueKey(msg)),
		Value:          wireMsgBytes,
	}

	s.producer.Produce(kafkaMsg)
	return nil
}

// Shutdown stops the underlying kafka producer
func (s KafkaMessageSender) Shutdown() {
	err := s.producer.Shutdown()
	if err != nil {
		log.WithError(err).Warn("kafkamessagesender: failed to shut down producer")
	}
}
