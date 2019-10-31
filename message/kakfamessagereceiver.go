package message

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"

	"github.internal.digitalocean.com/observability/firebolt/config"
	kafkainterface "github.internal.digitalocean.com/observability/firebolt/kafka"
	"github.internal.digitalocean.com/observability/firebolt/util"
)

// KafkaMessageReceiver consumes messages from the configured 'messagetopic'.
type KafkaMessageReceiver struct {
	consumer       kafkainterface.MessageConsumer
	topic          string
	notifier       NotificationFunc
	partitionCount int
	initialized    bool
	initMutex      sync.RWMutex
	partitionEOFs  int
	initBuffer     map[string]*wireMessage
}

const maxMessagesToReplay = 50000

// NewKafkaReceiver creates a new instance of the messagereceiver
func NewKafkaReceiver(config *config.InternalDataConfig) (Receiver, error) {
	r := &KafkaMessageReceiver{
		topic:      config.Params["messagetopic"],
		initMutex:  sync.RWMutex{},
		initBuffer: make(map[string]*wireMessage),
	}

	consumerConfigMap, err := r.buildConfigMap(config.Params)
	if err != nil {
		return nil, err
	}

	// create the consumer
	kc, err := kafka.NewConsumer(consumerConfigMap)
	if err != nil {
		log.WithError(err).Error("kafkamessagereceiver: failed to create message topic consumer")
		return nil, err
	}
	r.consumer = kc

	return r, nil
}

// Start instructs the messagereceiver to begin accepting messages
func (r *KafkaMessageReceiver) Start() {
	go r.handleEvents()
}

// Initialized indicates whether the receiver has completed initial read of all pending (non-acknowledged) messages from the
// transport.
func (r *KafkaMessageReceiver) Initialized() bool {
	r.initMutex.RLock()
	defer r.initMutex.RUnlock()
	return r.initialized
}

// SetNotificationFunc assigns a function to be called for each message that is delivered on the topic.
func (r *KafkaMessageReceiver) SetNotificationFunc(notifier NotificationFunc) {
	r.notifier = notifier
}

// buildConfigMap creates the kafka ConfigMap that configures the confluent / librdkafka consumer
func (r *KafkaMessageReceiver) buildConfigMap(config map[string]string) (*kafka.ConfigMap, error) {

	// default kafka consumer config
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":               config["brokers"],
		"group.id":                        "firebolt-messages", // offsets never stored but group required anyway
		"session.timeout.ms":              10000,
		"enable.auto.commit":              false,
		"go.events.channel.enable":        true,
		"go.events.channel.size":          100,
		"go.application.rebalance.enable": false,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"},
		"socket.keepalive.enable":         true,
		"log.connection.close":            false,
		"enable.partition.eof":            true,
	}

	err := util.ApplyLibrdkafkaConf(config, configMap)
	if err != nil {
		return nil, err
	}

	return configMap, nil
}

func (r *KafkaMessageReceiver) handleEvents() {
	// rather than calling Subscribe here, which would use consumer-group behavior that spreads the partitions across
	// all nodes/instances, query the topic metadata to find all partitions, then manually generate assignments for the consumer
	//
	// in tests where we use autocreated topics, it takes some time for the broker to assign leaders, so we need to retry
	// in a loop here
	var messageTopicPartitions []kafka.PartitionMetadata
	err := util.AwaitCondition(func() bool {
		metadata, err := r.consumer.GetMetadata(&r.topic, false, 30000)
		if err != nil {
			log.WithError(err).Error("kafkamessagereceiver: failed to read message topic metadata, retrying")
			return false
		}
		messageTopicPartitions = metadata.Topics[r.topic].Partitions
		r.partitionCount = len(messageTopicPartitions)
		if r.partitionCount > 1 {
			log.WithField("num_partitions", len(messageTopicPartitions)).Warn("kafkamessagereceiver: the message topic has more than one partition, messages may be delivered out-of-order")
		}
		return len(messageTopicPartitions) > 0
	}, 1*time.Second, 60*time.Second)
	if err != nil {
		log.WithError(err).Error("kafkamessagereceiver: too many tries to read message topic metadata, stopping consumer")
		return
	}

	assignments := r.buildPartitionAssignments(messageTopicPartitions)

	err = r.consumer.Assign(assignments)
	if err != nil {
		log.WithError(err).Error("kafkamessagereceiver: failed to assign all partitions to message topic consumer")
		return
	}

	for {
		select {
		case event := <-r.consumer.Events():
			if event == nil {
				log.Info("kafkamessagereceiver: consumer shutdown")
				return
			}
			r.processEvent(event)
		}
	}
}

func (r *KafkaMessageReceiver) buildPartitionAssignments(messageTopicPartitions []kafka.PartitionMetadata) []kafka.TopicPartition {
	assignments := []kafka.TopicPartition{}
	for _, partition := range messageTopicPartitions {
		if partition.Error.Code() != kafka.ErrNoError {
			log.WithField("partition_id", partition.ID).WithField("kafka_error_code", partition.Error.Code()).Error("kafkamessagereceiver: failed to start consumer due to kafka partition error")
		}

		low, high, err := r.consumer.QueryWatermarkOffsets(r.topic, partition.ID, 10000)
		if err != nil {
			log.WithError(err).Error("kafkamessagereceiver: failed to query watermark offsets")
			low = 0
		}

		// limit the total number of messages that will replay to prevent an extreme memory usage spike on startup
		startOffset := low
		if high-low > maxMessagesToReplay {
			startOffset = high - maxMessagesToReplay
		}

		log.WithField("partition_id", partition.ID).WithField("initial_offset", startOffset).WithField("high_offset", high).Info("kafkamessagereceiver: assigning message consumer partition")
		assignments = append(assignments, kafka.TopicPartition{
			Topic:     &r.topic,
			Partition: partition.ID,
			Offset:    kafka.Offset(startOffset),
		})
	}
	return assignments
}

func (r *KafkaMessageReceiver) processEvent(ev kafka.Event) {
	switch e := ev.(type) {
	case *kafka.Message:
		r.processMessage(e.Value)
	case kafka.PartitionEOF:
		r.partitionEOFs++
		if !r.initialized && r.partitionEOFs >= r.partitionCount {
			r.initMutex.Lock()
			defer r.initMutex.Unlock()

			r.initialized = true
			r.processInitBuffer()
		}
	case kafka.Error:
		log.WithField("kafka_event", e).Error("kafkamessagereceiver: kafka error")
		// per edenhill, errors are to be treated as informational and the consumer should continue with the assumption that librdkafka recovers
	}
}

func (r *KafkaMessageReceiver) processMessage(value []byte) {
	wireMsg := &wireMessage{}
	err := json.Unmarshal(value, wireMsg)
	if err != nil {
		log.WithError(err).Error("kafkamessagereceiver: failed to unmarshal wire message")
		return
	}

	if !r.initialized {
		r.initBuffer[uniqueKey(wireMsg.Message)] = wireMsg
	} else {
		if !wireMsg.Acknowleged {
			r.deliverMessage(wireMsg.Message)
		}
	}
}

func (r *KafkaMessageReceiver) deliverMessage(msg Message) {
	err := r.notifier(msg)
	if err != nil {
		log.WithField("message_type", msg.MessageType).Error("kafkamessagereceiver: failed to deliver message")
	}
}

// processInitBuffer runs after the consumer has consumed all initial messages, when the newest version (possibly acknowledged)
// will be in the initBuffer, to prevent acknowledged or stale messages from being redelivered
func (r *KafkaMessageReceiver) processInitBuffer() {
	log.WithField("buffered_messages", len(r.initBuffer)).Info("kafkamessagereceiver: delivering buffered messages")
	for _, wireMsg := range r.initBuffer {
		if !wireMsg.Acknowleged {
			msg := wireMsg.Message
			log.WithField("message_key", msg.Key).WithField("message_type", msg.MessageType).Infof("kafkamessagereceiver: delivering initial message")
			r.deliverMessage(msg)
		}
	}

	// free init buffer memory
	r.initBuffer = make(map[string]*wireMessage)
}

// Shutdown stops the message consumer
func (r *KafkaMessageReceiver) Shutdown() {
	log.Info("kafkamessagereceiver: shutdown initiated")
	err := r.consumer.Close()
	if err != nil {
		log.WithError(err).Error("kafkamessagereceiver: error while closing kafka consumer")
	}
	log.Info("kafkamessagereceiver: consumer closed")
}
