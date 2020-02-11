package kafkaproducer

import (
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"

	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/fbcontext"
	"github.com/digitalocean/firebolt/util"
)

// KafkaProducer is a firebolt node for producing messages onto a Kafka topic.
type KafkaProducer struct {
	fbcontext.ContextAware
	producer MessageProducer
	topic    string
	stopChan chan bool
}

// MessageProducer is an interface extracted from 'kafka.Producer' to make this mockable
// generated from this dir with 'mockery -name messageProducer -inpkg .' in case that interface changes
type MessageProducer interface {
	ProduceChannel() chan *kafka.Message
	Events() chan kafka.Event
	Flush(timeoutMs int) int
	Close()
}

// Setup creates the underlying Kafka producer client and events receiver, leaving it ready to handle events.
func (k *KafkaProducer) Setup(config map[string]string) error {
	configMap, err := k.buildConfigMap(config)
	if err != nil {
		return err
	}

	log.WithField("kafkabrokers", config["brokers"]).Info("creating kafka producer")
	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.WithError(err).Error("failed to create kafka producer")
		return err
	}
	log.Info("created kafka producer")

	k.producer = p
	k.topic = config["topic"]
	k.stopChan = make(chan bool)

	go k.startEventsReceiver()
	log.Info("started kafka events receiver")

	return nil
}

func (k *KafkaProducer) buildConfigMap(config map[string]string) (*kafka.ConfigMap, error) {
	err := k.checkConfig(config)
	if err != nil {
		return nil, err
	}

	// default kafka consumer config
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":            config["brokers"],
		"statistics.interval.ms":       60000,
		"queue.buffering.max.messages": 50000,
		"queue.buffering.max.kbytes":   256000,
		"queue.buffering.max.ms":       3000,
		"log.connection.close":         false,
		"socket.keepalive.enable":      true,
		"compression.codec":            "snappy",
	}

	err = util.ApplyLibrdkafkaConf(config, configMap)
	if err != nil {
		return nil, err
	}

	return configMap, nil
}

func (k *KafkaProducer) checkConfig(config map[string]string) error {
	if config["brokers"] == "" {
		return fmt.Errorf("kafkaproducer: missing or invalid value for config 'brokers': %s", config["brokers"])
	}

	return nil
}

// Process sends a single event `msg` to the configured Kafka topic.
func (k *KafkaProducer) Process(event *firebolt.Event) (*firebolt.Event, error) {
	// start with a type assertion because :sad-no-generics:
	produceRequest, ok := event.Payload.(firebolt.ProduceRequest)
	if !ok {
		return nil, errors.New("kafkaproducer: failed type assertion for conversion to ProduceRequest")
	}

	// allow overriding the node config topic on a per-msg basis
	destinationTopic := k.topic
	if produceRequest.Topic() != "" {
		destinationTopic = produceRequest.Topic()
	}
	if destinationTopic == "" {
		return nil, errors.New("kafkaproducer: missing topic name in both node config and ProduceRequest")
	}

	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &destinationTopic, Partition: kafka.PartitionAny},
		Value:          produceRequest.Message(),
	}

	k.Produce(kafkaMsg)
	return nil, nil
}

// Produce produces a single client-constructed kafka.Message to the configured Kafka topic.
func (k *KafkaProducer) Produce(msg *kafka.Message) {
	log.WithField("topic", k.topic).Debug("kafkaproducer: placing message on producechannel")
	k.producer.ProduceChannel() <- msg
}

// Shutdown stops the underlying Kafka producer client.
func (k *KafkaProducer) Shutdown() error {
	k.stop()
	return nil
}

// Running in a goroutine, asynchronously report on producer events
func (k *KafkaProducer) startEventsReceiver() {
	for e := range k.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.WithField("node_id", k.ID).WithField("producer_error", ev.TopicPartition.Error).Debug("produce failed")
			} else {
				log.WithField("node_id", k.ID).WithField("kafka_topic", *ev.TopicPartition.Topic).
					WithField("kafka_partition", ev.TopicPartition.Partition).
					WithField("partition_offset", ev.TopicPartition.Offset).
					Debug("produced message successfully")
			}
		case *kafka.Stats:
			log.WithField("node_id", k.ID).WithField("kafka_producer_stats", ev).Debug("librdkafka producer stats")
		default:
			log.WithField("node_id", k.ID).WithField("kafka_producer_event", ev).Warn("received unexpected kafka producer event")
		}
	}
}

// stop closes the underlying kafka producer after flushing any unwritten records
func (k *KafkaProducer) stop() {
	log.WithField("node_id", k.ID).Info("stopping kafka producer")
	k.producer.Flush(5000)
	k.producer.Close()
}

// Receive handles a message from another node or an external source
func (k *KafkaProducer) Receive(msg fbcontext.Message) error {
	return errors.New("kafkaproducer: message not supported")
}
