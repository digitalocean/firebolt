package kafkaconsumer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/digitalocean/firebolt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"

	"github.com/digitalocean/firebolt/fbcontext"
	kafkainterface "github.com/digitalocean/firebolt/kafka"
	"github.com/digitalocean/firebolt/metrics"
	"github.com/digitalocean/firebolt/util"
)

const (
	messageTypeRecoveryRequest = "recoveryrequest"   // request to initiate recovery for a single kafka partition
	messageTypeCancelRecovery  = "recoverycancelall" // request to cancel all active recovery
)

// KafkaConsumer is a firebolt source that receives records from a Kafka topic.
type KafkaConsumer struct {
	fbcontext.ContextAware
	consumer                kafkainterface.MessageConsumer
	topic                   string
	sendCh                  chan firebolt.Event
	doneCh                  chan struct{}
	assignPartitionsMutex   sync.Mutex
	assignPartitionsCtx     context.Context
	assignPartitionsCancel  context.CancelFunc
	maxInitialPartitionLag  int
	metrics                 *Metrics
	recoveryConsumerEnabled bool
	recoveryConsumer        *RecoveryConsumer
}

// Setup instantiates and configures the underlying Kafka consumer client
func (k *KafkaConsumer) Setup(config map[string]string, eventchan chan firebolt.Event) error {
	err := k.checkConfig(config)
	if err != nil {
		return err
	}

	// maximum number of records per kafka partition that we are willing to try to catch-up from when a partition is assigned
	maxInitialPartitionLag, err := strconv.Atoi(config["maxpartitionlag"])
	if err != nil {
		log.WithError(err).Error("failed to convert source maxpartitionlag to int")
		return err
	}
	k.maxInitialPartitionLag = maxInitialPartitionLag

	// parallel recovery disabled by default
	if config["parallelrecoveryenabled"] == "" {
		config["parallelrecoveryenabled"] = "false"
	}
	k.recoveryConsumerEnabled, _ = strconv.ParseBool(config["parallelrecoveryenabled"])

	configMap, err := k.buildConfigMap(config)
	if err != nil {
		return err
	}

	k.Subscribe([]string{messageTypeRecoveryRequest, messageTypeCancelRecovery})

	// create the consumer
	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		log.WithError(err).Error("failed to create consumer")
		return err
	}
	k.consumer = c
	k.topic = config["topic"]
	k.sendCh = eventchan
	k.doneCh = make(chan struct{}, 1)
	k.assignPartitionsMutex = sync.Mutex{}
	k.assignPartitionsCtx, k.assignPartitionsCancel = context.WithCancel(context.Background())

	// initialize metrics
	k.metrics = &Metrics{}
	k.metrics.RegisterConsumerMetrics()

	// create the recoveryConsumer
	if k.recoveryConsumerEnabled {
		k.recoveryConsumer, err = NewRecoveryConsumer(k.topic, k.sendCh, config, k.metrics, k.Ctx)
		if err != nil {
			log.WithError(err).Error("failed to create recovery consumer")
			return err
		}
	}

	log.Info("created kafka consumer")
	return nil
}

// buildConfigMap creates the kafka ConfigMap that configures the confluent / librdkafka consumer
func (k *KafkaConsumer) buildConfigMap(config map[string]string) (*kafka.ConfigMap, error) {
	// outbound channel buffer size
	bufsize, err := strconv.Atoi(config["buffersize"])
	if err != nil {
		return nil, errors.New("kafkaconsumer: failed to convert config 'buffersize' to integer")
	}

	// default kafka consumer config
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":               config["brokers"],
		"group.id":                        config["consumergroup"],
		"session.timeout.ms":              10000,
		"enable.auto.commit":              true,
		"auto.commit.interval.ms":         5000,
		"statistics.interval.ms":          60000,
		"go.events.channel.enable":        true,
		"go.events.channel.size":          bufsize,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"},
		"socket.keepalive.enable":         true,
		"log.connection.close":            false,
	}

	err = util.ApplyLibrdkafkaConf(config, configMap)
	if err != nil {
		return nil, err
	}

	return configMap, nil
}

// nolint: gocyclo
func (k *KafkaConsumer) checkConfig(config map[string]string) error {
	if config["brokers"] == "" {
		return fmt.Errorf("kafkaconsumer: missing or invalid value for config 'brokers': [%s]", config["brokers"])
	}

	if config["consumergroup"] == "" {
		return fmt.Errorf("kafkaconsumer: missing or invalid value for config 'consumergroup': [%s]", config["consumergroup"])
	}

	if config["topic"] == "" {
		return fmt.Errorf("kafkaconsumer: missing or invalid value for config 'topic': [%s]", config["topic"])
	}

	if config["buffersize"] == "" {
		return fmt.Errorf("kafkaconsumer: missing or invalid value for config 'buffersize': [%s]", config["buffersize"])
	}
	bufsize, err := strconv.Atoi(config["buffersize"])
	if err != nil {
		return fmt.Errorf("kafkaconsumer: failed to convert config 'buffersize' to integer: [%v]", err)
	}
	if bufsize < 1 {
		return fmt.Errorf("kafkaconsumer: invalid value for config 'buffersize', must be greater than zero: [%d]", bufsize)
	}

	// default maxpartitionlag to maxint so that the default is 'normal' kafka behavior where the committed offsets are used
	// regardless of how far behind we may be
	if config["maxpartitionlag"] == "" {
		config["maxpartitionlag"] = strconv.Itoa(math.MaxInt64)
		log.Info("defaulted maxpartitionlag to maxint")
	}
	maxPartitionLag, err := strconv.Atoi(config["maxpartitionlag"])
	if err != nil {
		return fmt.Errorf("kafkaconsumer: failed to convert config 'maxpartitionlag' to integer: [%v]", err)
	}
	if maxPartitionLag < 0 {
		return fmt.Errorf("kafkaconsumer: invalid value for config 'maxpartitionlag', cannot be negative: [%d]", bufsize)
	}

	// parallel recovery is disabled by default, so don't validate boolean if empty
	if config["parallelrecoveryenabled"] != "" {
		_, err = strconv.ParseBool(config["parallelrecoveryenabled"])
		if err != nil {
			return fmt.Errorf("kafkaconsumer: invalid value for required config 'parallelrecoveryenabled', must be a boolean: [%v]", err)
		}
	}

	return nil
}

// Start subscribes the Kafka consumer client to the configured topic and starts reading records from the consumer's
// channel
func (k *KafkaConsumer) Start() error {
	err := k.consumer.Subscribe(k.topic, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case event := <-k.consumer.Events():
			if event == nil {
				log.Info("kafkaconsumer: consumer shutdown")
				return nil
			}
			k.processEvent(event)
		case <-k.doneCh:
			log.Info("kafkaconsumer: notify done")

			// recoveryconsumer uses the same output channel as kafkaconsumer, so it must be shutdown before we return
			// and that channel is closed
			if k.recoveryConsumerEnabled {
				k.recoveryConsumer.Shutdown()
			}

			return nil
		}
	}
}

func (k *KafkaConsumer) processEvent(ev kafka.Event) {
	switch e := ev.(type) {
	case kafka.AssignedPartitions:
		go k.retryAssignPartitions(e.Partitions)
	case kafka.RevokedPartitions:
		k.revokePartitionAssignments()
	case *kafka.Message:
		k.sendCh <- firebolt.Event{
			Payload:  e.Value,
			Created:  time.Now(),
			Recovery: false,
		}
		metrics.Source().EventsEmitted.Inc()
		k.metrics.EventsConsumed.WithLabelValues(strconv.Itoa(int(e.TopicPartition.Partition))).Inc()
	case kafka.Error:
		log.WithField("kafka_event", e).Error("kafkaconsumer: consumer error")
	case *kafka.Stats:
		log.WithField("node_id", k.ID).WithField("kafka_consumer_stats", e.String()).Debug("kafkaconsumer: librdkafka consumer stats")
		k.metrics.UpdateConsumerMetrics(e.String(), k.topic)
	}
}

func (k *KafkaConsumer) revokePartitionAssignments() {
	log.Info("kafkaconsumer: revoke all partition assignments")

	// first cancel any assignments that may be in progress and wait for the lock to be available
	k.assignPartitionsCancel()
	k.assignPartitionsMutex.Lock()
	defer func() {
		k.assignPartitionsMutex.Unlock()
		// reset the cancel context so that it can be used to cancel the next call to retryAssignPartitions
		k.assignPartitionsCtx, k.assignPartitionsCancel = context.WithCancel(context.Background())
	}()

	err := k.consumer.Unassign()
	if err != nil {
		log.WithError(err).Error("kafkaconsumer: failed to unassign partitions")
	}

	// keep the recoveryConsumer in sync with our assignments
	if k.recoveryConsumerEnabled {
		k.recoveryConsumer.SetAssignedPartitions([]kafka.TopicPartition{})
		err := k.recoveryConsumer.RefreshAssignments()
		if err != nil {
			log.WithError(err).Error("kafkaconsumer: failed to unassign recoveryconsumer partitions")
		}
	}
}

// retryAssignPartitions continuously retries assigning partitions - we found during a Kafka broker outage that a consumer
// rebalance occurred and subsequently errors occurred while fetching committed offsets or partition watermarks, so we
// retry until the cluster is healthy again
func (k *KafkaConsumer) retryAssignPartitions(partitions []kafka.TopicPartition) {
	for _, topicPartition := range partitions {
		log.WithField("partition_id", topicPartition.Partition).Info("kafkaconsumer: received new partition assignment")
	}

	// wait for the lock to be available
	k.assignPartitionsMutex.Lock()
	defer k.assignPartitionsMutex.Unlock()

	// initial, normal attempt
	err := k.assignPartitions(partitions)
	if err != nil {
		log.Error("kafkaconsumer: failed to assign partitions, starting retry loop")
		ticker := time.NewTicker(3 * time.Second)

		for {
			select {
			case <-ticker.C:
				err := k.assignPartitions(partitions)
				if err == nil {
					log.Info("kafkaconsumer: partitions assigned successfully, stopping retry loop")
					ticker.Stop()
					return
				}
				log.Error("kafkaconsumer: failed to assign partitions, retrying in 3s")
			case <-k.assignPartitionsCtx.Done():
				// cancellation happens whenever librdkafka sends a 'revokePartitions' event
				log.Warn("kafkaconsumer: revoke assignment request cancelled pending partition assignment")
				ticker.Stop()
				return
			}
		}
	}

	for _, topicPartition := range partitions {
		log.WithField("partition_id", topicPartition.Partition).Info("kafkaconsumer: successfully assigned partitions on first attempt")
	}
}

// assignPartitions calculates the offest to start from and assigns partitions to this consumer *and* the recoveryconsumer
// if recovery conditions are met
func (k *KafkaConsumer) assignPartitions(partitions []kafka.TopicPartition) error {
	// every time a rebalance happens we'll get assigned partitions, first we calculate what offset to start from for each partition
	partitionsWithOffsets, err := k.calculateAssignmentOffsets(partitions)
	if err != nil {
		return err
	}

	// assign the partitions (and offsets) to the consumer
	err = k.consumer.Assign(partitionsWithOffsets)
	if err != nil {
		log.WithError(err).Error("kafkaconsumer: failed to assign partitions")
		return err
	}

	// keep the recoveryConsumer in sync with this consumer's partition assignments
	if k.recoveryConsumerEnabled {
		partitionsCopy := make([]kafka.TopicPartition, len(partitionsWithOffsets))
		copy(partitionsCopy, partitionsWithOffsets)
		k.recoveryConsumer.SetAssignedPartitions(partitionsCopy)
	}

	return nil
}

// calculateAssignmentOffsets determines, each time a rebalance happens and partitions are assigned to this consumer, what offset
// it should start reading from.
// In the basic case it will start reading from the committed (stored in kafka) offsets for the partition, however in an
// outage recovery situation it's possible that we are far enough behind that it would take quite a while to recover.
// For this reason users can configure a 'maxInitialPartitionLag' that caps the number of events per-partition that it
// will try to 'catch up' during restart by using 'highWatermark - maxInitialPartitionLag' as the initial offset.
func (k *KafkaConsumer) calculateAssignmentOffsets(assignedPartitions []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	maxInitialPartitionLagOffset := kafka.Offset(k.maxInitialPartitionLag)

	// get the committed offsets, this is queried separately because unfortunately stored offsets are not returned along with the assignment
	committedOffsets, err := k.consumer.Committed(assignedPartitions, 10000)
	if err != nil { // one error that can happen here is 'Local: Timed out', seen during a brief kafka outage, see OB-1239
		log.WithError(err).Error("kafkaconsumer: failed to query committed offsets")
		return nil, err
	}

	partitions := make([]kafka.TopicPartition, len(assignedPartitions))
	for i, tp := range assignedPartitions {
		log.WithField("kafka_topic", *tp.Topic).WithField("partition_id", tp.Partition).Info("kafkaconsumer: consumer was assigned partition")
		partitionOffset := k.offsetForPartition(tp.Partition, committedOffsets)
		if partitionOffset == kafka.OffsetInvalid {
			partitionOffset = kafka.Offset(0)
		}

		// find the low/high total offset range available on the broker for this partition
		low, high, err := k.consumer.QueryWatermarkOffsets(k.topic, tp.Partition, 10000)
		if err != nil { // one error that can happen here is 'Local: Operation in progress', seen during a brief kafka outage, see OB-1239
			log.WithError(err).Error("kafkaconsumer: failed to query watermark offsets")
			return nil, err
		}
		log.WithField("partition_id", tp.Partition).WithField("offset_low", low).WithField("offset_high", high).Info("kafkaconsumer: query partition for watermarks")

		if kafka.Offset(high)-partitionOffset > maxInitialPartitionLagOffset {
			log.WithField("partition_id", tp.Partition).WithField("stored_offset", partitionOffset).WithField("partition_latest_offset", high).WithField("max_offset_lag", k.maxInitialPartitionLag).Warn("kafkaconsumer: offset assignment: recovery limited to max offset; data loss is expected")
			if maxInitialPartitionLagOffset > kafka.Offset(high) {
				log.WithField("partition_id", tp.Partition).WithField("stored_offset", partitionOffset).WithField("partition_latest_offset", high).WithField("max_offset_lag", k.maxInitialPartitionLag).Warn("kafkaconsumer: high watermark less than maxinitialpartitionlag, setting offset to zero")
				partitionOffset = 0
			} else {
				offsetWithCappedLag := kafka.Offset(high) - maxInitialPartitionLagOffset
				// request recovery if needed; this should never be done for new topics
				if k.recoveryConsumerEnabled { //&& partitionOffset != kafka.OffsetInvalid
					k.recoveryConsumer.RequestRecovery(tp.Partition, partitionOffset, offsetWithCappedLag)
				}
				partitionOffset = offsetWithCappedLag
			}
		} else {
			log.WithField("partition_id", tp.Partition).WithField("stored_offset", partitionOffset).WithField("partition_latest_offset", high).WithField("max_offset_lag", k.maxInitialPartitionLag).Info("kafkaconsumer: offset assignment: normal assignment using stored offset")
		}

		tp.Offset = partitionOffset
		partitions[i] = tp
	}

	return partitions, nil
}

func (k *KafkaConsumer) offsetForPartition(partition int32, offsets []kafka.TopicPartition) kafka.Offset {
	for _, tp := range offsets {
		if tp.Partition == partition {
			log.WithField("partition_id", partition).Debugf("kafkaconsumer: got committed offset %s", tp.Offset.String())
			return tp.Offset
		}
	}

	log.WithField("partition_id", partition).Warn("kafkaconsumer: no committed offset found for this partition, starting from offset 0")
	return 0
}

// Shutdown stops the Kafka consumer client
func (k *KafkaConsumer) Shutdown() error {
	log.Info("kafkaconsumer: shutdown initiated")
	k.doneCh <- struct{}{}
	k.assignPartitionsCancel()
	err := k.consumer.Close()
	if err != nil {
		log.WithError(err).Error("kafkaconsumer: error while closing kafka consumer")
	}
	log.Info("kafkaconsumer: consumer closed")

	return nil
}

// Receive handles a message from another node or an external source
func (k *KafkaConsumer) Receive(msg fbcontext.Message) error {
	if !k.recoveryConsumerEnabled {
		// when recovery is disabled, ignore all recovery command messages
		return nil
	}

	if msg.MessageType == messageTypeRecoveryRequest {
		k.recoveryConsumer.tracker.receiveRequest(msg.Key, msg.Payload)
		return nil
	}
	if msg.MessageType == messageTypeCancelRecovery {
		err := k.recoveryConsumer.tracker.cancelAll()
		if err != nil {
			return err
		}
		return k.Ctx.AckMessage(msg) // acknowledge receipt so that this will not be redelivered
	}
	return fmt.Errorf("kafkaconsumer: unexpected messagetype %s", msg.MessageType)
}

// GetMetrics returns the instance of ConsumerMetrics used by this kafkaconsumer.
func (k *KafkaConsumer) GetMetrics() *Metrics {
	return k.metrics
}
