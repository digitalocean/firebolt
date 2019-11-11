package kafkaconsumer

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/digitalocean/firebolt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"

	"github.com/digitalocean/firebolt/fbcontext"
	kafkainterface "github.com/digitalocean/firebolt/kafka"
	"github.com/digitalocean/firebolt/util"
)

const updateRecoveryRequestSeconds = 5
const refreshPeriodMs = 10000 // how often we check recoverytracker for new recovery requests

// RecoveryConsumer is a rate-limited consumer that reprocesses any records that were missed during an outage.  Whenever
// parallel recover is enabled, if KafkaConsumer is initially assigned a partition with high watermark too far ahead
// (as defined by config 'maxpartitionlag') there will be a gap of missed records between the old stored offset and
// (highwatermark - maxpartitionlag).
// KafkaConsumer will request recovery for that partition, those requests are managed by RecoveryTracker.  The kafkaconsumer's
// partition assignments, which are auto-balanced, drive the recoveryconsumer as well.
// There are two orders of operation for starting recovery for a partition:
// *Initial Request, Partition Assignment Exists First*
// kafkaconsumer is assigned a partition, detects a recovery condition, requests recovery, and the call to RequestRecovery()
// synchronously assigns that partition within RecoveryConsumer
// *Rebalance Assignment, Recovery Request Exists First*
// kafkaconsumer is assigned a partition, notifies recoveryconsumer by calling AssignPartitions, fetches the recovery request
// for the partition from recoverytracker, and assigns the partition within recoveryconsumer
//
// Recovery is rate limited by config 'parallelrecoverymaxrate'; this represents the total number of events per second to
// receiver; the value should be set to ensure that downstream systems are not overwhelmed during recovery.
//
// There are a few different types of race conditions to worry about here...
// * Race conditions on consumer rebalance:
// If one node gets partitions assigned, finds a gap between stored/highwatermark, and creates a request, then a consumer
// rebalance follows and the same partition assigned to another instance...
// - if that happens *before* offsets have been saved by the original assignee, a new request will be created that should match or
//   instead have a slightly higher highwatermark (and thus toOffset), which is fine to overwrite the old recovery request
// - if that request happens *after* offsets saved, a new request should not be created because that saved offset is within
//   'maxpartitionlag', and the existing request is still valid
// * Race conditions within recovery, between partition assignment to recoveryconsumer and recoveryrequest availability in
// recoverytracker:
// - on startup if recoveryrequests already exist, partitions may be assigned in kafkaconsumer before recoverytracker has
// 	 consumed those existing recoveryrequests, so when recoverytracker *does* consume them recoveryconsumer must update;
//   we use a ticker in recoveryconsumer that refreshes assignments from recoverytracker periodically
//
// What about a case where a recovery request is in progress for an outage, and there's a second outage?
// Because offsets have been stored, if the second outage was short enough to not generate a new recovery request then the old
// recovery request will restart.
// However if the second outage was long enough to generate a new recovery request, it will clobber the old one, and that old
// in-progress recovery is abandoned.   This shortcoming is worth addressing in a future release - we wouldn't want multiple
// recoveries running at the same time for a single partition, but each recovery request could be an ordered queue of requests,
// with request offset ranges that overlap automatically merged.
//
type RecoveryConsumer struct {
	consumer                kafkainterface.MessageConsumer
	topic                   string
	sendCh                  chan firebolt.Event
	doneCh                  chan struct{}
	tracker                 *RecoveryTracker
	assignedPartitions      []kafka.TopicPartition           // all partitions assigned to kafkaconsumer on this instance; not just those in need of recovery
	activePartitionMap      map[int32]partitionRecoveryState // partitions under active recovery in this recoveryconsumer
	maxRecordsToRecover     int                              // max records *per kafka partition* to recover
	maxRecordsPerSec        int                              // rate limit *per kafka partition* to recover
	updateRequestEvery      int64                            // update the recoveryrequest each time this number of records have been processed
	rateLimiter             *rate.Limiter
	ctx                     context.Context
	metrics                 *Metrics
	refreshTicker           *time.Ticker
	partitionAssignmentLock sync.RWMutex
}

type partitionRecoveryState struct {
	partition  kafka.TopicPartition
	fromOffset int64 // continuously updated from offsets (most recent offset recovered) for each partition
	toOffset   int64 // final offset to recover for this partition so that we know when to stop recovering
}

// NewRecoveryConsumer creates a RecoveryConsumer
func NewRecoveryConsumer(topic string, sendCh chan firebolt.Event, config map[string]string, metrics *Metrics, ctx fbcontext.FBContext) (*RecoveryConsumer, error) {

	maxRecordsToRecover, err := strconv.Atoi(config["parallelrecoverymaxrecords"])
	if err != nil {
		return nil, errors.New("recoveryconsumer: failed to convert config 'parallelrecoverymaxrecords' to integer")
	}

	maxRecordsPerSec, err := strconv.Atoi(config["parallelrecoverymaxrate"])
	if err != nil {
		return nil, errors.New("recoveryconsumer: failed to convert config 'parallelrecoverymaxrate' to integer")
	}

	r := &RecoveryConsumer{
		topic:               topic,
		sendCh:              sendCh,
		doneCh:              make(chan struct{}),
		assignedPartitions:  []kafka.TopicPartition{},
		maxRecordsToRecover: maxRecordsToRecover,
		maxRecordsPerSec:    maxRecordsPerSec,
		updateRequestEvery:  int64(updateRecoveryRequestSeconds * maxRecordsPerSec),
		rateLimiter:         rate.NewLimiter(rate.Limit(maxRecordsPerSec), 100),
		ctx:                 context.Background(),
		metrics:             metrics,
		refreshTicker:       time.NewTicker(refreshPeriodMs * time.Millisecond),
	}

	// set up the recoverytracker
	rt, err := NewRecoveryTracker(metrics, ctx)
	if err != nil {
		return nil, err
	}
	r.tracker = rt

	// create the consumer
	configMap, err := r.buildConfigMap(config)
	if err != nil {
		return nil, err
	}
	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		log.WithError(err).Error("failed to create recovery consumer")
		return nil, err
	}
	r.consumer = c

	go r.handleEvents()

	// periodically check recoverytracker for new RecoveryRequests
	go func() {
		for t := range r.refreshTicker.C {
			log.WithField("refresh_time", t).Debug("recoveryconsumer: refreshing partition assignments")
			err := r.RefreshAssignments()
			if err != nil {
				log.WithError(err).Error("recoveryconsumer: failed to refresh assignments")
			}
		}
	}()

	return r, nil
}

// buildConfigMap creates the kafka ConfigMap that configures the confluent / librdkafka consumer
func (rc *RecoveryConsumer) buildConfigMap(config map[string]string) (*kafka.ConfigMap, error) {
	// outbound channel buffer size
	bufsize, err := strconv.Atoi(config["buffersize"])
	if err != nil {
		return nil, errors.New("recoveryconsumer: failed to convert config 'buffersize' to integer")
	}

	// default kafka consumer config
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":               config["brokers"],
		"group.id":                        "firebolt-recoveryconsumer", // offsets never stored but group required anyway
		"session.timeout.ms":              10000,
		"enable.auto.commit":              false,   // offsets are driven by recovery requests, not consumer groups
		"auto.offset.reset":               "error", // attempt to prevent recovery issues caused by consumer jumping forward to recent offsets
		"go.events.channel.enable":        true,
		"go.events.channel.size":          bufsize,
		"go.application.rebalance.enable": false, // follows KafkaConsumer's partition assignments
		"socket.keepalive.enable":         true,
		"log.connection.close":            false,
	}

	err = util.ApplyLibrdkafkaConf(config, configMap)
	if err != nil {
		return nil, err
	}

	return configMap, nil
}

func (rc *RecoveryConsumer) handleEvents() {
	for {
		select {
		case event := <-rc.consumer.Events():
			if event == nil {
				log.Info("recoveryconsumer: consumer shutdown")
				return
			}
			rc.processEvent(event)
		case <-rc.doneCh:
			log.Info("recoveryconsumer: notify done")
			return
		}
	}
}

func (rc *RecoveryConsumer) processEvent(ev kafka.Event) {
	switch e := ev.(type) {
	case *kafka.Message:
		rc.recoverSingleEvent(e)
	case kafka.Error:
		rc.processError(e.Code())
	}
}

// processError handles kafka errors on the recovery consumer.   Normally the right thing to do is ignore the error and let
// librdkafka recover, but the error "Broker: Invalid message" means that the offset we're trying to recovery has been purged
// from the broker, so we handle that by updating the fromOffset and reassigning partitions.
func (rc *RecoveryConsumer) processError(e kafka.ErrorCode) {
	if e == kafka.ErrInvalidMsg || e == kafka.ErrOffsetOutOfRange {
		// we don't know from the error which of the partitions under recovery caused this error, so check them all
		func() {
			log.WithField("kafka_error", e.String()).Info("recoveryconsumer: received invalid message error, resetting from_offsets based on low watermarks")
			rc.partitionAssignmentLock.Lock()
			defer rc.partitionAssignmentLock.Unlock()

			// want to avoid handling this repeatedly, so first pause recovery entirely by removing all active partitions
			activePartitions := rc.activePartitionMap
			rc.setActivePartitionMap(make(map[int32]partitionRecoveryState))

			// check watermarks for each partition we _were_ recovering and reset the from offset to the low watermark if needed
			log.WithField("active_partition_count", len(activePartitions)).Info("recoveryconsumer: checking partitions after invalid message error")
			for _, partition := range activePartitions {
				low, _, err := rc.consumer.QueryWatermarkOffsets(rc.topic, partition.partition.Partition, 10000)
				if err != nil {
					log.WithError(err).Error("recoveryconsumer: failed to query watermark offsets")
					return
				}
				log.WithField("partition_id", partition.partition.Partition).WithField("offset_low", low).WithField("from_offset", partition.fromOffset).WithField("to_offset", partition.toOffset).Info("recoveryconsumer: while handling message error, got watermark offsets for partition")

				if partition.fromOffset < low {
					if low >= partition.toOffset {
						log.WithField("partition_id", partition.partition.Partition).WithField("offset_low", low).WithField("to_offset", partition.toOffset).Info("recoveryconsumer: cancelling recovery request after broker log truncation")
						err := rc.tracker.MarkRecoveryComplete(partition.partition.Partition, partition.toOffset)
						if err != nil {
							log.WithError(err).Error("recoveryconsumer: failed to mark recovery request complete")
						}
					} else {
						log.WithField("partition_id", partition.partition.Partition).WithField("offset_low", low).WithField("from_offset", partition.fromOffset).Info("recoveryconsumer: updating recovery request after broker log truncation")
						err := rc.tracker.UpdateRecoveryRequest(partition.partition.Partition, low, partition.toOffset)
						if err != nil {
							log.WithError(err).Error("recoveryconsumer: failed to update recovery request")
						}
					}
				}
			}
		}()
		// the next time RefreshAssignments runs it will restart all recovery using the updated low watermark
	}

	// all other errors are logged and librdkafka is responsible for recovery
	log.WithField("kafka_event", e.String()).Error("recoveryconsumer: kafka error")
}

//nolint: gocyclo
func (rc *RecoveryConsumer) recoverSingleEvent(e *kafka.Message) {
	var (
		recoveryState            partitionRecoveryState
		partitionIsInRecovery    bool
		remainingEventsToRecover int64
	)

	// hold this read lock for the minimum possible time
	func() {
		rc.partitionAssignmentLock.RLock()
		defer rc.partitionAssignmentLock.RUnlock()

		// get current recovery state for this partition
		recoveryState, partitionIsInRecovery = rc.activePartitionMap[e.TopicPartition.Partition]
		remainingEventsToRecover = recoveryState.toOffset - int64(e.TopicPartition.Offset)
	}()

	// after assignments change there can be a brief period where events arrive for the formerly-assigned partitions
	if !partitionIsInRecovery {
		return
	}

	// when one recovery request completes and another one is starting for the same partition, there is a brief period where
	// events arrive for the previous offset range are still arriving
	if int64(e.TopicPartition.Offset) < recoveryState.fromOffset {
		return
	}

	//TODO: add an integration test that auto-cancels recovery based on invalidoffset, just use a random negative offset if that works

	// if we're done with recovery for this partition, no need to continue
	if remainingEventsToRecover < 0 {
		log.WithField("partition_id", e.TopicPartition.Partition).WithField("event_offset", e.TopicPartition.Offset).WithField("to_offset", recoveryState.toOffset).Info("recoveryconsumer: recovery is complete for partition")
		err := rc.tracker.MarkRecoveryComplete(e.TopicPartition.Partition, recoveryState.toOffset)
		if err != nil {
			log.WithField("partition_id", e.TopicPartition.Partition).WithError(err).Error("recoveryconsumer: failed to mark recovery request complete")
		}
		err = rc.RefreshAssignments() // update the partitions assigned to the consumer *before* we remove it from the active list on the next line
		if err != nil {
			log.WithError(err).Error("recoveryconsumer: finished recovery for a partition then failed to update assignments")
		}
		return
	}

	// check if done with recovery for this partition?
	if int64(e.TopicPartition.Offset) > recoveryState.fromOffset { // this condition prevents re-recovering duplicate records after refreshing assignments
		// record valid for recovery; enforce rate limit
		err := rc.rateLimiter.Wait(rc.ctx)
		if err != nil {
			log.WithError(err).Warn("recoveryconsumer: rate limiter failed")
		}

		// update the fromOffset
		recoveryState.fromOffset = int64(e.TopicPartition.Offset)

		// and process the event
		rc.metrics.RecoveryEvents.WithLabelValues(strconv.Itoa(int(e.TopicPartition.Partition))).Inc()
		rc.sendCh <- firebolt.Event{
			Payload:  e.Value,
			Created:  time.Now(),
			Recovery: true,
		}
	}

	// periodically update the recovery request on the tracking topic, so that if the partition is reassigned to another instance, or recovery itself needs to be restarted, we don't repeat too much work
	if int64(e.TopicPartition.Offset)%rc.updateRequestEvery == 0 && remainingEventsToRecover > 0 {
		err := rc.tracker.UpdateRecoveryRequest(e.TopicPartition.Partition, int64(e.TopicPartition.Offset), recoveryState.toOffset)
		if err != nil {
			log.WithError(err).Error("recoveryconsumer: failed to persist recovery request")
		}
	}
}

// RequestRecovery creates a RecoveryTask to track recovery for a single partition.
func (rc *RecoveryConsumer) RequestRecovery(partitionID int32, fromOffset kafka.Offset, toOffset kafka.Offset) {
	from := int64(fromOffset)
	to := int64(toOffset)
	if to-from > int64(rc.maxRecordsToRecover) {
		from = to - int64(rc.maxRecordsToRecover)
		log.WithField("partition_id", partitionID).WithField("from_offset", fromOffset).WithField("to_offset", from).Warn("recoveryconsumer:  recovery for partition limited by 'parallelrecoverymaxrecords', not all data will be recovered")
	}

	// pass this request to recoverytracker in case rebalance / restart / another outage requires it to be resumed
	err := rc.tracker.AddRecoveryRequest(partitionID, from, to)
	if err != nil {
		log.WithError(err).Error("recoveryconsumer: failed to persist recovery request")
	}

	// recovery doesn't start until RefreshAssignments() runs next
}

// RefreshAssignments updates the current partitions that are being recovered when it's known that the set of recoveryrequests
// managed by RecoveryTracker may have changed.
func (rc *RecoveryConsumer) RefreshAssignments() error {
	// build a candidate set of assignments by matching the assigned partitions with the recovery requests from the tracker
	recoveryCandidates := make(map[int32]partitionRecoveryState)
	for _, partition := range rc.assignedPartitions {
		recoveryRequest := rc.tracker.GetRecoveryRequest(partition.Partition)
		recoveryState, partitionIsInRecovery := rc.activePartitionMap[partition.Partition]

		if recoveryRequest != nil {
			// use the most recent offset consumed if the partition is already in recovery, falling back to the fromOffset from the tracker request
			fromOffset := recoveryRequest.FromOffset
			if partitionIsInRecovery && recoveryState.fromOffset > fromOffset {
				fromOffset = recoveryState.fromOffset
			}

			recoveryPartition := kafka.TopicPartition{
				Topic:     partition.Topic,
				Partition: partition.Partition,
				Offset:    kafka.Offset(fromOffset),
			}

			recoveryState := partitionRecoveryState{
				partition:  recoveryPartition,
				fromOffset: fromOffset,
				toOffset:   recoveryRequest.ToOffset,
			}
			recoveryCandidates[partition.Partition] = recoveryState
		}
	}

	// check for changes, and only reassign to the consumer if we found a change in the number of assigned partitions or the partitionIDs
	if rc.partitionAssignmentsChanged(recoveryCandidates) {
		// lock in-process recovery for all partitions so that they stop processing records & don't overwrite the zero-value of the RecoveryRemaining gauge
		rc.partitionAssignmentLock.Lock()
		defer rc.partitionAssignmentLock.Unlock()

		// empty the channel
		err := rc.consumer.Unassign()
		if err != nil {
			log.WithError(err).Error("recoveryconsumer: failed to unassign partitions during RefreshAssignments")
		}
		for len(rc.consumer.Events()) > 0 {
			<-rc.consumer.Events()
		}

		var partitionsToAssign []kafka.TopicPartition
		for _, partition := range recoveryCandidates {
			log.WithField("partition_id", partition.partition.Partition).WithField("from_offset", partition.fromOffset).WithField("to_offset", partition.toOffset).Info("recoveryconsumer: assigning partition for recovery")
			rc.tracker.logRecoveryRequests(partition.partition.Partition)
			partitionsToAssign = append(partitionsToAssign, partition.partition)
		}

		rc.setActivePartitionMap(recoveryCandidates)
		return rc.consumer.Assign(partitionsToAssign)
	}

	return nil
}

// partitionAssignmentsEqual returns true iff the two arrays contain the same set of partitionIDs
func (rc *RecoveryConsumer) partitionAssignmentsChanged(candidates map[int32]partitionRecoveryState) bool {
	if len(candidates) == len(rc.activePartitionMap) {
		for _, candidate := range candidates {
			activePartitionRecoveryState, ok := rc.activePartitionMap[candidate.partition.Partition]
			if !ok { // this candidate is *not* in active recovery
				return true
			}
			if candidate.toOffset != activePartitionRecoveryState.toOffset {
				return true
			}
		}
		return false
	}
	return true
}

// setActivePartitionMap replaces the current active partition map with the passed values, and resets recovery metrics to
// clears any leftover values from the old partition assignments
func (rc *RecoveryConsumer) setActivePartitionMap(partitions map[int32]partitionRecoveryState) {
	// clear remaining recovery metrics from the *previous* assignments
	for _, partition := range rc.activePartitionMap {
		rc.metrics.RecoveryRemaining.WithLabelValues(strconv.Itoa(int(partition.partition.Partition))).Set(0.0)
	}

	rc.activePartitionMap = partitions

	rc.metrics.RecoveryPartitions.Set(float64(len(partitions)))
}

// SetAssignedPartitions updates the slice of partitions that are currently assigned in kafkaconsumer.
func (rc *RecoveryConsumer) SetAssignedPartitions(partitions []kafka.TopicPartition) {
	log.WithField("num_partitions", len(partitions)).Info("recoveryconsumer: kafkaconsumer updated assigned partitions")
	rc.assignedPartitions = partitions
}

// Shutdown stops the recovery consumer
func (rc *RecoveryConsumer) Shutdown() {
	log.Info("recoveryconsumer: shutdown initiated")
	rc.refreshTicker.Stop()
	rc.doneCh <- struct{}{}
	err := rc.consumer.Close()
	if err != nil {
		log.WithError(err).Error("recoveryconsumer: error while closing kafka consumer")
	}
	rc.tracker.Shutdown()
	log.Info("recoveryconsumer: consumer closed")
}
