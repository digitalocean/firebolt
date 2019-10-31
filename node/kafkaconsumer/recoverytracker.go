package kafkaconsumer

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.internal.digitalocean.com/observability/firebolt/fbcontext"
)

// RecoveryTracker uses a Kafka compact topic to persist requests for partition data recovery.   It caches the most recent
// recoveryRequests
type RecoveryTracker struct {
	recoveryRequests map[int32]*RecoveryRequests
	requestLock      sync.RWMutex
	metrics          *Metrics
	ctx              fbcontext.FBContext
}

// RecoveryRequest is a request for recovery to fill in missed data for a single partition.
type RecoveryRequest struct {
	PartitionID int32     `json:"partition_id"`
	FromOffset  int64     `json:"from_offset"`
	ToOffset    int64     `json:"to_offset"`
	Created     time.Time `json:"created"`
}

// RecoveryRequests is the ordered list of active recoveries for a single partition.
type RecoveryRequests struct {
	Requests []*RecoveryRequest `json:"recovery_requests"`
}

// NewRecoveryTracker creates a RecoveryTracker which uses the messaging framework to manage the recovery process
func NewRecoveryTracker(metrics *Metrics, ctx fbcontext.FBContext) (*RecoveryTracker, error) {
	r := &RecoveryTracker{
		recoveryRequests: make(map[int32]*RecoveryRequests),
		metrics:          metrics,
		ctx:              ctx,
	}

	return r, nil
}

// GetRecoveryRequest returns the most recent RecoveryRequest for the partition passed.   It may return 'nil' if either
// no RecoveryRequest has ever been created for this partition or if the most recent RecoveryRequest for the partition
// has been marked complete.
func (rt *RecoveryTracker) GetRecoveryRequest(partitionID int32) *RecoveryRequest {
	rt.requestLock.RLock()
	defer rt.requestLock.RUnlock()

	// only return the first; recovery requests should be worked in order
	requests := rt.recoveryRequests[partitionID]
	if requests != nil && len(requests.Requests) > 0 {
		return requests.Requests[0]
	}
	return nil
}

// RecoveryRequestCount returns the number of currently tracked recovery requests.
func (rt *RecoveryTracker) RecoveryRequestCount() int {
	rt.requestLock.RLock()
	defer rt.requestLock.RUnlock()

	return len(rt.recoveryRequests)
}

// AddRecoveryRequest creates and persists a new recovery request for the passed partition
func (rt *RecoveryTracker) AddRecoveryRequest(partitionID int32, fromOffset int64, toOffset int64) error {
	log.WithField("partition_id", partitionID).WithField("from_offset", fromOffset).WithField("to_offset", toOffset).Warn("recoverytracker: requesting partition recovery")

	rt.requestLock.Lock()
	defer rt.requestLock.Unlock()

	requests := rt.recoveryRequests[partitionID]
	if requests == nil {
		requests = &RecoveryRequests{}
		rt.recoveryRequests[partitionID] = requests
	}

	// if this overlaps an existing request, merge them in-place
	overlapFound := false
	for _, request := range requests.Requests {
		// test for overlaps
		if fromOffset <= request.ToOffset && request.FromOffset <= toOffset {
			log.WithField("partition_id", partitionID).WithField("from_offset", request.FromOffset).
				WithField("to_offset", request.ToOffset).Info("recoverytracker: merging with existing recovery request")

			request.FromOffset = min(fromOffset, request.FromOffset)
			request.ToOffset = max(toOffset, request.ToOffset)
			overlapFound = true
		}
	}

	// otherwise create and add a new request
	if !overlapFound {
		request := &RecoveryRequest{
			PartitionID: partitionID,
			FromOffset:  fromOffset,
			ToOffset:    toOffset,
			Created:     time.Now(),
		}
		requests.Requests = append(requests.Requests, request)
	}
	return rt.sendRecoveryRequests(partitionID, requests)
}

// UpdateRecoveryRequest persists the passed request to recover skipped data for a partition to a Kafka compact topic.
func (rt *RecoveryTracker) UpdateRecoveryRequest(partitionID int32, fromOffset int64, toOffset int64) error {
	rt.requestLock.RLock()
	defer rt.requestLock.RUnlock()

	// replace the request with the updated version
	requests := rt.recoveryRequests[partitionID]
	if requests != nil && len(requests.Requests) > 0 {
		request := requests.Requests[0]
		if request.ToOffset == toOffset {
			// only fromOffset should ever get updated
			request.FromOffset = fromOffset

			// after updating the fromOffset, recalculate metrics
			rt.updateMetrics(partitionID)

			return rt.sendRecoveryRequests(partitionID, requests)
		}
		log.WithField("partition_id", partitionID).WithField("update_to_offset", toOffset).WithField("existing_to_offset", request.ToOffset).Error("recoverytracker: attempt to update recovery request for to_offset that does not match the first request")
		return fmt.Errorf("failed to update recovery request for partition %d", partitionID)
	}
	log.WithField("partition_id", partitionID).Error("recoverytracker: attempt to update recovery request when no requests exist for this partition")
	return fmt.Errorf("failed to update recovery request for partition %d", partitionID)
}

// recovery remaining metrics
func (rt *RecoveryTracker) updateMetrics(partitionID int32) {
	requests := rt.recoveryRequests[partitionID]
	var remainingEvents float64
	for _, request := range requests.Requests {
		remainingEvents += float64(request.ToOffset - request.FromOffset)
	}
	rt.metrics.RecoveryRemaining.WithLabelValues(strconv.Itoa(int(partitionID))).Set(remainingEvents)
}

// MarkRecoveryComplete sends an updated RecoveryRequest message to mark the recovery complete.
func (rt *RecoveryTracker) MarkRecoveryComplete(partitionID int32, toOffset int64) error {
	log.WithField("partition_id", partitionID).WithField("to_offset", toOffset).Info("recoverytracker: marking partition recovery complete")
	rt.requestLock.RLock()
	defer rt.requestLock.RUnlock()

	requests := rt.recoveryRequests[partitionID]
	if requests == nil {
		return fmt.Errorf("failed to mark recovery complete for partition %d, no recovery requests exist for this partition", partitionID)
	}

	// remove the completed recovery request
	removedRequest := false
	var retained []*RecoveryRequest
	for idx, request := range requests.Requests {
		if request.ToOffset == toOffset {
			removedRequest = true
			log.WithField("index", idx).WithField("partition_id", partitionID).WithField("to_offset", toOffset).Info("recoverytracker: removed matching recovery request")
		} else {
			retained = append(retained, request)
		}
	}

	if removedRequest {
		// apply the updated requests and notify the other instances in the cluster
		requests.Requests = retained
		rt.updateMetrics(partitionID)
		return rt.sendRecoveryRequests(partitionID, requests)
	}
	log.WithField("partition_id", partitionID).WithField("to_offset", toOffset).Error("recoverytracker: failed to mark recovery complete, no current request matched the provided to_offset, logging recovery requests")
	rt.logRecoveryRequests(partitionID)
	return fmt.Errorf("failed to update recovery request for partition %d", partitionID)
}

func (rt *RecoveryTracker) logRecoveryRequests(partitionID int32) {
	requests := rt.recoveryRequests[partitionID]
	for idx, request := range requests.Requests {
		log.WithField("index", idx).WithField("partition_id", partitionID).WithField("from_offset", request.FromOffset).WithField("to_offset", request.ToOffset).Info("recoverytracker: print recovery request")
	}
}

func (rt *RecoveryTracker) cancelAll() error {
	rt.requestLock.RLock()
	defer rt.requestLock.RUnlock()

	for partition, recoveryRequests := range rt.recoveryRequests {
		log.WithField("partition_id", partition).Info("recoverytracker: operator requested cancel of all recovery for partition")
		recoveryRequests.Requests = make([]*RecoveryRequest, 0)
		rt.updateMetrics(partition)
		err := rt.sendRecoveryRequests(partition, recoveryRequests)
		if err != nil {
			return err
		}
	}
	return nil
}

// produceRecoveryRequests produces the RecoveryRequests onto the tracking Kafka topic.
func (rt *RecoveryTracker) sendRecoveryRequests(partitionID int32, requests *RecoveryRequests) error {
	requestBytes, err := json.Marshal(requests)
	if err != nil {
		return fmt.Errorf("recoverytracker: failed to marshal recoveryrequests [%v]", err)
	}

	msg := fbcontext.Message{
		MessageType: messageTypeRecoveryRequest,
		Key:         strconv.Itoa(int(partitionID)),
		Payload:     requestBytes,
	}

	if rt.ctx == nil {
		log.Warn("recoverytracker: detected nil fbcontext")
		return nil
	}
	return rt.ctx.SendMessage(msg)
}

func (rt *RecoveryTracker) receiveRequest(partitionIDStr string, requestBytes []byte) {
	rt.requestLock.Lock()
	defer rt.requestLock.Unlock()

	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil {
		log.WithError(err).Errorf("recoverytracker: failed to convert recovery request key to partitionID int")
	}

	requests := &RecoveryRequests{}
	err = json.Unmarshal(requestBytes, requests)
	if err != nil {
		log.WithError(err).Error("recoverytracker: failed to unmarshal recovery requests")
		return
	}

	rt.recoveryRequests[int32(partitionID)] = requests
}

// Shutdown stops the recovery tracker
func (rt *RecoveryTracker) Shutdown() {
}

// max returns the larger of x or y
func max(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}

// min returns the smaller of x or y
func min(x, y int64) int64 {
	if x > y {
		return y
	}
	return x
}
