package kafkaconsumer

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.internal.digitalocean.com/observability/firebolt/fbcontext"
	"github.internal.digitalocean.com/observability/firebolt/metrics"
)

func expectedMsgMatcher(_ fbcontext.Message) bool { // confirm that an expected message is sent
	return true
}

func TestAddRecoveryRequest(t *testing.T) {
	metrics.Init("test")
	m := &Metrics{}
	m.RegisterConsumerMetrics()
	mockContext := &fbcontext.MockFBContext{}
	rt, err := NewRecoveryTracker(m, mockContext)
	assert.Nil(t, err)

	// add some recovery requests
	mockContext.On("SendMessage", mock.MatchedBy(expectedMsgMatcher)).Return(nil)
	rt.AddRecoveryRequest(0, 1000, 2000)
	rt.AddRecoveryRequest(1, 2000, 3000)
	rt.AddRecoveryRequest(1, 12000, 13000)
	rt.AddRecoveryRequest(2, 3000, 4000)
	assert.Equal(t, 3, len(rt.recoveryRequests))
	assert.Equal(t, 1, len(rt.recoveryRequests[0].Requests))
	assert.Equal(t, 2, len(rt.recoveryRequests[1].Requests))
	assert.Equal(t, 1, len(rt.recoveryRequests[2].Requests))

	// add duplicate requests
	rt.AddRecoveryRequest(0, 1000, 2500)
	rt.AddRecoveryRequest(1, 12000, 13500)
	assert.Equal(t, 3, len(rt.recoveryRequests))
	assert.Equal(t, 1, len(rt.recoveryRequests[0].Requests))
	assert.Equal(t, 2, len(rt.recoveryRequests[1].Requests))
	assert.Equal(t, 1, len(rt.recoveryRequests[2].Requests))
	assert.Equal(t, int64(2500), rt.recoveryRequests[0].Requests[0].ToOffset)
	assert.Equal(t, int64(13500), rt.recoveryRequests[1].Requests[1].ToOffset)

	// add an overlapping request
	rt.AddRecoveryRequest(1, 12500, 14000)
	assert.Equal(t, 3, len(rt.recoveryRequests))
	assert.Equal(t, 1, len(rt.recoveryRequests[0].Requests))
	assert.Equal(t, 2, len(rt.recoveryRequests[1].Requests))
	assert.Equal(t, 1, len(rt.recoveryRequests[2].Requests))
	assert.Equal(t, int64(2000), rt.recoveryRequests[1].Requests[0].FromOffset)
	assert.Equal(t, int64(3000), rt.recoveryRequests[1].Requests[0].ToOffset)
	assert.Equal(t, int64(12000), rt.recoveryRequests[1].Requests[1].FromOffset)
	assert.Equal(t, int64(14000), rt.recoveryRequests[1].Requests[1].ToOffset)
}

func TestReceiveRecoveryRequest(t *testing.T) {
	rt := buildTrackerWithRequests(nil)

	assert.Equal(t, 2, len(rt.recoveryRequests))
	assert.Equal(t, 1, len(rt.recoveryRequests[0].Requests))
	assert.Equal(t, 2, len(rt.recoveryRequests[1].Requests))
	assert.Equal(t, int64(1000), rt.recoveryRequests[1].Requests[0].FromOffset)
	assert.Equal(t, int64(20000), rt.recoveryRequests[1].Requests[0].ToOffset)
}

func TestMarkRecoveryComplete(t *testing.T) {
	mockContext := &fbcontext.MockFBContext{}
	rt := buildTrackerWithRequests(mockContext)

	mockContext.On("SendMessage", mock.MatchedBy(expectedMsgMatcher)).Return(nil)

	assert.Equal(t, 2, len(rt.recoveryRequests))
	assert.Equal(t, 1, len(rt.recoveryRequests[0].Requests))
	assert.Equal(t, 2, len(rt.recoveryRequests[1].Requests))

	err := rt.MarkRecoveryComplete(0, int64(10000))
	assert.Nil(t, err)
	err = rt.MarkRecoveryComplete(1, int64(20000))
	assert.Nil(t, err)

	assert.Equal(t, 2, len(rt.recoveryRequests))
	assert.Equal(t, 0, len(rt.recoveryRequests[0].Requests))
	assert.Equal(t, 1, len(rt.recoveryRequests[1].Requests))

	// toOffset does not match expected
	err = rt.MarkRecoveryComplete(1, int64(123435))
	assert.NotNil(t, err)
	assert.Equal(t, "failed to update recovery request for partition 1", err.Error())

	// partition has no pending recovery requests
	err = rt.MarkRecoveryComplete(2, int64(123435))
	assert.NotNil(t, err)
	assert.Equal(t, "failed to mark recovery complete for partition 2, no recovery requests exist for this partition", err.Error())
}

func TestCancelAll(t *testing.T) {
	mockContext := &fbcontext.MockFBContext{}
	rt := buildTrackerWithRequests(mockContext)

	mockContext.On("SendMessage", mock.MatchedBy(expectedMsgMatcher)).Return(nil)

	assert.Equal(t, 2, len(rt.recoveryRequests))
	assert.Equal(t, 1, len(rt.recoveryRequests[0].Requests))
	assert.Equal(t, 2, len(rt.recoveryRequests[1].Requests))

	err := rt.cancelAll()
	assert.Nil(t, err)

	assert.Equal(t, 2, len(rt.recoveryRequests))
	assert.Equal(t, 0, len(rt.recoveryRequests[0].Requests))
	assert.Equal(t, 0, len(rt.recoveryRequests[1].Requests))
}

func buildTrackerWithRequests(ctx fbcontext.FBContext) *RecoveryTracker {
	metrics.Init("test")
	m := &Metrics{}
	m.RegisterConsumerMetrics()
	rt, _ := NewRecoveryTracker(m, ctx)

	recoveryRequests0 := &RecoveryRequests{}
	recoveryRequests0.Requests = append(recoveryRequests0.Requests, &RecoveryRequest{
		PartitionID: 0,
		FromOffset:  int64(0),
		ToOffset:    int64(10000),
		Created:     time.Now(),
	})
	requestBytes, _ := json.Marshal(recoveryRequests0)
	rt.receiveRequest("0", requestBytes)

	recoveryRequests1 := &RecoveryRequests{}
	recoveryRequests1.Requests = append(recoveryRequests1.Requests, &RecoveryRequest{
		PartitionID: 1,
		FromOffset:  int64(1000),
		ToOffset:    int64(20000),
		Created:     time.Now(),
	})
	recoveryRequests1.Requests = append(recoveryRequests1.Requests, &RecoveryRequest{
		PartitionID: 1,
		FromOffset:  int64(20000),
		ToOffset:    int64(30000),
		Created:     time.Now(),
	})
	requestBytes, _ = json.Marshal(recoveryRequests1)
	rt.receiveRequest("1", requestBytes)

	return rt
}
