package kafkaconsumer

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/fbcontext"
	kafkainterface "github.com/digitalocean/firebolt/kafka"
	"github.com/digitalocean/firebolt/metrics"
)

func createRecoveryConsumerConfig() map[string]string {
	config := make(map[string]string)
	config["brokers"] = "fakehost:9092"
	config["consumergroup"] = "testconsumergroup"
	config["topic"] = "testtopic"
	config["buffersize"] = "10"
	config["maxpartitionlag"] = "5"
	config["parallelrecoveryenabled"] = "true"
	config["parallelrecoverymaxrecords"] = "100"
	config["parallelrecoverymaxrate"] = "10"

	return config
}

func TestRecoveryConsumerSetup(t *testing.T) {
	metrics.Init("recoveryconsumer_test")

	ch := make(chan firebolt.Event)
	config := createRecoveryConsumerConfig()
	m := &Metrics{}
	m.RegisterConsumerMetrics()
	rc, err := NewRecoveryConsumer("logs-all", ch, config, m, nil)
	assert.Nil(t, err)
	assert.NotNil(t, rc.consumer)
}

func TestRefreshAssignments(t *testing.T) {
	metrics.Init("recoveryconsumer_test")

	rc := &RecoveryConsumer{}
	m := &Metrics{}
	m.RegisterConsumerMetrics()
	rc.metrics = m
	mockConsumer := &kafkainterface.MockMessageConsumer{}
	rc.consumer = mockConsumer
	rc.topic = "unit-test"
	rc.tracker = &RecoveryTracker{}

	// assigned partitions X recovery requests = active recovery partitions
	assigned := make([]kafka.TopicPartition, 4)
	assigned[0] = kafka.TopicPartition{
		Topic:     &rc.topic,
		Partition: 0,
		Offset:    kafka.Offset(0),
	}
	assigned[1] = kafka.TopicPartition{
		Topic:     &rc.topic,
		Partition: 1,
		Offset:    kafka.Offset(0),
	}
	assigned[2] = kafka.TopicPartition{
		Topic:     &rc.topic,
		Partition: 2,
		Offset:    kafka.Offset(0),
	}
	rc.assignedPartitions = assigned

	request1 := &RecoveryRequest{
		PartitionID: 1,
		FromOffset:  100,
		ToOffset:    200,
		Created:     time.Now(),
	}
	request2 := &RecoveryRequest{
		PartitionID: 2,
		FromOffset:  200,
		ToOffset:    300,
		Created:     time.Now(),
	}
	request3 := &RecoveryRequest{
		PartitionID: 3,
		FromOffset:  300,
		ToOffset:    400,
		Created:     time.Now(),
	}

	recoveryRequests := make(map[int32]*RecoveryRequests)
	recoveryRequests[1] = &RecoveryRequests{
		Requests: []*RecoveryRequest{request1},
	}
	recoveryRequests[2] = &RecoveryRequests{
		Requests: []*RecoveryRequest{request2},
	}
	recoveryRequests[3] = &RecoveryRequests{
		Requests: []*RecoveryRequest{request3},
	}
	rc.tracker.recoveryRequests = recoveryRequests

	mockConsumer.On("Events").Return(make(chan kafka.Event))
	mockConsumer.On("Unassign").Return(nil)
	mockConsumer.On("Assign", mock.AnythingOfType("[]kafka.TopicPartition")).Return(nil)
	rc.RefreshAssignments()

	// verify that the right assignments happened in rc.activePartitionMap
	assert.Equal(t, 2, len(rc.activePartitionMap))
	assert.Equal(t, kafka.Offset(100), rc.activePartitionMap[1].partition.Offset)
	assert.Equal(t, kafka.Offset(200), rc.activePartitionMap[2].partition.Offset)
}

func TestProcessRecoveryKafkaError(t *testing.T) {
	metrics.Init("recoveryconsumer_test")

	rc := &RecoveryConsumer{}
	mockConsumer := &kafkainterface.MockMessageConsumer{}
	rc.consumer = mockConsumer
	m := &Metrics{}
	m.RegisterConsumerMetrics()
	rc.metrics = m
	rc.topic = "unit-test"
	rc.tracker = &RecoveryTracker{}
	rc.tracker.recoveryRequests = make(map[int32]*RecoveryRequests)
	rc.tracker.metrics = m
	mockContext := &fbcontext.MockFBContext{}
	rc.tracker.ctx = mockContext

	// set up activePartitionMap and corresponding entries in RecoveryTracker
	rc.activePartitionMap = make(map[int32]partitionRecoveryState)
	partition0 := kafka.TopicPartition{
		Topic:     &rc.topic,
		Partition: 0,
		Offset:    kafka.Offset(0),
	}
	recoveryState0 := partitionRecoveryState{
		partition:  partition0,
		fromOffset: int64(0),
		toOffset:   int64(10000),
	}
	rc.activePartitionMap[0] = recoveryState0
	rc.tracker.recoveryRequests[0] = &RecoveryRequests{}
	rc.tracker.recoveryRequests[0].Requests = append(rc.tracker.recoveryRequests[0].Requests, &RecoveryRequest{
		PartitionID: 0,
		FromOffset:  int64(0),
		ToOffset:    int64(10000),
		Created:     time.Now(),
	})

	partition1 := kafka.TopicPartition{
		Topic:     &rc.topic,
		Partition: 1,
		Offset:    kafka.Offset(0),
	}
	recoveryState1 := partitionRecoveryState{
		partition:  partition1,
		fromOffset: int64(0),
		toOffset:   int64(20000),
	}
	rc.activePartitionMap[1] = recoveryState1
	rc.tracker.recoveryRequests[1] = &RecoveryRequests{}
	rc.tracker.recoveryRequests[1].Requests = append(rc.tracker.recoveryRequests[1].Requests, &RecoveryRequest{
		PartitionID: 1,
		FromOffset:  int64(0),
		ToOffset:    int64(20000),
		Created:     time.Now(),
	})
	rc.tracker.recoveryRequests[1].Requests = append(rc.tracker.recoveryRequests[1].Requests, &RecoveryRequest{
		PartitionID: 1,
		FromOffset:  int64(20000),
		ToOffset:    int64(30000),
		Created:     time.Now(),
	})

	// irrelevant error: nothing should happen
	rc.processError(kafka.ErrBadCompression)
	assert.NotNil(t, rc.tracker.recoveryRequests[0])
	assert.Equal(t, int64(0), rc.tracker.recoveryRequests[0].Requests[0].FromOffset)
	assert.Equal(t, 2, len(rc.tracker.recoveryRequests[1].Requests))

	// test the 'new low watermark is within the to/from range' case
	expectedMsgMatcher1 := func(msg fbcontext.Message) bool { // confirm that an expected message is sent
		return msg.MessageType == messageTypeRecoveryRequest &&
			msg.Key == "1"
	}
	mockConsumer.On("QueryWatermarkOffsets", rc.topic, int32(0), 10000).Return(int64(0), int64(100000), nil)
	mockConsumer.On("QueryWatermarkOffsets", rc.topic, int32(1), 10000).Return(int64(3675), int64(100000), nil) // low watermark is within the recovery To-From range
	mockContext.On("SendMessage", mock.MatchedBy(expectedMsgMatcher1)).Return(nil)
	rc.processError(kafka.ErrInvalidMsg)
	assert.Equal(t, int64(0), rc.tracker.recoveryRequests[0].Requests[0].FromOffset)
	assert.Equal(t, int64(3675), rc.tracker.recoveryRequests[1].Requests[0].FromOffset)

	// prev test will have reset the activePartitionMap; let's pretend to restart recovery for another test scenario
	rc.activePartitionMap[0] = recoveryState0
	rc.activePartitionMap[1] = recoveryState1

	// test the 'new low watermark is greater than the to/from range' case
	// we need to reset the mockConsumer to return new values
	mockConsumer = &kafkainterface.MockMessageConsumer{}
	rc.consumer = mockConsumer
	expectedCmdMatcher2 := func(msg fbcontext.Message) bool {
		return msg.MessageType == messageTypeRecoveryRequest &&
			msg.Key == "0"
	}
	mockConsumer.On("QueryWatermarkOffsets", rc.topic, int32(0), 10000).Return(int64(27000), int64(100000), nil) // low watermark is higher than the recovery ToOffset
	mockConsumer.On("QueryWatermarkOffsets", rc.topic, int32(1), 10000).Return(int64(0), int64(100000), nil)
	mockContext.On("SendMessage", mock.MatchedBy(expectedCmdMatcher2)).Return(nil)
	rc.processError(kafka.ErrOffsetOutOfRange)
	assert.Equal(t, 0, len(rc.tracker.recoveryRequests[0].Requests))
}
