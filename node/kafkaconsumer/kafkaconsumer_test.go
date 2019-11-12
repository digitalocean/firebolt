package kafkaconsumer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/digitalocean/firebolt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"

	kafkainterface "github.com/digitalocean/firebolt/kafka"
	"github.com/digitalocean/firebolt/metrics"
)

func createValidConfig() map[string]string {
	config := make(map[string]string)
	config["brokers"] = "fakehost:9092"
	config["consumergroup"] = "testconsumergroup"
	config["topic"] = "testtopic"
	config["buffersize"] = "10"
	config["maxpartitionlag"] = "5"
	config["parallelrecoveryenabled"] = "false"

	return config
}

func TestCheckConfig(t *testing.T) {
	kc := &KafkaConsumer{}

	// valid
	config := createValidConfig()
	err := kc.checkConfig(config)
	assert.Nil(t, err)

	// missing brokers
	config = createValidConfig()
	config["brokers"] = ""
	err = kc.checkConfig(config)
	assert.NotNil(t, err)

	// missing consumergroup
	config = createValidConfig()
	config["consumergroup"] = ""
	err = kc.checkConfig(config)
	assert.NotNil(t, err)

	// missing topic
	config = createValidConfig()
	config["topic"] = ""
	err = kc.checkConfig(config)
	assert.NotNil(t, err)

	// buffersize NaN
	config = createValidConfig()
	config["buffersize"] = "NaN"
	err = kc.checkConfig(config)
	assert.NotNil(t, err)
}

func TestBuildConfigMap(t *testing.T) {
	kc := &KafkaConsumer{}
	config := createValidConfig()

	// add librdkafka overrides
	config["librdkafka.session.timeout.ms"] = "90000"
	config["librdkafka.some.madeup.property"] = "foobar"

	configMap, err := kc.buildConfigMap(config)
	assert.Nil(t, err)
	assert.Equal(t, "fakehost:9092", (*configMap)["bootstrap.servers"])
	assert.Equal(t, true, (*configMap)["go.events.channel.enable"])
	assert.Equal(t, "testconsumergroup", (*configMap)["group.id"])
	assert.Equal(t, "90000", (*configMap)["session.timeout.ms"])
	assert.Equal(t, "foobar", (*configMap)["some.madeup.property"])
}

func TestCheckConfigInvalidConfig(t *testing.T) {
	kc := &KafkaConsumer{}
	config := createValidConfig()

	// missing buffersize
	config["buffersize"] = ""
	err := kc.checkConfig(config)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "kafkaconsumer: missing or invalid value for config 'buffersize'"), err.Error())

	// invalid buffersize
	config["buffersize"] = "NaN"
	err = kc.checkConfig(config)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "kafkaconsumer: failed to convert config 'buffersize' to integer"), err.Error())

	// negative buffersize
	config["buffersize"] = "-100"
	err = kc.checkConfig(config)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "kafkaconsumer: invalid value for config 'buffersize', must be greater than zero"), err.Error())

	// invalid parallelrecoveryenabled
	config = createValidConfig()
	config["parallelrecoveryenabled"] = "not-a-boolean"
	err = kc.checkConfig(config)
	assert.NotNil(t, err)
	assert.Equal(t, "kafkaconsumer: invalid value for required config 'parallelrecoveryenabled', must be a boolean: [strconv.ParseBool: parsing \"not-a-boolean\": invalid syntax]", err.Error())
}

func TestBuildConfigMapInvalidConfig(t *testing.T) {
	kc := &KafkaConsumer{}
	config := createValidConfig()

	// invalid buffersize
	config["buffersize"] = "NaN"
	configMap, err := kc.buildConfigMap(config)
	assert.Nil(t, configMap)
	assert.NotNil(t, err)
	assert.Equal(t, "kafkaconsumer: failed to convert config 'buffersize' to integer", err.Error())
}

func TestSetup(t *testing.T) {
	metrics.Init("kafkaconsumer_test")

	ch := make(chan firebolt.Event)
	kc := &KafkaConsumer{}
	config := createValidConfig()
	err := kc.Setup(config, ch)
	assert.Nil(t, err)
	assert.NotNil(t, kc.consumer)
}

func TestSetupInvalidConfig(t *testing.T) {
	kc := &KafkaConsumer{}

	// invalid maxpartitionlag
	config := createValidConfig()
	config["maxpartitionlag"] = "NaN"
	err := kc.Setup(config, make(chan firebolt.Event))
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "kafkaconsumer: failed to convert config 'maxpartitionlag' to integer"), err.Error())
}

func TestCalculateOffsets(t *testing.T) {
	metrics.Init("kafkaconsumer_test")

	kc := &KafkaConsumer{}
	kc.maxInitialPartitionLag = 5
	mockConsumer := &kafkainterface.MockMessageConsumer{}
	kc.consumer = mockConsumer
	kc.topic = "unit-test"

	assigned := make([]kafka.TopicPartition, 4)
	assigned[0] = kafka.TopicPartition{
		Topic:     &kc.topic,
		Partition: 0,
		Offset:    kafka.Offset(0),
	}
	assigned[1] = kafka.TopicPartition{
		Topic:     &kc.topic,
		Partition: 1,
		Offset:    kafka.Offset(0),
	}
	assigned[2] = kafka.TopicPartition{
		Topic:     &kc.topic,
		Partition: 2,
		Offset:    kafka.Offset(0),
	}
	assigned[3] = kafka.TopicPartition{
		Topic:     &kc.topic,
		Partition: 3,
		Offset:    kafka.Offset(0),
	}

	committed := make([]kafka.TopicPartition, 4)
	committed[0] = kafka.TopicPartition{
		Topic:     &kc.topic,
		Partition: 0,
		Offset:    kafka.Offset(3),
	}
	committed[1] = kafka.TopicPartition{
		Topic:     &kc.topic,
		Partition: 1,
		Offset:    kafka.Offset(5),
	}
	committed[2] = kafka.TopicPartition{
		Topic:     &kc.topic,
		Partition: 2,
		Offset:    kafka.Offset(7),
	}
	committed[3] = kafka.TopicPartition{
		Topic:     &kc.topic,
		Partition: 2,
		Offset:    kafka.Offset(-1000),
	}

	// mock invocations
	mockConsumer.On("Committed", assigned, 10000).Return(committed, nil)
	mockConsumer.On("QueryWatermarkOffsets", kc.topic, int32(0), 10000).Return(int64(0), int64(10), nil)
	mockConsumer.On("QueryWatermarkOffsets", kc.topic, int32(1), 10000).Return(int64(0), int64(10), nil)
	mockConsumer.On("QueryWatermarkOffsets", kc.topic, int32(2), 10000).Return(int64(0), int64(10), nil)
	mockConsumer.On("QueryWatermarkOffsets", kc.topic, int32(3), 10000).Return(int64(0), int64(3), nil)

	resultOffsets, err := kc.calculateAssignmentOffsets(assigned)
	assert.Nil(t, err)
	assert.NotNil(t, resultOffsets)
	assert.Equal(t, 4, len(resultOffsets))
	assert.Equal(t, kafka.Offset(5), resultOffsets[0].Offset)
	assert.Equal(t, kafka.Offset(5), resultOffsets[1].Offset)
	assert.Equal(t, kafka.Offset(7), resultOffsets[2].Offset)
	assert.Equal(t, kafka.Offset(0), resultOffsets[3].Offset) // high watermark is 3, make sure 3-5 doesn't cause it to try to read offset -2
}

func TestProcessEvent(t *testing.T) {
	metrics.Init("kafkaconsumer_test")

	kc := &KafkaConsumer{}
	kc.maxInitialPartitionLag = 5
	mockConsumer := &kafkainterface.MockMessageConsumer{}
	kc.consumer = mockConsumer
	kc.topic = "unit-test"

	ch := make(chan firebolt.Event, 100) // it's not buffered at real runtime, but this makes assertions easier in the test
	config := createValidConfig()
	err := kc.Setup(config, ch)
	assert.Nil(t, err)

	// messages should flow through to the channel
	kc.processEvent(&kafka.Message{
		Value: ([]byte)("this is only a test"),
	})
	kc.processEvent(&kafka.Message{
		Value: ([]byte)("one more test"),
	})
	assert.Equal(t, 2, len(ch))
}

func TestProcessEventPartitionAssignment(t *testing.T) {
	metrics.Init("kafkaconsumer_test")

	kc := &KafkaConsumer{}
	kc.maxInitialPartitionLag = 5
	mockConsumer := &kafkainterface.MockMessageConsumer{}
	kc.consumer = mockConsumer
	kc.topic = "unit-test"

	// partition assignment
	partition := kafka.TopicPartition{
		Topic:     &kc.topic,
		Partition: 0,
	}
	partitions := []kafka.TopicPartition{partition}
	assigned := kafka.AssignedPartitions{
		Partitions: partitions,
	}
	// mocks for calculateAssignmentOffsets
	committed := kafka.TopicPartition{
		Topic:     &kc.topic,
		Partition: 0,
		Offset:    kafka.Offset(3),
	}
	mockConsumer.On("Committed", partitions, 10000).Return([]kafka.TopicPartition{committed}, nil)
	mockConsumer.On("QueryWatermarkOffsets", kc.topic, int32(0), 10000).Return(int64(0), int64(10), nil)
	expectedPartitionsWithOffsets, err := kc.calculateAssignmentOffsets(partitions)
	assert.Nil(t, err)
	// mocks for processEvent
	mockConsumer.On("Assign", expectedPartitionsWithOffsets).Return(nil)
	kc.processEvent(assigned)
}

// in this test we try assigning partitions with a mock that will make the assignment fail and enter a retry loop
// then we revoke the partitions, which should cancel the retry loop
// then we retry assigning partitions and confirm that the retry logic still works
func TestPartitionAssignmentRevocationAndRetry(t *testing.T) {
	metrics.Init("kafkaconsumer_test")

	kc := &KafkaConsumer{}
	kc.assignPartitionsCtx, kc.assignPartitionsCancel = context.WithCancel(context.Background())
	mockConsumer := &kafkainterface.MockMessageConsumer{} // replace the real consumer with a mock
	kc.consumer = mockConsumer
	kc.topic = "unit-test"

	// first we try to assign partitions but fail due to a kafka broker error fetching partition metadata
	partition1 := kafka.TopicPartition{
		Topic:     &kc.topic,
		Partition: 0,
	}
	partition2 := kafka.TopicPartition{
		Topic:     &kc.topic,
		Partition: 1,
	}
	partitions := []kafka.TopicPartition{partition1, partition2}
	assigned := kafka.AssignedPartitions{
		Partitions: partitions,
	}
	mockConsumer.On("Committed", partitions, 10000).Return(nil, errors.New("local: Operation in progress [1]"))
	kc.processEvent(assigned)

	time.Sleep(7 * time.Second) // should see a few "retrying in 3s" logged; that error ^ is preventing partition assignment

	mockConsumer.On("Unassign").Return(nil)
	kc.revokePartitionAssignments()
	time.Sleep(3 * time.Second)
	// should not see any "retrying in 3s" logs here

	// confirm that another retry can succeed
	kc.processEvent(assigned)
	time.Sleep(7 * time.Second) // should see a few "retrying in 3s" logged; that error ^ is preventing partition assignment

	mockConsumer.AssertNumberOfCalls(t, "Committed", 6)
	mockConsumer.AssertNumberOfCalls(t, "Unassign", 1)
}
