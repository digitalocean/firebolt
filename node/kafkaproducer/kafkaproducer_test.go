package kafkaproducer

import (
	"strings"
	"testing"

	"github.com/digitalocean/firebolt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func createValidConfig() map[string]string {
	config := make(map[string]string)
	config["brokers"] = "localhost:9092"
	config["topic"] = "testtopic"
	config["librdkafka.queue.buffering.max.messages"] = "10"
	return config
}

func TestBuildConfigMap(t *testing.T) {
	kc := &KafkaProducer{}
	config := createValidConfig()

	configMap, err := kc.buildConfigMap(config)
	assert.Nil(t, err)
	assert.Equal(t, "localhost:9092", (*configMap)["bootstrap.servers"])
	assert.Equal(t, "10", (*configMap)["queue.buffering.max.messages"]) // librdkafka override
	assert.Equal(t, 256000, (*configMap)["queue.buffering.max.kbytes"]) // librdkafka default
}

func TestSetupInvalidConfig(t *testing.T) {
	kp := &KafkaProducer{}
	config := createValidConfig()
	config["brokers"] = ""
	err := kp.Setup(config)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "kafkaproducer: missing or invalid value"))
}

func TestKafkaProducer(t *testing.T) {
	// without infrastructure set up for an integration test, best we can do is ensuring that msg makes it to the producer ch
	kp := &KafkaProducer{}
	config := createValidConfig()

	kp.Setup(config)
	assert.NotNil(t, kp.producer)
	assert.Equal(t, "testtopic", kp.topic)
	assert.NotNil(t, kp.stopChan)

	result, err := kp.Process(&firebolt.Event{
		Payload: []byte("<191>2006-01-02T15:04:05.999999-07:00 host.example.org test: @cee:{\"a\":\"b\"}\n"),
	})
	assert.Nil(t, err)
	assert.Nil(t, result) // kafkaproducer is a sink node, no data is passed to children
	//assert.Equal(t, 1, len(kp.producer.ProduceChannel()))

	err = kp.Shutdown()
	assert.Nil(t, err)
	assert.Panics(t, func() {
		kp.producer.ProduceChannel() <- &kafka.Message{} // confirm that the channel is closed, shutdown was successful
	})
}
