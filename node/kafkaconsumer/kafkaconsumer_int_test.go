// +build integration

package kafkaconsumer

import (
	"fmt"
	"testing"
	"time"

	"github.com/digitalocean/firebolt/testutil"

	"github.com/digitalocean/firebolt"

	"github.com/digitalocean/firebolt/metrics"

	"github.com/stretchr/testify/assert"

	"github.com/digitalocean/firebolt/node/kafkaproducer"
)

func TestKafkaConsumer(t *testing.T) {
	metrics.Init("kafkaconsumer")

	// using a unique (by time) topic name makes it easier to run this test repeatedly in local testing without interference
	// from the data left over by previous runs
	topicName := fmt.Sprintf("kafkaconsumer-%d", time.Now().UnixNano())

	// create a producer for test data
	kp := &kafkaproducer.KafkaProducer{}
	producerConfig := make(map[string]string)
	producerConfig["brokers"] = "localhost:9092"
	producerConfig["topic"] = topicName
	err := kp.Setup(producerConfig)
	assert.Nil(t, err)

	// produce 1000 records
	for i := 0; i < 1000; i++ {
		kp.Process(&firebolt.Event{
			Payload: []byte(fmt.Sprintf("record number %d", i)),
			Created: time.Now(),
		})
	}

	// sleep to give kafka time to autocreate the topic and have a leader for it's partition
	time.Sleep(10 * time.Second)

	// start the kafka consumer with 'maxpartitionlag' 60
	kc := &KafkaConsumer{}
	consumerConfig := make(map[string]string)
	consumerConfig["brokers"] = "localhost:9092"
	consumerConfig["consumergroup"] = "kafkaconsumer-inttest"
	consumerConfig["topic"] = topicName
	consumerConfig["buffersize"] = "100"
	consumerConfig["maxpartitionlag"] = "60"
	recordsCh := make(chan firebolt.Event, 1000)
	err = kc.Setup(consumerConfig, recordsCh)
	assert.Nil(t, err)
	go kc.Start()

	// the maxpartitionlag should take effect, and we get 240 records (4 partitions * 60) instead of all 1000
	err = testutil.AwaitCondition(func() bool {
		return len(recordsCh) == 240
	}, 500*time.Millisecond, 30*time.Second)
	if err != nil {
		fmt.Printf("failed, expected 240 found %d\n", len(recordsCh))
		t.FailNow()
	}

	for len(recordsCh) > 0 { // empty the channel
		<-recordsCh
	}

	// produce 1000 records
	for i := 0; i < 1000; i++ {
		kp.Process(&firebolt.Event{
			Payload: []byte(fmt.Sprintf("record number %d", i)),
			Created: time.Now(),
		})
	}

	// we should get them all this time
	time.Sleep(5 * time.Second)
	assert.Equal(t, 1000, len(recordsCh))

	err = kp.Shutdown()
	assert.Nil(t, err)
	err = kc.Shutdown()
	assert.Nil(t, err)
}
