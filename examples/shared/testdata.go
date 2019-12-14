package shared

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/node/kafkaconsumer"
)

var sourceTopic = "ktk-source"
var destTopic = "ktk-dest"

// ProduceTestData generates syslog entries onto the test topic.
func ProduceTestData(count int) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
	})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	for i := 0; i < count; i++ {
		log := fmt.Sprintf("<191>2021-01-02T15:04:05.999999-07:00 host.example.org test: @cee:{\"msg\":\"log %d\"}", i)
		kafkaMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &sourceTopic, Partition: kafka.PartitionAny},
			Value:          []byte(log),
		}
		p.ProduceChannel() <- kafkaMsg
	}
}

func ConsumeTestData(expected int) {
	ch := make(chan firebolt.Event, 1000)
	config := make(map[string]string)
	config["brokers"] = "localhost"
	config["consumergroup"] = "example-dest-group"
	config["topic"] = destTopic
	config["buffersize"] = "1000"
	consumer := &kafkaconsumer.KafkaConsumer{}
	consumer.Setup(config, ch)
	go consumer.Start()

	count := 0
	for {
		select {
		case result := <-ch:
			fmt.Println("got result: " + string(result.Payload.([]byte)))
			count++
			if count >= expected {
				fmt.Printf("SUCCESS: received %d results\n", count)
				go consumer.Shutdown()
				return
			}
		case <-time.After(60 * time.Second):
			fmt.Println("timeout consuming results after 60s")
			go consumer.Shutdown()
			return
		}
	}
}
