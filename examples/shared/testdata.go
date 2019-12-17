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

	// wait for acknowledgement for all messages before returning
	doneChan := make(chan bool)
	go func() {
		responseCount := 0
		defer close(doneChan)
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				responseCount++
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				}
				if responseCount >= count {
					return
				}
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	for i := 0; i < count; i++ {
		log := fmt.Sprintf("<191>2021-01-02T15:04:05.999999-07:00 host.example.org test: @cee:{\"msg\":\"log %d\"}", i)
		kafkaMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &sourceTopic, Partition: kafka.PartitionAny},
			Value:          []byte(log),
		}
		p.ProduceChannel() <- kafkaMsg
	}

	_ = <-doneChan
}

func ConsumeTestData(topic string, expected int) {
	ch := make(chan firebolt.Event, 1000)
	config := make(map[string]string)
	config["brokers"] = "localhost"
	config["consumergroup"] = "example-dest-group"
	config["topic"] = topic
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
				fmt.Printf("***\n***SUCCESS: received %d results\n***\n", count)
				go consumer.Shutdown()
				return
			}
		case <-time.After(60 * time.Second):
			fmt.Print("***\n***FAILED: timeout consuming results after 60s\n***\n")
			go consumer.Shutdown()
			return
		}
	}
}
