package testutil

import (
	"errors"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

func EnsureTestTopicExists(topicName string, partitions int32) error {
	// connect an adminclient
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Admin.Timeout = 90 * time.Second
	admin, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, config)
	if err != nil {
		log.WithError(err).Error("topics: failed to create cluster admin")
		return err
	}
	defer admin.Close()

	// create the topic
	err = admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		log.WithError(err).Error("failed to create test topic")
		return err
	}

	// wait for the topic to exist
	start := time.Now()
	for {
		elapsed := time.Since(start)
		if elapsed > (60 * time.Second) {
			return errors.New("timeout waiting for new topic to exist")
		}

		topicMap, err := admin.ListTopics()
		if err != nil {
			log.WithError(err).Error("topics: failed to list topics")
			return err
		}

		_, ok := topicMap[topicName]
		if ok {
			// topic exists!
			log.Info("topic created successfully")
			return nil
		}

		time.Sleep(200 * time.Millisecond)
	}

}
