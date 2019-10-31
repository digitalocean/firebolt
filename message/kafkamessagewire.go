package message

import (
	"time"
)

// wireMessage is the structure that we send on the Kafka topic
type wireMessage struct {
	Message     Message   `json:"message"`
	Updated     time.Time `json:"updated"`
	Acknowleged bool      `json:"ack"`
}

// uniqueKey concatenates messagetype with the key so that we don't risk collisions across message producers
func uniqueKey(msg Message) string {
	return msg.MessageType + "-" + msg.Key
}
