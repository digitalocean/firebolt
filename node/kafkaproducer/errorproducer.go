package kafkaproducer

import (
	"encoding/json"
	"errors"

	log "github.com/sirupsen/logrus"

	"github.com/digitalocean/firebolt"
)

// ErrorProducer is a firebolt node for producing EventError messages onto a Kafka topic.
type ErrorProducer struct {
	KafkaProducer
}

// Process sends an EventError to the configured Kafka topic as a JSON string.
func (ep *ErrorProducer) Process(event *firebolt.Event) (*firebolt.Event, error) {
	eventError, ok := event.Payload.(firebolt.EventError)
	if !ok {
		return nil, errors.New("errorproducer: failed type assertion for conversion to firebolt.EventError")
	}

	var errBytes []byte
	errBytes, err := json.Marshal(eventError)
	if err != nil {
		log.WithError(err).WithField("node_id", ep.ID).Warn("failed to marshal error to json, error_handler data lost")

		// if the event was not marshallable to json, return the error instead so that the user can figure out what happened
		eventError.Event = err
		errBytes, _ = json.Marshal(eventError)
	}

	return ep.KafkaProducer.Process(&firebolt.Event{
		Payload: ProduceRequest{
			Message: errBytes,
		},
		Created: event.Created,
	})
}
