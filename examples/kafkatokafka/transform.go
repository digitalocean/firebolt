package kafkatokafka

import (
	"errors"

	"github.internal.digitalocean.com/observability/firebolt"

	"github.internal.digitalocean.com/observability/firebolt/fbcontext"
)

// Transform is a firebolt node for ../..
type Transform struct {
	fbcontext.ContextAware
}

// Setup is a no-op in transform
func (t *Transform) Setup(config map[string]string) error {
	return nil
}

// Process transforms the data stream
func (t *Transform) Process(event *firebolt.Event) (*firebolt.Event, error) {
	// start with a type assertion because :sad-no-generics:
	eventBytes, ok := event.Payload.([]byte)
	if !ok {
		return nil, errors.New("failed type assertion for conversion to []byte")
	}

	result, err := t.transformEvent(eventBytes)

	return event.WithPayload(result), err
}

func (t *Transform) transformEvent(b []byte) (*firebolt.Event, error) {
	return nil, nil
}

// Shutdown is a no-op in syslogparser
func (t *Transform) Shutdown() error {
	return nil
}

// Receive handles a message from another node or an external source
func (t *Transform) Receive(msg fbcontext.Message) error {
	return errors.New("syslogparser: messaging not supported")
}
