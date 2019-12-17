package kafkatokafka

import (
	"errors"

	"github.com/digitalocean/captainslog"
	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/fbcontext"
	"github.com/digitalocean/firebolt/node"

	log "github.com/sirupsen/logrus"
)

var _ node.SyncNode = &Parser{}

// Parser is a firebolt `node.SyncNode` for parsing syslog messages from bytes using `digitalocean/captainslog`.
type Parser struct {
	fbcontext.ContextAware
}

// Setup is a no-op in parser
func (p *Parser) Setup(config map[string]string) error {
	return nil
}

// Process takes the inbound `msg`, which is a byte array, and parses a `captainslog.SyslogMsg` from it
func (p *Parser) Process(event *firebolt.Event) (*firebolt.Event, error) {
	// start with a type assertion because :sad-no-generics:
	msgBytes, ok := event.Payload.([]byte)
	if !ok {
		return nil, errors.New("failed type assertion for conversion to []byte")
	}

	original, err := captainslog.NewSyslogMsgFromBytes(msgBytes)
	if err != nil {
		log.WithField("raw_msg", string(msgBytes)).Debug("failed to parse log msg")
		return nil, err
	}

	return event.WithPayload(original), nil
}

// Shutdown is a no-op in parser
func (p *Parser) Shutdown() error {
	return nil
}

// Receive handles a message from another node or an external source
func (p *Parser) Receive(msg fbcontext.Message) error {
	return errors.New("parser: messaging not supported")
}
