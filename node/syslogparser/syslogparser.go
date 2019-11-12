package syslogparser

import (
	"errors"

	"github.com/digitalocean/firebolt"

	"github.com/digitalocean/captainslog"
	log "github.com/sirupsen/logrus"

	"github.com/digitalocean/firebolt/fbcontext"
)

// SyslogParser is a firebolt node for parsing syslog messages from bytes using Captainslog.
type SyslogParser struct {
	fbcontext.ContextAware
}

// Setup is a no-op in syslogparser
func (s *SyslogParser) Setup(config map[string]string) error {
	return nil
}

// Process takes the inbound `msg`, which is a byte array, and parses a captainslog.SyslogMsg from it
func (s *SyslogParser) Process(event *firebolt.Event) (*firebolt.Event, error) {

	// start with a type assertion because :sad-no-generics:
	msgBytes, ok := event.Payload.([]byte)
	if !ok {
		return nil, errors.New("failed type assertion for conversion to []byte")
	}

	original, err := captainslog.NewSyslogMsgFromBytes(msgBytes)
	if err != nil {
		log.WithField("raw_msg", string(msgBytes)).Debug("failed to parse syslog msg")
		return nil, err
	}

	return event.WithPayload(original), nil
}

// Shutdown is a no-op in syslogparser
func (s *SyslogParser) Shutdown() error {
	return nil
}

// Receive handles a message from another node or an external source
func (s *SyslogParser) Receive(msg fbcontext.Message) error {
	return errors.New("syslogparser: messaging not supported")
}
