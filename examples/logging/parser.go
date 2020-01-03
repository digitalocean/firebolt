package logging

import (
	"errors"

	"github.com/digitalocean/captainslog"
	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/fbcontext"
	"github.com/digitalocean/firebolt/node"

	log "github.com/sirupsen/logrus"
)

var _ node.SyncNode = &Parser{}

// Parser is a firebolt `node.SyncNode` for parsing syslog messages from bytes using `digitalocean/captainslog`.  Because
// this is a `node.SyncNode`, its 'Process()' method must return immediately. A `node.AsyncNode` interface is also available
// for async operations such as bulk db writes.
type Parser struct {
	fbcontext.ContextAware
}

// Setup is a no-op in parser.   This is where you'd do any one-time setup, like establishing a db connection or populating
// a cache.
func (p *Parser) Setup(config map[string]string) error {
	return nil
}

// Process performs whatever work this node does on each event as it arrives.  This example takes the inbound `msg`, which
// is a byte array, and parses a `captainslog.SyslogMsg` from it.
func (p *Parser) Process(event *firebolt.Event) (*firebolt.Event, error) {
	// start with a type assertion because :sad-no-generics:
	msgBytes, ok := event.Payload.([]byte)
	if !ok {
		return nil, errors.New("failed type assertion for conversion to []byte")
	}

	// parse the log message into a struct using 'digitalocean/captainslog'
	original, err := captainslog.NewSyslogMsgFromBytes(msgBytes)
	if err != nil {
		log.WithField("raw_msg", string(msgBytes)).Debug("failed to parse log msg")

		// Returning an error.  You could add an `error_handler` to the node config in 'firebolt.yaml' to process these errors.
		// Because this is an error, it will not be passed to the child nodes of this node.  Alternately, returning nil,nil
		// would filter out the record and child nodes would not be called.
		return nil, err
	}

	// returning an event, this will be passed to the children of this node
	return event.WithPayload(original), nil
}

// Shutdown is a no-op in parser.   This is where you'd clean up any resources on application shutdown.
func (p *Parser) Shutdown() error {
	return nil
}

// Receive handles a message from another node or an external source.   This example doesn't use messaging.
func (p *Parser) Receive(msg fbcontext.Message) error {
	return errors.New("parser: messaging not supported")
}
