package kafkatokafka

import (
	"encoding/json"
	"errors"

	"github.com/digitalocean/firebolt/node"

	"github.com/digitalocean/captainslog"
	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/fbcontext"
)

type jsonLog struct {
	Program string `json:"program"`
	Host    string `json:"host"`
	Msg     string `json:"message"`
}

var _ node.SyncNode = &JSONBuilder{}

// JSONBuilder is a firebolt `node.SyncNode` for building JSON extracts from syslog messages.
type JSONBuilder struct {
	fbcontext.ContextAware
}

// Setup is a no-op in jsonbuilder
func (j *JSONBuilder) Setup(config map[string]string) error {
	return nil
}

// Process takes the inbound `msg`, a captainslog.SyslogMsg, and builds a JSON string from it, returning that string as
// a []byte
func (j *JSONBuilder) Process(event *firebolt.Event) (*firebolt.Event, error) {

	// start with a type assertion because :sad-no-generics:
	log, ok := event.Payload.(captainslog.SyslogMsg)
	if !ok {
		return nil, errors.New("failed type assertion for conversion to captainslog.SyslogMsg")
	}

	// build a JSON string from some of the log fields
	jsonLog := &jsonLog{
		Program: log.Tag.Program,
		Host:    log.Host,
		Msg:     log.Content,
	}
	jsonBytes, err := json.Marshal(jsonLog)
	if err != nil {
		return nil, err
	}
	//println("built JSON: " + string(jsonBytes))

	return event.WithPayload(jsonBytes), nil
}

// Shutdown is a no-op in jsonbuilder
func (j *JSONBuilder) Shutdown() error {
	return nil
}

// Receive handles a message from another node or an external source
func (j *JSONBuilder) Receive(msg fbcontext.Message) error {
	return errors.New("parser: messaging not supported")
}
