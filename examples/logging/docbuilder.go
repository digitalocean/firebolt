package logging

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/digitalocean/firebolt/testutil"

	"github.com/digitalocean/firebolt/node/elasticsearch"

	"github.com/digitalocean/firebolt/node"

	"github.com/digitalocean/captainslog"
	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/fbcontext"
)

type jsonLogProgramNumeric struct {
	Program int    `json:"program"`
	Host    string `json:"host"`
	Msg     string `json:"message"`
}

type jsonLogProgramObject struct {
	Program program `json:"program"`
	Host    string  `json:"host"`
	Msg     string  `json:"message"`
}

type program struct {
	PID  int    `json:"pid"`
	Name string `json:"name"`
}

const esIndexName = "logs"

var _ node.SyncNode = &DocBuilder{}

// DocBuilder is a firebolt `node.SyncNode` for building elasticsearch IndexRequests from syslog messages.
// Because this is a `node.SyncNode`, its 'Process()' method must return immediately. A `node.AsyncNode` interface is
// also available for async operations such as bulk db writes.
type DocBuilder struct {
	fbcontext.ContextAware
}

// Setup ensures that the target Elasticsearch index exists.
func (d *DocBuilder) Setup(config map[string]string) error {
	testutil.CreateElasticsearchIndex(esIndexName)
	return nil
}

// Process takes the inbound `msg`, a captainslog.SyslogMsg, and builds a JSON string from it, returning that string as
// a []byte.
func (d *DocBuilder) Process(event *firebolt.Event) (*firebolt.Event, error) {

	// start with a type assertion because :sad-no-generics:
	log, ok := event.Payload.(captainslog.SyslogMsg)
	if !ok {
		return nil, errors.New("failed type assertion for conversion to captainslog.SyslogMsg")
	}

	pid, err := strconv.Atoi(log.Tag.Pid)
	if err != nil {
		fmt.Printf("ERROR: failed to convert log PID %s to integer\n", log.Tag.Pid)
	}

	// The first 100 documents are generated with the 'program' field as a numeric value, and after that it will be
	// an object.  These latter documents will fail to index into Elasticsearch, as the field has already been coerced
	// to numeric.
	// This allows us to demonstrate the 'error_handler' sending failed logs to a kafka topic.
	//
	var jsonLog interface{}
	if pid < 100 {
		jsonLog = &jsonLogProgramNumeric{
			Program: pid,
			Host:    log.Host,
			Msg:     log.Content,
		}
	} else {
		jsonLog = &jsonLogProgramObject{
			Program: program{
				PID:  pid,
				Name: log.Tag.Program,
			},
			Host: log.Host,
			Msg:  log.Content,
		}
	}

	// the 'elasticsearch' node that comes next expects 'IndexRequest' events
	indexRequest := elasticsearch.IndexRequest{
		Index: esIndexName,
		Doc:   jsonLog,
	}

	return event.WithPayload(indexRequest), nil
}

// Shutdown is a no-op in docbuilder.   This is where you'd clean up any resources on application shutdown.
func (d *DocBuilder) Shutdown() error {
	return nil
}

// Receive handles a message from another node or an external source.   This example doesn't use messaging.
func (d *DocBuilder) Receive(msg fbcontext.Message) error {
	return errors.New("docbuilder: messaging not supported")
}
