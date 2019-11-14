package internal

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/digitalocean/firebolt/node/elasticsearch"

	log "github.com/sirupsen/logrus"

	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/fbcontext"
	"github.com/digitalocean/firebolt/node"
)

var (
	// SuccessEvents collects events that successfully reach ResultsNode
	SuccessEvents = make(chan string, 100000)

	// FilteredEvents collects events that are filtered out by FilterNode
	FilteredEvents = make(chan string, 100000)

	// ErrorEvents collects events that return errors in ErrorNode
	ErrorEvents = make(chan string, 100000)

	// ErrorHandlerEvents collects events that return errors in ErrorHandlerNode
	ErrorHandlerEvents = make(chan interface{}, 100000)

	// FilterNodeMessages collects messages received by FilterNode
	FilterNodeMessages = make(chan fbcontext.Message, 100000)

	// ResultsNodeMessages collects messages received by ResultsNode
	ResultsNodeMessages = make(chan fbcontext.Message, 100000)

	// AsyncPassedEvents collects messages passed through by AsyncFilterNode
	AsyncPassedEvents = make(chan string, 100000)

	// AsyncFilteredEvents collects messages filtered out by AsyncFilterNode
	AsyncFilteredEvents = make(chan string, 100000)
)

// RegisterTestNodeTypes registers every source and node type in this file so that they're ready to be used in tests
func RegisterTestNodeTypes() {
	node.GetRegistry().RegisterSourceType("simplesource", func() node.Source {
		return &SimpleSource{}
	}, reflect.TypeOf(([]byte)(nil)))

	node.GetRegistry().RegisterNodeType("filternode", func() node.Node {
		return &FilterNode{}
	}, reflect.TypeOf(([]byte)(nil)), reflect.TypeOf(""))

	node.GetRegistry().RegisterNodeType("errornode", func() node.Node {
		return &ErrorNode{}
	}, reflect.TypeOf(""), reflect.TypeOf(""))

	node.GetRegistry().RegisterNodeType("errorhandlernode", func() node.Node {
		return &ErrorHandlerNode{}
	}, reflect.TypeOf(&firebolt.EventError{}), nil)

	node.GetRegistry().RegisterNodeType("resultsnode", func() node.Node {
		return &ResultsNode{}
	}, reflect.TypeOf(""), reflect.TypeOf(""))

	node.GetRegistry().RegisterNodeType("slownode", func() node.Node {
		return &SlowNode{}
	}, reflect.TypeOf(""), reflect.TypeOf(""))

	node.GetRegistry().RegisterNodeType("stringtobytesnode", func() node.Node {
		return &StringToBytesNode{}
	}, reflect.TypeOf(""), reflect.TypeOf(([]byte)(nil)))

	node.GetRegistry().RegisterNodeType("asyncfilternode", func() node.Node {
		return &AsyncFilterNode{}
	}, reflect.TypeOf(([]byte)(nil)), reflect.TypeOf(""))

	node.GetRegistry().RegisterNodeType("indexrequestbuildernode", func() node.Node {
		return &IndexRequestBuilderNode{}
	}, reflect.TypeOf(""), reflect.TypeOf(elasticsearch.IndexRequest{}))
}

// SimpleSource is a source for testing that produces test records and exits
type SimpleSource struct {
	fbcontext.ContextAware
	sendchan chan firebolt.Event
	donechan chan bool
}

// Setup instantiates and configures the Source
func (s *SimpleSource) Setup(config map[string]string, recordschan chan firebolt.Event) error {
	s.sendchan = recordschan
	s.donechan = make(chan bool, 1)
	return nil
}

// Start runs the Source
func (s *SimpleSource) Start() error {
	for i := 0; i < 10; i++ {
		s.sendchan <- firebolt.Event{
			Payload: []byte("success " + strconv.Itoa(i)),
		}
	}
	for j := 0; j < 5; j++ {
		s.sendchan <- firebolt.Event{
			Payload: []byte("filterme " + strconv.Itoa(j)),
		}
	}
	for k := 0; k < 3; k++ {
		s.sendchan <- firebolt.Event{
			Payload: []byte("error " + strconv.Itoa(k)),
		}
	}

	// wait to be done
	for len(s.donechan) == 0 {
		time.Sleep(500 * time.Millisecond)
		log.Info("simple source waiting for shutdown")
	}
	log.Info("simple source shutting down")

	return nil
}

// Shutdown stops the Source and cleans up any resources used
func (s *SimpleSource) Shutdown() error {
	log.Info("simplesource: shutdown called")
	s.donechan <- true
	return nil
}

// Receive handles a message from another node or an external source
func (s *SimpleSource) Receive(msg fbcontext.Message) error {
	return errors.New("simplesource: message not supported")
}

// FilterNode is a Node that filters out any events starting with 'filter'
type FilterNode struct {
	fbcontext.ContextAware
}

// Setup initializes the node so that it's ready for processing
func (f *FilterNode) Setup(config map[string]string) error {
	f.Subscribe([]string{"IntTestMessage", "OtherMessage"})
	return nil
}

// Process handles the event and returns an optional result, and an optional error
func (f *FilterNode) Process(event *firebolt.Event) (*firebolt.Event, error) {
	bytes, ok := event.Payload.([]byte)
	if !ok {
		return nil, errors.New("failed type assertion for conversion to []byte")
	}
	str := string(bytes)
	if !strings.HasPrefix(str, "filter") {
		newevent := &firebolt.Event{
			Payload: str,
			Created: event.Created,
		}
		return newevent, nil
	}
	FilteredEvents <- str
	return nil, nil
}

// Shutdown provides an opportunity for the Node to clean up resources on shutdown
func (f *FilterNode) Shutdown() error {
	return nil
}

// Receive handles a message from another node or an external source
func (f *FilterNode) Receive(msg fbcontext.Message) error {
	FilterNodeMessages <- msg
	return nil
}

// SendMessage lets unit/integration tests send a message through the fbcontext.
func (f *FilterNode) SendMessage(msg fbcontext.Message) error {
	return f.Ctx.SendMessage(msg)
}

// AckMessage lets unit/integration tests acknowledge receipt of a message.
func (f *FilterNode) AckMessage(msg fbcontext.Message) error {
	return f.Ctx.AckMessage(msg)
}

// ErrorNode is a Node that returns an error for any events starting with 'error'
type ErrorNode struct {
	fbcontext.ContextAware
}

// Setup is a no-op in errornode
func (e *ErrorNode) Setup(config map[string]string) error {
	return nil
}

// Process handles the event and returns an optional result, and an optional error
func (e *ErrorNode) Process(event *firebolt.Event) (*firebolt.Event, error) {
	str, ok := event.Payload.(string)
	if !ok {
		return nil, errors.New("failed type assertion for conversion to string")
	}
	if !strings.HasPrefix(str, "error") {
		return event, nil
	}
	ErrorEvents <- str
	return nil, errors.New("so erroneous")
}

// Shutdown provides an opportunity for the Node to clean up resources on shutdown
func (e *ErrorNode) Shutdown() error {
	return nil
}

// Receive handles a message from another node or an external source
func (e *ErrorNode) Receive(msg fbcontext.Message) error {
	return errors.New("message not supported")
}

// ErrorHandlerNode is a Node used as `error_handler` that simply puts the events it gets on a channel
type ErrorHandlerNode struct {
	fbcontext.ContextAware
}

// Setup is a no-op in errorhandlernode
func (ehn *ErrorHandlerNode) Setup(config map[string]string) error {
	return nil
}

// Process handles the event and returns an optional result, and an optional error
func (ehn *ErrorHandlerNode) Process(event *firebolt.Event) (*firebolt.Event, error) {
	ErrorHandlerEvents <- event
	return nil, nil
}

// Shutdown provides an opportunity for the Node to clean up resources on shutdown
func (ehn *ErrorHandlerNode) Shutdown() error {
	return nil
}

// Receive handles a message from another node or an external source
func (ehn *ErrorHandlerNode) Receive(msg fbcontext.Message) error {
	return errors.New("message not supported")
}

// ResultsNode is a Node that writes all result data to an array for inspection & assertions
type ResultsNode struct {
	fbcontext.ContextAware
}

// Setup is a no-op in resultsnode
func (r *ResultsNode) Setup(config map[string]string) error {
	return nil
}

// Process handles the event and returns an optional result, and an optional error
func (r *ResultsNode) Process(event *firebolt.Event) (*firebolt.Event, error) {
	str, ok := event.Payload.(string)
	if !ok {
		return nil, errors.New("failed type assertion for conversion to string")
	}
	// sleep to simulate a slow node that does I/O
	time.Sleep(200 * time.Millisecond)
	if !strings.HasPrefix(str, "success") {
		return nil, errors.New("only success should have reached this node")
	}
	SuccessEvents <- str
	return event, nil
}

// Shutdown provides an opportunity for the Node to clean up resources on shutdown
func (r *ResultsNode) Shutdown() error {
	return nil
}

// Receive handles a message from another node or an external source
func (r *ResultsNode) Receive(msg fbcontext.Message) error {
	ResultsNodeMessages <- msg
	return nil
}

// StringToBytesNode is a Node that converts strings to []byte
type StringToBytesNode struct {
	fbcontext.ContextAware
}

// Setup is a no-op in StringToBytesNode
func (s *StringToBytesNode) Setup(config map[string]string) error {
	return nil
}

// Process handles the event and returns an optional result, and an optional error
func (s *StringToBytesNode) Process(event *firebolt.Event) (*firebolt.Event, error) {
	str, ok := event.Payload.(string)
	if !ok {
		return nil, errors.New("failed type assertion for conversion to string")
	}

	bytes := []byte(str)
	return event.WithPayload(bytes), nil
}

// Shutdown provides an opportunity for the Node to clean up resources on shutdown
func (s *StringToBytesNode) Shutdown() error {
	return nil
}

// Receive handles a message from another node or an external source
func (s *StringToBytesNode) Receive(msg fbcontext.Message) error {
	return errors.New("message not supported")
}

// SlowNode is a Node that sleeps for 5s while processing each message
type SlowNode struct {
	fbcontext.ContextAware
}

// Setup is a no-op in slownode
func (r *SlowNode) Setup(config map[string]string) error {
	return nil
}

// Process handles the event and returns an optional result, and an optional error
func (r *SlowNode) Process(event *firebolt.Event) (*firebolt.Event, error) {
	str, ok := event.Payload.(string)
	if !ok {
		return nil, errors.New("failed type assertion for conversion to string")
	}
	// sleep, we want this processing to be slow so that shutdown waitgroup times out
	time.Sleep(5 * time.Second)
	SuccessEvents <- str
	return event, nil
}

// Shutdown provides an opportunity for the Node to clean up resources on shutdown
func (r *SlowNode) Shutdown() error {
	return nil
}

// Receive handles a message from another node or an external source
func (r *SlowNode) Receive(msg fbcontext.Message) error {
	return errors.New("message not supported")
}

// AsyncFilterNode is a AsyncNode version of FilterNode that sleeps for 1s while processing each message
type AsyncFilterNode struct {
	fbcontext.ContextAware
}

// Setup is a no-op in asyncfilternode
func (a *AsyncFilterNode) Setup(config map[string]string) error {
	return nil
}

// ProcessAsync handles the event and returns an optional result, and an optional error
func (a *AsyncFilterNode) ProcessAsync(event *firebolt.AsyncEvent) {
	bytes, ok := event.Payload.([]byte)
	if !ok {
		event.ReturnError(errors.New("failed type assertion for conversion to string"))
		return
	}
	str := string(bytes)

	// simulate an async process that takes one second
	go func() {
		time.Sleep(1 * time.Second)

		if !strings.HasPrefix(str, "filter") {
			event.ReturnEvent(event.WithPayload(str))
			AsyncPassedEvents <- str
			return
		}
		AsyncFilteredEvents <- str
		event.ReturnFiltered()
	}()

	return
}

// Shutdown provides an opportunity for the Node to clean up resources on shutdown
func (a *AsyncFilterNode) Shutdown() error {
	return nil
}

// Receive handles a message from another node or an external source
func (a *AsyncFilterNode) Receive(msg fbcontext.Message) error {
	return errors.New("message not supported")
}

// IndexRequestBuilderNode is a Node that converts strings to elasticsearch IndexRequests
type IndexRequestBuilderNode struct {
	docNum int
	fbcontext.ContextAware
}

// Setup is a no-op
func (i *IndexRequestBuilderNode) Setup(config map[string]string) error {
	return nil
}

type inttestDoc struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

// Process handles the event and returns an optional result, and an optional error
func (i *IndexRequestBuilderNode) Process(event *firebolt.Event) (*firebolt.Event, error) {
	str, ok := event.Payload.(string)
	if !ok {
		return nil, errors.New("failed type assertion for conversion to string")
	}

	i.docNum++
	indexRequest := elasticsearch.IndexRequest{
		Index:       "inttest",
		MappingType: "",
		DocID:       strconv.Itoa(i.docNum) + ":" + str,
		Doc: inttestDoc{
			ID:    str,
			Value: str,
		},
	}

	return event.WithPayload(indexRequest), nil
}

// Shutdown provides an opportunity for the Node to clean up resources on shutdown
func (i *IndexRequestBuilderNode) Shutdown() error {
	return nil
}

// Receive handles a message from another node or an external source
func (i *IndexRequestBuilderNode) Receive(msg fbcontext.Message) error {
	return errors.New("message not supported")
}
