package node

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/fbcontext"
	"github.com/digitalocean/firebolt/metrics"
)

// Node is a single unit of processing; a tree of Nodes comprise all event processing in a user application.
type Node interface {
	Init(id string, ctx fbcontext.FBContext)
	AcceptsMessage(messageType string) bool
	Setup(config map[string]string) error
	Shutdown() error
	Receive(msg fbcontext.Message) error
}

// SyncNode is a Node which responds synchronously.
type SyncNode interface {
	Node
	Process(event *firebolt.Event) (*firebolt.Event, error)
}

// AsyncNode is a Node which responds asynchronously.
type AsyncNode interface {
	Node
	ProcessAsync(event *firebolt.AsyncEvent)
}

//TODO: would it be helpful to measure the number of outstanding requests per node in case somehow a panic is killing
// the goroutine and those events are failing but not counted as failing?
// add a panic recovery that marks failed and logs?
// yes this ^^ plus (delivered - succeeded - failed) as a metric for 'inflight' to be sure nothing falls through the cracks
// TODO: add a future work ticket to have a timeout for async nodes, enforced by firebolt, configured in firebolt.yaml,
//  that kills the goroutine and replaces it with a new worker in the pool, with metrics

// Config is the set of configuration values used to initialize the Context for a Node
type Config struct {
	ID                  string            `yaml:"id"`
	Name                string            `yaml:"name"`
	Workers             int               `yaml:"workers"`
	BufferSize          int               `yaml:"buffersize"`
	Params              map[string]string `yaml:"params"`
	Children            []*Config         `yaml:"children"`
	ErrorHandler        *Config           `yaml:"error_handler"`
	Disabled            bool              `yaml:"disabled"`
	DiscardOnFullBuffer bool              `yaml:"discard_on_full_buffer"`
}

// Context is the execution context for a given node
type Context struct {
	Config        *Config
	Async         bool
	Ch            chan firebolt.Event // ch is the input buffer for this node
	StopCh        chan bool
	NodeProcessor Node
	Children      []*Context
	ErrorHandler  *Context
	MessageTypes  []string
	WaitGroup     *sync.WaitGroup
	ShutdownOnce  *sync.Once
}

// InitNodeContextHierarchy recursively builds the hierarchy of node execution Contexts starting at the passed node.
func InitNodeContextHierarchy(nodeConfig *Config) *Context {
	if nodeConfig.Disabled {
		log.WithField("nodeId", nodeConfig.ID).Info("node is marked disabled, skipping this node and all children")
		return nil
	}

	// children first
	var childContexts []*Context
	for _, childConfig := range nodeConfig.Children {
		ctx := InitNodeContextHierarchy(childConfig)
		if ctx != nil {
			childContexts = append(childContexts, ctx)
		}
	}

	// error handler, if one is configured
	var errorHandler *Context
	if nodeConfig.ErrorHandler != nil {
		errorHandler = &Context{
			Config:        nodeConfig.ErrorHandler,
			Ch:            make(chan firebolt.Event, nodeConfig.ErrorHandler.BufferSize),
			StopCh:        make(chan bool, nodeConfig.Workers), // unclean shutdown will use one message per worker on StopCh
			NodeProcessor: GetRegistry().InstantiateNode(nodeConfig.ErrorHandler.Name),
			WaitGroup:     &sync.WaitGroup{},
			ShutdownOnce:  &sync.Once{},
		}
	}

	// create the node processor itself
	nodeProcessor := GetRegistry().InstantiateNode(nodeConfig.Name)
	_, isSync := nodeProcessor.(SyncNode)
	_, isAsync := nodeProcessor.(AsyncNode)
	if !isSync && !isAsync {
		log.WithField("node_id", nodeConfig.ID).WithField("node_name", nodeConfig.Name).Error("node: configured node must implement either SyncNode or AsyncNode")
		panic("node: configured node " + nodeConfig.ID + " must implement either SyncNode or AsyncNode")
	}

	log.WithField("node_id", nodeConfig.ID).WithField("node_name", nodeConfig.Name).Info("building node context")
	ctx := &Context{
		Config:        nodeConfig,
		Ch:            make(chan firebolt.Event, nodeConfig.BufferSize),
		StopCh:        make(chan bool),
		NodeProcessor: nodeProcessor,
		Async:         isAsync,
		Children:      childContexts,
		ErrorHandler:  errorHandler,
		WaitGroup:     &sync.WaitGroup{},
		ShutdownOnce:  &sync.Once{},
	}

	return ctx
}

// ProcessEvent calls the node's 'process' method for the passed event and handles success / filter / failure cases.
func (nc *Context) ProcessEvent(event *firebolt.Event) {
	metrics.Node().EventsReceived.WithLabelValues(nc.Config.ID).Inc()

	// process
	if !nc.Async {
		result, err := nc.invokeProcessorSync(event)
		nc.handleResult(err, event, result)
	} else {
		nc.invokeProcessorAsync(event)
	}
}

func (nc *Context) handleResult(err error, event *firebolt.Event, result *firebolt.Event) {
	// success, filter, failure cases
	if err != nil {
		// failure
		nc.handleFailure(event, err)
	} else {
		if result == nil {
			// filtered
			log.WithField("node_id", nc.Config.ID).Debug("nil returned, filtering event")
			metrics.Node().Filtered.WithLabelValues(nc.Config.ID).Inc()
		} else {
			// successful
			metrics.Node().Successes.WithLabelValues(nc.Config.ID).Inc()
			for _, childNode := range nc.Children {
				nc.deliverToChild(childNode, result)
			}
		}
	}
}

// deliverToChild delivers a successful result to a single child node, discarding or blocking (backpressure) if the
// child's channel is full based on the node's configuration
func (nc *Context) deliverToChild(childNode *Context, event *firebolt.Event) {
	select {
	case childNode.Ch <- *event:
		// message was put on channel successfully
	default:
		// target channel was full
		if childNode.Config.DiscardOnFullBuffer {
			metrics.Node().DiscardedEvents.WithLabelValues(childNode.Config.ID).Inc()
		} else {
			// may block if ch still full, creating backpressure
			metrics.Node().BufferFullEvents.WithLabelValues(childNode.Config.ID).Inc()
			childNode.Ch <- *event
		}
	}
	metrics.Node().BufferedEvents.WithLabelValues(childNode.Config.ID).Set(float64(len(childNode.Ch)))
}

// invokeProcessorSync calls the Process() method on this node type, timing its execution
func (nc *Context) invokeProcessorSync(event *firebolt.Event) (*firebolt.Event, error) {
	start := time.Now()

	syncNode, ok := nc.NodeProcessor.(SyncNode)
	if !ok {
		panic("node: sync invocation failed to convert target to SyncNode")
	}

	result, err := syncNode.Process(event)
	metrics.Node().ProcessTime.WithLabelValues(nc.Config.ID).Observe(time.Since(start).Seconds())
	return result, err
}

// invokeProcessorAsync calls the ProcessAsync() method on this node type
func (nc *Context) invokeProcessorAsync(event *firebolt.Event) {
	start := time.Now()

	errFunc := func(err error) {
		metrics.Node().ProcessTime.WithLabelValues(nc.Config.ID).Observe(time.Since(start).Seconds())
		nc.handleResult(err, event, nil)
	}
	eventFunc := func(result *firebolt.AsyncEvent) {
		metrics.Node().ProcessTime.WithLabelValues(nc.Config.ID).Observe(time.Since(start).Seconds())
		if result != nil {
			nc.handleResult(nil, event, result.Event)
		} else {
			nc.handleResult(nil, event, nil)
		}
	}
	filterFunc := func() {
		metrics.Node().ProcessTime.WithLabelValues(nc.Config.ID).Observe(time.Since(start).Seconds())
		nc.handleResult(nil, event, nil)
	}

	asyncEvent := firebolt.NewAsyncEvent(event, errFunc, eventFunc, filterFunc)

	asyncNode, ok := nc.NodeProcessor.(AsyncNode)
	if !ok {
		panic("node: async invocation failed to convert target to AsyncNode")
	}
	asyncNode.ProcessAsync(asyncEvent)
}

// handleFailure passes the event and error data to an ErrorHandler, if one is configured for this node.
func (nc *Context) handleFailure(event *firebolt.Event, err error) {
	log.WithField("node_id", nc.Config.ID).WithField("event_data", event).WithError(err).Debug("failed processing for event")
	metrics.Node().Failures.WithLabelValues(nc.Config.ID).Inc()
	if nc.ErrorHandler != nil {
		// wrap up the event and the resulting error and hand 'em off to the ErrorHandler
		eventError := firebolt.EventError{
			Event: event,
			Err:   err,
		}

		// process just like a 'normal' node (an errorhandler node is validated not to have any children nor an errorhandler of its own)
		nc.ErrorHandler.Ch <- firebolt.Event{
			Payload: eventError,
			Created: time.Now(),
		}
		metrics.Node().BufferedEvents.WithLabelValues(nc.ErrorHandler.Config.ID).Set(float64(len(nc.ErrorHandler.Ch)))
	}
}
