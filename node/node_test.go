package node_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/digitalocean/firebolt/util"

	"github.com/stretchr/testify/assert"

	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/config"
	"github.com/digitalocean/firebolt/fbcontext"
	"github.com/digitalocean/firebolt/metrics"
	"github.com/digitalocean/firebolt/node"
	"github.com/digitalocean/firebolt/node/kafkaconsumer"
	"github.com/digitalocean/firebolt/node/kafkaproducer"
	"github.com/digitalocean/firebolt/node/syslogparser"
)

func registerTestNodeTypes() {
	// built-ins
	node.GetRegistry().RegisterSourceType("kafkaconsumer", func() node.Source {
		return &kafkaconsumer.KafkaConsumer{}
	}, reflect.TypeOf(([]byte)(nil)))

	node.GetRegistry().RegisterNodeType("syslogparser", func() node.Node {
		return &syslogparser.SyslogParser{}
	}, reflect.TypeOf(([]byte)(nil)), reflect.TypeOf(""))

	node.GetRegistry().RegisterNodeType("kafkaproducer", func() node.Node {
		return &kafkaproducer.KafkaProducer{}
	}, reflect.TypeOf((*firebolt.ProduceRequest)(nil)).Elem(), nil)

	node.GetRegistry().RegisterNodeType("errorkafkaproducer", func() node.Node {
		return &kafkaproducer.ErrorProducer{}
	}, reflect.TypeOf(&firebolt.EventError{}), nil)

	// test types
	node.GetRegistry().RegisterNodeType("filternode", func() node.Node {
		return &FilterNode{}
	}, reflect.TypeOf(""), reflect.TypeOf((*firebolt.ProduceRequest)(nil)).Elem())

	node.GetRegistry().RegisterNodeType("asyncfilternode", func() node.Node {
		return &AsyncFilterNode{}
	}, reflect.TypeOf(""), reflect.TypeOf((*firebolt.ProduceRequest)(nil)).Elem())
}

func TestInitContext(t *testing.T) {
	registerTestNodeTypes()

	cfg, err := config.Read("../testdata/testconfig_elauneind.yaml")
	assert.Nil(t, err)

	context := node.InitNodeContextHierarchy(cfg.Nodes[0])
	assert.Equal(t, "syslog1", context.Config.ID)
	assert.NotNil(t, context.ErrorHandler)
	assert.Equal(t, "errorkafkaproducer", context.ErrorHandler.Config.ID)
	assert.Equal(t, 2, len(context.Children))         // syslog1 has two children, filternode and asyncfilternode
	assert.Equal(t, false, context.Children[0].Async) // filternode is sync
	assert.Equal(t, true, context.Children[1].Async)  // asyncfilternode is async
	assert.Equal(t, "filternode", context.Children[0].Config.ID)
	assert.Nil(t, context.Children[0].ErrorHandler)
	assert.Equal(t, 2, len(context.Children[0].Children)) // only two children; the third child is marked 'disabled' in the config file
}

func TestProcess(t *testing.T) {
	metrics.Init("node_test")
	registerTestNodeTypes()

	cfg, err := config.Read("../testdata/testconfig_elauneind.yaml")
	assert.Nil(t, err)
	context := node.InitNodeContextHierarchy(cfg.Nodes[0]) // syslog1

	// recall that the node is a syslogparser
	// success
	context.ProcessEvent(&firebolt.Event{
		Payload: []byte("<191>2006-01-02T15:04:05.999999-07:00 host.example.org test: @cee:{\"a\":\"b\"}\n"),
		Created: time.Now(),
	})
	assert.Equal(t, 1, len(context.Children[0].Ch))
	assert.Equal(t, 0, len(context.ErrorHandler.Ch)) // no errors yet

	// same checks as abv, but validating against metrics
	be, err := util.GetGaugeVecValue(metrics.Node().BufferedEvents, "filternode")
	assert.NoError(t, err)
	assert.Equal(t, 1.0, be)
	ec, err := util.GetCounterVecValue(metrics.Node().Failures, "syslog1")
	assert.NoError(t, err)
	assert.Equal(t, 0.0, ec)

	// failure, should go to syslog1 and fail and go to errorkafkaproducer
	context.ProcessEvent(&firebolt.Event{
		Payload: []byte("not really a syslog msg"),
		Created: time.Now(),
	})
	assert.Equal(t, 1, len(context.Children[0].Ch)) // still 1 from before
	//TODO: it doesnt' use the channel currently; but it should
	// assert.Equal(t, 1, len(context.ErrorHandler.Ch)) // first error

	// validating against metrics
	be, err = util.GetGaugeVecValue(metrics.Node().BufferedEvents, "filternode")
	assert.NoError(t, err)
	assert.Equal(t, 1.0, be)
	ec, err = util.GetCounterVecValue(metrics.Node().Failures, "syslog1")
	assert.NoError(t, err)
	assert.Equal(t, 1.0, ec)
	ec, err = util.GetGaugeVecValue(metrics.Node().BufferedEvents, "errorkafkaproducer")
	assert.NoError(t, err)
	assert.Equal(t, 1.0, ec)

	// filternode should filter this one
	filterNode := context.Children[0]
	assert.Equal(t, "filternode", filterNode.Config.ID)
	filterNode.ProcessEvent(&firebolt.Event{
		Payload: "filter this message",
		Created: time.Now(),
	})
	assert.Equal(t, 0, len(filterNode.Children[0].Ch)) // was filtered; not delivered to children's channels

	// validating against metrics
	sc, err := util.GetCounterVecValue(metrics.Node().Successes, "filternode")
	assert.NoError(t, err)
	assert.Equal(t, 0.0, sc)
	fl, err := util.GetCounterVecValue(metrics.Node().Filtered, "filternode")
	assert.NoError(t, err)
	assert.Equal(t, 1.0, fl)
	fa, err := util.GetCounterVecValue(metrics.Node().Failures, "filternode")
	assert.NoError(t, err)
	assert.Equal(t, 0.0, fa)

	// filternode should pass this one
	filterNode.ProcessEvent(&firebolt.Event{
		Payload: "yeah thats right pass this one",
		Created: time.Now(),
	})
	assert.Equal(t, 1, len(context.Children[0].Children[0].Ch))

	// validating against metrics
	sc, err = util.GetCounterVecValue(metrics.Node().Successes, "filternode")
	assert.NoError(t, err)
	assert.Equal(t, 1.0, sc)
	ec, err = util.GetCounterVecValue(metrics.Node().Filtered, "filternode")
	assert.NoError(t, err)
	assert.Equal(t, 1.0, ec)
	ec, err = util.GetCounterVecValue(metrics.Node().Failures, "filternode")
	assert.NoError(t, err)
	assert.Equal(t, 0.0, ec)

	// asyncfilternode should filter this one
	asyncFilterNode := context.Children[1]
	assert.Equal(t, "asyncfilternode", asyncFilterNode.Config.ID)
	asyncFilterNode.ProcessEvent(&firebolt.Event{
		Payload: "asyncfilter this message",
		Created: time.Now(),
	})
	assert.Equal(t, 0, len(asyncFilterNode.Children[0].Ch)) // was filtered; not delivered to children's channels

	// validating against metrics
	sc, err = util.GetCounterVecValue(metrics.Node().Successes, "asyncfilternode")
	assert.NoError(t, err)
	assert.Equal(t, 0.0, sc)
	fl, err = util.GetCounterVecValue(metrics.Node().Filtered, "asyncfilternode")
	assert.NoError(t, err)
	assert.Equal(t, 1.0, fl)
	fa, err = util.GetCounterVecValue(metrics.Node().Failures, "asyncfilternode")
	assert.NoError(t, err)
	assert.Equal(t, 0.0, fa)

	// asyncfilternode should pass this one
	asyncFilterNode.ProcessEvent(&firebolt.Event{
		Payload: "not filtered at all",
		Created: time.Now(),
	})
	assert.Equal(t, 1, len(context.Children[0].Children[0].Ch))

	// validating against metrics
	sc, err = util.GetCounterVecValue(metrics.Node().Successes, "asyncfilternode")
	assert.NoError(t, err)
	assert.Equal(t, 1.0, sc)
	fl, err = util.GetCounterVecValue(metrics.Node().Filtered, "asyncfilternode")
	assert.NoError(t, err)
	assert.Equal(t, 1.0, fl)
	fa, err = util.GetCounterVecValue(metrics.Node().Failures, "asyncfilternode")
	assert.NoError(t, err)
	assert.Equal(t, 0.0, fa)

	// asyncfilternode will return an error because the payload isn't a string
	asyncFilterNode.ProcessEvent(&firebolt.Event{
		Payload: 1,
		Created: time.Now(),
	})
	assert.Equal(t, 1, len(context.Children[0].Children[0].Ch))

	// validating against metrics
	sc, err = util.GetCounterVecValue(metrics.Node().Successes, "asyncfilternode")
	assert.NoError(t, err)
	assert.Equal(t, 1.0, sc)
	fl, err = util.GetCounterVecValue(metrics.Node().Filtered, "asyncfilternode")
	assert.NoError(t, err)
	assert.Equal(t, 1.0, fl)
	fa, err = util.GetCounterVecValue(metrics.Node().Failures, "asyncfilternode")
	assert.NoError(t, err)
	assert.Equal(t, 1.0, fa)
}

// test 'discard_on_full_buffer' by passing a message to 'filternode' after filling it's first child's buffer
func TestDeliverToChild(t *testing.T) {
	metrics.Init("node_test")
	registerTestNodeTypes()

	cfg, err := config.Read("../testdata/testconfig_elauneind.yaml")

	assert.Nil(t, err)
	context := node.InitNodeContextHierarchy(cfg.Nodes[0])

	// the first child [0] is used to test discard, the second child [1] is used to test blocking/backpressure
	// first msg fills up the first child's buffer
	context.Children[0].ProcessEvent(&firebolt.Event{
		Payload: "this message passes the filter",
		Created: time.Now(),
	})
	assert.Equal(t, 1, len(context.Children[0].Children[0].Ch))
	assert.Equal(t, 1, len(context.Children[0].Children[1].Ch))

	// second is discarded by the first child which has buffersize 1 and discard_on_full_buffer=true
	context.Children[0].ProcessEvent(&firebolt.Event{
		Payload: "another message passes the filter",
		Created: time.Now(),
	})
	assert.Equal(t, 1, len(context.Children[0].Children[0].Ch))
	assert.Equal(t, 2, len(context.Children[0].Children[1].Ch))

	// third fills the buffer on the second child
	context.Children[0].ProcessEvent(&firebolt.Event{
		Payload: "another message passes the filter",
		Created: time.Now(),
	})
	assert.Equal(t, 1, len(context.Children[0].Children[0].Ch))
	assert.Equal(t, 3, len(context.Children[0].Children[1].Ch))

	// fourth should block due to full buffer on the second child
	go context.Children[0].ProcessEvent(&firebolt.Event{
		Payload: "another message passes the filter",
		Created: time.Now(),
	})
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, len(context.Children[0].Children[0].Ch))
	assert.Equal(t, 3, len(context.Children[0].Children[1].Ch))
	time.Sleep(100 * time.Millisecond)
	_ = <-context.Children[0].Children[1].Ch // read one from the full channel to unblock
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, len(context.Children[0].Children[0].Ch))
	assert.Equal(t, 3, len(context.Children[0].Children[1].Ch)) // full again
}

// FilterNode is a Node that filters out any events starting with 'filter'
type FilterNode struct {
	fbcontext.ContextAware
}

// Setup is a no-op in filternode
func (f *FilterNode) Setup(config map[string]string) error {
	return nil
}

func (f *FilterNode) Process(event *firebolt.Event) (*firebolt.Event, error) {
	str, ok := event.Payload.(string)
	if !ok {
		return nil, errors.New("failed type assertion for conversion to string")
	}
	if !strings.HasPrefix(str, "filter") {
		return &firebolt.Event{
			Payload: &firebolt.SimpleProduceRequest{MessageBytes: []byte(str)},
			Created: event.Created,
		}, nil
	}
	return nil, nil
}

// Shutdown provides an opportunity for the Node to clean up resources on shutdown
func (f *FilterNode) Shutdown() error {
	return nil
}

func (f *FilterNode) Receive(msg fbcontext.Message) error {
	return errors.New("filternode: message not supported")
}

//TODO: duplicate ID detection is not checking the errorhandlers I think?  add a failing test to prove it

// AsyncFilterNode is a Node that filters out any events starting with 'asyncfilter'
type AsyncFilterNode struct {
	fbcontext.ContextAware
}

// Setup is a no-op in asyncfilternode
func (a *AsyncFilterNode) Setup(config map[string]string) error {
	return nil
}

func (a *AsyncFilterNode) ProcessAsync(event *firebolt.AsyncEvent) {
	str, ok := event.Payload.(string)
	if !ok {
		event.ReturnError(errors.New("failed type assertion for conversion to string"))
		return
	}

	if !strings.HasPrefix(str, "asyncfilter") {
		event.ReturnEvent(&firebolt.AsyncEvent{
			Event: &firebolt.Event{
				Payload: &firebolt.SimpleProduceRequest{MessageBytes: []byte(str)},
				Created: event.Created,
			},
		})
		return
	}
	event.ReturnFiltered()
}

// Shutdown provides an opportunity for the Node to clean up resources on shutdown
func (a *AsyncFilterNode) Shutdown() error {
	return nil
}

func (a *AsyncFilterNode) Receive(msg fbcontext.Message) error {
	return errors.New("asyncfilternode: message not supported")
}
