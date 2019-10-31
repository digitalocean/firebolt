package elasticsearch

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.internal.digitalocean.com/observability/firebolt"
	"github.internal.digitalocean.com/observability/firebolt/fbcontext"
	"github.internal.digitalocean.com/observability/firebolt/metrics"

	elastic "github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
)

func TestSetup(t *testing.T) {
	metrics.Init("elasticsearch_test")
	e := &Elasticsearch{}
	e.serviceFactory = mockBulkServiceFactory{}

	// invalid config values
	config := make(map[string]string)
	err := e.Setup(config)
	assert.Error(t, err)
	assert.Equal(t, "missing config value [elastic-addr]", err.Error())

	config["elastic-addr"] = "localhost" // clean up the prev err
	config["batch-size"] = "abc"         // not an int
	err = e.Setup(config)
	assert.Error(t, err)
	assert.Equal(t, "expected integer value for config [batch-size]", err.Error())

	config["batch-size"] = "10"               // clean up the prev err
	config["batch-max-wait-ms"] = "-99999999" // less than minvalue
	err = e.Setup(config)
	assert.Error(t, err)
	assert.Equal(t, "config value [batch-max-wait-ms] requires value between [1] and [2147483647]", err.Error())

	config["batch-max-wait-ms"] = "10"     // clean up the prev err
	config["reconnect-batch-count"] = "-1" // less than minvalue
	err = e.Setup(config)
	assert.Error(t, err)
	assert.Equal(t, "config value [reconnect-batch-count] requires value between [1] and [2147483647]", err.Error())

	config["reconnect-batch-count"] = "1"  // clean up the prev err
	config["bulk-index-max-retries"] = "0" // less than minvalue
	err = e.Setup(config)
	assert.Error(t, err)
	assert.Equal(t, "config value [bulk-index-max-retries] requires value between [1] and [2147483647]", err.Error())

	config["bulk-index-max-retries"] = strconv.Itoa(math.MaxInt32) // clean up the prev err
	config["index-workers"] = ""                                   // not an int
	err = e.Setup(config)
	assert.Error(t, err)
	assert.Equal(t, "expected integer value for config [index-workers]", err.Error())

	config["index-workers"] = "5"              // clean up the prev err
	config["bulk-index-timeout-seconds"] = "0" // less than minvalue
	err = e.Setup(config)
	assert.Error(t, err)
	assert.Equal(t, "config value [bulk-index-timeout-seconds] requires value between [1] and [2147483647]", err.Error())

	// valid config
	config = make(map[string]string)
	config["elastic-addr"] = "localhost"
	config["batch-size"] = "10"
	config["rebind-message-count"] = "5" // others will be set from defaults
	err = e.Setup(config)
	assert.NoError(t, err)
}

func TestProcessAsync(t *testing.T) {
	metrics.Init("elasticsearch_test")
	e := &Elasticsearch{}
	e.serviceFactory = mockBulkServiceFactory{}
	config := make(map[string]string)
	config["elastic-addr"] = "localhost"
	config["batch-size"] = "1" // batch size 1 means the batch will execute immediately, so we can make simple assertions expecting that the work happens immediately
	err := e.Setup(config)
	assert.NoError(t, err)

	// channels for threadsafe counting
	errCounter := make(chan interface{}, 100000)
	filteredCounter := make(chan interface{}, 100000)
	eventCounter := make(chan interface{}, 100000)

	// error: event is wrong type
	event := &firebolt.AsyncEvent{
		Event: &firebolt.Event{
			Payload: "i am the wrong type, this node requires IndexRequest",
			Created: time.Now(),
		},
		ReturnError: func(err error) {
			errCounter <- 1
		},
	}
	e.ProcessAsync(event)
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, 1, len(errCounter))

	// valid IndexRequest
	event = &firebolt.AsyncEvent{
		Event: &firebolt.Event{
			Payload: IndexRequest{
				Index:       "default",
				MappingType: "events",
				Doc:         "i am a log line",
			},
			Created: time.Now(),
		},
		ReturnError: func(err error) {
			errCounter <- 1
		},
		ReturnFiltered: func() {
			filteredCounter <- 1
		},
		ReturnEvent: func(asyncEvent *firebolt.AsyncEvent) {
			eventCounter <- 1
		},
	}
	request := elastic.NewBulkIndexRequest().Index("default").Type("events").Doc("i am a log line")
	bulkServiceMock.On("Add", request).Return(nil)
	bulkServiceMock.On("NumberOfActions").Return(1)
	bulkServiceMock.On("Do", mock.AnythingOfType("*context.timerCtx")).Return(&elastic.BulkResponse{}, nil) // complete success
	e.ProcessAsync(event)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 1, len(errCounter)) // no more errors were recorded
	assert.Equal(t, 0, len(filteredCounter))
	assert.Equal(t, 1, len(eventCounter))
}

func TestReceive(t *testing.T) {
	e := &Elasticsearch{}
	err := e.Receive(fbcontext.Message{})
	assert.Error(t, err) // this node doesn't support messaging
}

func TestShutdown(t *testing.T) {
	metrics.Init("elasticsearch_test")
	e := &Elasticsearch{}
	e.serviceFactory = mockBulkServiceFactory{}
	config := make(map[string]string)
	config["elastic-addr"] = "localhost"
	err := e.Setup(config)
	assert.NoError(t, err)

	err = e.Shutdown()
	assert.NoError(t, err)
}
