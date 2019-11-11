// +build integration

package elasticsearch

import (
	"strconv"
	"testing"
	"time"

	"github.com/olivere/elastic/v7"

	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/metrics"
	"github.com/digitalocean/firebolt/util"

	"github.com/stretchr/testify/assert"
)

// channels for threadsafe counting
var (
	indexName       = "es-11inttest" // to run this test repeatedly against a running ES instance, vary the index name
	inttestErrors   = make(chan error, 100000)
	inttestFiltered = make(chan interface{}, 100000)
	inttestEvents   = make(chan *firebolt.AsyncEvent, 100000)
)

func TestElasticsearchNode(t *testing.T) {
	metrics.Init("elasticsearch")

	// create an index
	util.WaitForPort(t, 9200)     // wait for ES to be up
	err := CreateIndex(indexName) // create the ES target index
	assert.NoError(t, err)

	// setup the elasticsearch node
	node := &Elasticsearch{}
	config := make(map[string]string)
	config["elastic-addr"] = "http://localhost:9200"
	config["batch-size"] = "10"
	err = node.Setup(config)
	assert.NoError(t, err)

	// first send some successful documents, these set will set the datatype of the 'user' field to 'numeric'
	for i := 0; i < 150; i++ {
		event := createIndexRequestEvent(&payloadUserNumeric{
			Msg:  "thingsandstuff",
			User: i,
		})
		node.ProcessAsync(event)
	}

	// pause so that the docs above get committed to the index and the mapping types are established
	time.Sleep(1 * time.Second)

	// then test some failures, these try 'user' field as 'text' and they should fail with mapping type exceptions
	for i := 0; i < 50; i++ {
		event := createIndexRequestEvent(&payloadUserObject{
			Msg: "errorsandstuff",
			User: user{
				ID:   i,
				Name: "user " + strconv.Itoa(i),
			},
		})
		node.ProcessAsync(event)
	}

	time.Sleep(1 * time.Second)

	// number of success, fail, filtered expected
	assert.Equal(t, 150, len(inttestEvents))
	assert.Equal(t, 50, len(inttestErrors))
	assert.Equal(t, 0, len(inttestFiltered))

	// validate that error details are returned
	e := <-inttestErrors
	ee := e.(firebolt.FBError)
	assert.Equal(t, "ES_INDEX_ERROR", ee.Code)
	assert.Equal(t, "failed to index to elasticsearch", ee.Msg)
	errorDetails := ee.ErrorInfo.(*elastic.ErrorDetails)
	assert.Equal(t, "mapper_parsing_exception", errorDetails.Type)

	// query to check results
	hits, err := SearchAllDocuments(indexName)
	assert.NoError(t, err)
	assert.Equal(t, int64(150), hits.TotalHits.Value)
	hit := hits.Hits[0]
	assert.Equal(t, indexName, hit.Index)
	assert.Equal(t, "_doc", hit.Type)
}

type payloadUserNumeric struct {
	Msg  string `json:"msg"`
	User int    `json:"user"`
}

type payloadUserObject struct {
	Msg  string `json:"msg"`
	User user   `json:"user"`
}

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func createIndexRequestEvent(doc interface{}) *firebolt.AsyncEvent {
	event := &firebolt.AsyncEvent{
		Event: &firebolt.Event{
			Payload: IndexRequest{
				Index: indexName,
				// ES7 used for testing, so leave out the type
				// MappingType: "events",
				Doc: doc,
			},
			Created: time.Now(),
		},
		ReturnError: func(err error) {
			inttestErrors <- err
		},
		ReturnFiltered: func() {
			inttestFiltered <- 1
		},
		ReturnEvent: func(asyncEvent *firebolt.AsyncEvent) {
			inttestEvents <- asyncEvent
		},
	}

	return event
}
