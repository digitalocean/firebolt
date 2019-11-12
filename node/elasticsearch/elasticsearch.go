package elasticsearch

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/fbcontext"
)

// IndexRequest is the event payload type to use when passing data to the elasticsearch node.
type IndexRequest struct {
	Index       string      `json:"index"`
	MappingType string      `json:"mapping_type"` // in ES 7.x+, leave MappingType unset and ES will use `_doc`
	DocID       string      `json:"doc_id"`
	Doc         interface{} `json:"doc"`
}

// Elasticsearch is a Node that uses elastic_index_client to index documents in the configured ElasticSearch cluster.
type Elasticsearch struct {
	fbcontext.ContextAware
	serviceFactory bulkServiceFactory
	indexClient    *ElasticIndexClient
	done           context.CancelFunc
}

// Setup is a no-op in index
func (i *Elasticsearch) Setup(cfgMap map[string]string) error {
	ctx, done := context.WithCancel(context.Background())
	i.done = done

	config := firebolt.Nodeconfig(cfgMap)
	esURL, err := config.StringConfigRequired("elastic-addr")
	if err != nil {
		return err
	}

	batchSize, err := config.IntConfig("batch-size", 100, 1, math.MaxInt32)
	if err != nil {
		return err
	}

	batchMaxWaitMs, err := config.IntConfig("batch-max-wait-ms", 1000, 1, math.MaxInt32)
	if err != nil {
		return err
	}

	bulkIndexTimeoutMs, err := config.IntConfig("bulk-index-timeout-ms", 5000, 1, math.MaxInt32)
	if err != nil {
		return err
	}

	reconnectBatchCount, err := config.IntConfig("reconnect-batch-count", 10000, 1, math.MaxInt32)
	if err != nil {
		return err
	}

	bulkIndexMaxRetries, err := config.IntConfig("bulk-index-max-retries", 3, 1, math.MaxInt32)
	if err != nil {
		return err
	}

	bulkIndexTimeoutSeconds, err := config.IntConfig("bulk-index-timeout-seconds", 20, 1, math.MaxInt32)
	if err != nil {
		return err
	}

	indexWorkers, err := config.IntConfig("index-workers", 1, 1, math.MaxInt32)
	if err != nil {
		return err
	}

	// initialize metrics
	metrics := &Metrics{}
	metrics.RegisterElasticIndexMetrics()

	// service factory; in tests it must be prepopulated with a mock
	if i.serviceFactory == nil {
		i.serviceFactory = newEsBulkServiceFactory(ctx, esURL, reconnectBatchCount, bulkIndexTimeoutMs, metrics)
	}

	i.indexClient = NewElasticIndexClient(
		i.serviceFactory,
		metrics,
		batchSize,
		bulkIndexMaxRetries,
		bulkIndexTimeoutSeconds,
		indexWorkers,
		time.Duration(batchMaxWaitMs)*time.Millisecond)

	go i.indexClient.Run(ctx)

	return nil
}

// ProcessAsync enqueues the document index request for bulk indexing
func (i *Elasticsearch) ProcessAsync(event *firebolt.AsyncEvent) {
	indexRequest, ok := event.Payload.(IndexRequest)
	if !ok {
		event.ReturnError(errors.New("failed type assertion for conversion to IndexRequest"))
		return
	}

	ir := &eventIndexRequest{
		Index:       indexRequest.Index,
		MappingType: indexRequest.MappingType,
		Doc:         indexRequest.Doc,
		DocID:       indexRequest.DocID,
		Event:       event,
	}

	i.indexClient.Send(ir)
}

// Receive handles a message from another node or an external source
func (i *Elasticsearch) Receive(msg fbcontext.Message) error {
	return errors.New("elasticsearch: message not supported")
}

// Shutdown provides an opportunity for the Node to clean up resources on shutdown
func (i *Elasticsearch) Shutdown() error {
	i.done()
	return nil
}
