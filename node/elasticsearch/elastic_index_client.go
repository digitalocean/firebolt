package elasticsearch

import (
	"context"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/digitalocean/firebolt"
	elastic "github.com/olivere/elastic/v7"

	log "github.com/sirupsen/logrus"
)

var (
	// ErrMaxRetries is an error used to alert the caller of doBulkIndex
	// that the maximum number of retries was exceeded and the batch should
	// not try to be indexed again. Messages will be dropped in this case.
	ErrMaxRetries = errors.New("max partial bulk retries reached")
)

type eventIndexRequest struct {
	Index       string
	MappingType string
	Doc         interface{}
	DocID       string
	Event       *firebolt.AsyncEvent
}

// ElasticIndexClient is an implementation of the IndexClient interface.
type ElasticIndexClient struct {
	batchSize      int
	batchMaxWait   time.Duration
	maxRetries     int
	timeoutSeconds int

	pool      chan int
	indexChan chan *eventIndexRequest

	connectionFactory bulkServiceFactory
	metrics           *Metrics
}

// NewElasticIndexClient returns an IndexClient with a downstream elasticsearch connection.
func NewElasticIndexClient(
	connectionFactory bulkServiceFactory,
	metrics *Metrics,
	batchSize,
	maxRetries,
	timeoutSeconds,
	workerPool int,
	batchMaxWait time.Duration) *ElasticIndexClient {

	c := &ElasticIndexClient{
		connectionFactory: connectionFactory,
		metrics:           metrics,
		batchSize:         batchSize,
		batchMaxWait:      batchMaxWait,
		maxRetries:        maxRetries,
		timeoutSeconds:    timeoutSeconds,
		pool:              make(chan int, workerPool),
		indexChan:         make(chan *eventIndexRequest),
	}

	for i := 0; i < workerPool; i++ {
		c.pool <- 1
	}
	return c
}

// Send prepares a LogMessage to be sent to elasticsearch
func (c *ElasticIndexClient) Send(request *eventIndexRequest) {
	c.indexChan <- request
}

// Run runs the elasticsearch indexing client
func (c *ElasticIndexClient) Run(ctx context.Context) {
	go c.batch(ctx)

	<-ctx.Done()
	c.Stop()
}

// Stop stops the elasticsearch indexing client
func (c *ElasticIndexClient) Stop() {
	// allow some time to finish indexing all currently queued events
	time.Sleep(3 * time.Second)
}

func (c *ElasticIndexClient) batch(ctx context.Context) {
	freshSlice := func() []*eventIndexRequest {
		return make([]*eventIndexRequest, 0)
	}

	messages := freshSlice()

	// Here we listen over three possible events:
	//   1. The normal operation is to just handle a stream of messages
	//      on the c.indexChan and as soon as the c.batchSize is reached,
	//      send the appropriate batch.
	//   2. In the event that within 250 milliseconds, the c.batchSize number of
	//      messages hasn't been read, we'll send the batch of messages,
	//      regardless of its size.
	//   3. The context has been canceled, return immediately.
	// In the first two cases, a call to c.sendBatch(...) will block
	// unless there are available goroutines to process messages to
	// elasticsearch.
	for {
		select {
		case msg := <-c.indexChan:
			messages = append(messages, msg)
			if len(messages) == c.batchSize {
				c.retryBulkIndex(messages, 0)
				messages = freshSlice()
			}
		case <-time.After(c.batchMaxWait):
			c.retryBulkIndex(messages, 0)
			messages = freshSlice()
		case <-ctx.Done():
			log.Info("shutting down index client batch goroutine")
			close(c.indexChan)
			return
		}
	}
}

func (c *ElasticIndexClient) retryBulkIndex(messages []*eventIndexRequest, retryCount int) {
	c.metrics.AvailableBatchRoutines.Set(float64(len(c.pool)))
	<-c.pool

	// make the bulk index calls to ES on a goroutine so that the caller can continue with the next batch
	go func() {
		defer func() { c.pool <- 1 }()

		// when the whole batch fails, it typically indicates that ES is unavailable, so we keep retrying forever
		// and once all the pool workers are in use backpressure will flow up to the nodes above this one
		for i := 0; ; i++ {
			err := c.doBulkIndex(messages, retryCount)
			if err == nil { // request-level success (still might be individual doc index errors)
				return
			}

			if err == ErrMaxRetries {
				return
			}

			// after an error exponential back off starting at 5s, then 10s, 30s, etc., capped at 3 minutes
			maxSleep := 3 * time.Minute
			calcSleep := time.Duration(math.Pow(2, float64(i))) * 5 * time.Second
			sleep := time.Duration(math.Min(float64(maxSleep), float64(calcSleep)))
			time.Sleep(sleep)
		}
	}()
}

func (c *ElasticIndexClient) doBulkIndex(requests []*eventIndexRequest, retryCount int) error {
	// nothing to do
	if len(requests) == 0 {
		return nil
	}

	// build the bulk request
	bulk := c.connectionFactory.BulkService()
	for _, req := range requests {
		request := elastic.NewBulkIndexRequest().Index(req.Index).Type(req.MappingType).Doc(req.Doc)
		if req.DocID != "" {
			request = request.Id(req.DocID)
		}
		bulk.Add(request)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.timeoutSeconds)*time.Second)
	defer cancel()
	startBulk := time.Now()
	defer func() { c.metrics.BulkProcessTime.Observe(time.Since(startBulk).Seconds()) }()

	// send the bulk index request to elasticsearch
	res, err := bulk.Do(ctx)

	// whole batch error
	if err != nil {
		c.metrics.BulkErrors.WithLabelValues(strconv.Itoa(retryCount)).Inc()
		log.Error(err)
		return err
	}

	// partial error, indexing failed for some documents
	if res.Errors {
		err := c.handleErrorResponses(retryCount, res, requests)
		if err != nil && err == ErrMaxRetries {
			return err
		}
	} else {
		// all docs were successfully indexed
		for _, req := range requests {
			req.Event.ReturnEvent(req.Event)
		}
	}

	select {
	default:
	case <-ctx.Done():
		c.metrics.BulkTimeouts.Inc()
		return ctx.Err()
	}

	return nil
}

func (c *ElasticIndexClient) handleErrorResponses(retryCount int, res *elastic.BulkResponse, requests []*eventIndexRequest) error {
	c.metrics.BulkIndividualErrors.WithLabelValues(strconv.Itoa(retryCount)).Add(float64(len(res.Failed())))
	retryRequests := make([]*eventIndexRequest, 0)

	// res.Items is a []map[string]*BulkResponseItem. The map keys are "index", "update", "delete", and "create".
	// we should only have "index" here, any other value is unexpected
	for bulkIndexPos, item := range res.Items {
		for action, i := range item {
			req := requests[bulkIndexPos]

			if action != "index" {
				// This should never happen based on our use of the es client
				log.WithField("action", action).Error("unexpected action returned from bulk index operation")
				continue
			}

			// Any 2xx response code should be considered a successful indexing operation
			if !(i.Status >= 200 && i.Status <= 299) {
				if i.Error == nil {
					log.WithField("status_code", i.Status).Warn("non 2xx response code the bulk indexing operation, but the returned error field is nil")
					continue
				} else {
					log.WithFields(log.Fields{
						"es_error_type":   i.Error.Type,
						"es_error_reason": i.Error.Reason,
					}).Debug("bulk indexing failure")
				}

				// don't retry mapping failures, zero chance of success on retry
				isTypeConflict := i.Error.Type == "mapper_parsing_exception"
				log.WithFields(log.Fields{
					"es_error_type":   i.Error.Type,
					"es_error_reason": i.Error.Reason,
					"type_conflict":   isTypeConflict,
				}).Debug("elasticsearch: failed to index, elasticsearch returned an error")

				if !isTypeConflict {
					retryRequests = append(retryRequests, req)
				}

				// return an error for operations that won't be retried
				if isTypeConflict || retryCount == c.maxRetries {
					e := firebolt.NewFBError("ES_INDEX_ERROR", "failed to index to elasticsearch", firebolt.WithInfo(i.Error))
					c.metrics.IndexErrors.WithLabelValues(i.Error.Type).Inc()
					req.Event.ReturnError(e)
				}

				// log errors at max retries
				if retryCount == c.maxRetries {
					log.WithFields(log.Fields{
						"es_error_type":   i.Error.Type,
						"es_error_reason": i.Error.Reason,
					}).Warn("elasticsearch: batch index operation failed after max retries")
				}
			} else {
				// success (this is the case where there were a mix of errors and successes in this bulk request)
				req.Event.ReturnEvent(req.Event)
			}
		}
	}

	// stop retrying at maxRetries
	if retryCount == c.maxRetries {
		c.metrics.BulkMaxRetriesReached.Add(float64(len(res.Failed())))
		return ErrMaxRetries
	}

	go c.retryBulkIndex(retryRequests, retryCount+1)
	return nil
}
