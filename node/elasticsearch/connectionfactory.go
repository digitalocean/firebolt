package elasticsearch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/olivere/elastic/v7"
	log "github.com/sirupsen/logrus"
)

// mocks are generated from this dir with 'mockery -name bulkService -inpkg .' in case that interface changes
type bulkService interface {
	Timeout(timeout string) *elastic.BulkService
	Add(requests ...elastic.BulkableRequest) *elastic.BulkService
	NumberOfActions() int
	Do(ctx context.Context) (*elastic.BulkResponse, error)
}

type bulkServiceFactory interface {
	BulkService() bulkService
}

type esBulkServiceFactory struct {
	ctx              context.Context
	connectLock      sync.RWMutex
	esURL            string
	esUsername       string
	esPassword       string
	esClient         *elastic.Client
	reconnectBatches int64
	timeoutMs        int
	batchCount       int64
	metrics          *Metrics
}

func newEsBulkServiceFactory(ctx context.Context, url string, esUsername string, esPassword string, reconnectBatches int, timeoutMs int, metrics *Metrics) *esBulkServiceFactory {
	factory := &esBulkServiceFactory{
		ctx:              ctx,
		connectLock:      sync.RWMutex{},
		esURL:            url,
		esUsername:       esUsername,
		esPassword:       esPassword,
		reconnectBatches: int64(reconnectBatches),
		timeoutMs:        timeoutMs,
		metrics:          metrics,
	}

	// initial connection
	factory.reconnect(ctx)
	return factory
}

func (e *esBulkServiceFactory) BulkService() bulkService {

	// periodically establish a new connection to help prevent hotspots in ES clusters with multiple client nodes
	atomic.AddInt64(&e.batchCount, 1)
	if e.batchCount == e.reconnectBatches {
		e.reconnect(e.ctx)
		e.batchCount = 0
	}

	// read lock before using the client conn allows many goroutines to run at once while blocking operations during a reconnect
	e.connectLock.RLock()
	defer e.connectLock.RUnlock()

	return e.esClient.Bulk().Timeout(fmt.Sprintf("%dms", e.timeoutMs))
}

func (e *esBulkServiceFactory) reconnect(ctx context.Context) {
	e.connectLock.Lock()
	defer e.connectLock.Unlock()

	for true {
		esClient, err := elastic.NewSimpleClient(elastic.SetURL(e.esURL), elastic.SetBasicAuth(e.esUsername, e.esPassword))
		if err == nil {
			e.esClient = esClient
			return
		}
		log.WithError(err).Error("elasticsearch: failed to connect to cluster")

		e.metrics.ElasticsearchConnectionFailures.Inc()

		select {
		case <-ctx.Done():
			log.Info("shutting down index client reconnect")
			return
		}
	}
}

var bulkServiceMock = &mockBulkService{}

// mockBulkServiceFactory returns a mock implementation of bulkService for use in tests
type mockBulkServiceFactory struct {
}

func (m mockBulkServiceFactory) BulkService() bulkService {
	return bulkServiceMock
}
