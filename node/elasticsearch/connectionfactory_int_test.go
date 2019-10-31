// +build integration

package elasticsearch

import (
	"context"
	"testing"

	"github.internal.digitalocean.com/observability/firebolt/metrics"

	"github.com/stretchr/testify/assert"
)

func TestReconnect(t *testing.T) {
	// initialize metrics
	metrics.Init("elasticsearch")

	metrics := &Metrics{}
	metrics.RegisterElasticIndexMetrics()

	cf := newEsBulkServiceFactory(context.TODO(), "http://localhost:9200", 3, 10000, metrics)

	// because reconnectBatches is 3, two reconnects will occur during this test
	for i := 0; i < 10; i++ {
		bulk := cf.BulkService()
		assert.NotNil(t, bulk)
	}
}
