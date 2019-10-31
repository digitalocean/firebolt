package elasticsearch

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.internal.digitalocean.com/observability/firebolt/metrics"
)

// Metrics encapsulates the prometheus metrics produced by the elasticsearch indexer.
type Metrics struct {
	BulkErrors                      prometheus.CounterVec
	BulkIndividualErrors            prometheus.CounterVec
	BulkProcessTime                 prometheus.Histogram
	BulkTimeouts                    prometheus.Counter
	BulkMaxRetriesReached           prometheus.Counter
	IndexErrors                     prometheus.CounterVec
	ElasticsearchConnectionFailures prometheus.Counter
	AvailableBatchRoutines          prometheus.Gauge
}

// RegisterElasticIndexMetrics initializes metrics and registers them with the prometheus client.
func (m *Metrics) RegisterElasticIndexMetrics() {
	m.BulkErrors = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "bulk_errors_total",
			Help:      "Count of bulk indexing failures to elasticsearch.",
		},
		[]string{"retry_count"},
	)

	m.BulkIndividualErrors = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "bulk_individual_errors_total",
			Help:      "Count of individual logs that failed to bulk index to elasticsearch.",
		},
		[]string{"retry_count"},
	)

	m.BulkProcessTime = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "bulk_process_time",
			Help:      "Time to write bulk logs to elasticsearch",
			Buckets:   prometheus.ExponentialBuckets(0.01, 3, 8),
		},
	)

	m.BulkTimeouts = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "bulk_timeouts_total",
			Help:      "Number of times bulk indexing to elasticsearch timed out.",
		},
	)

	m.IndexErrors = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "index_errors",
			Help:      "Count of individual logs that failed to index to elasticsearch.",
		},
		[]string{"error_type"},
	)

	m.BulkMaxRetriesReached = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "bulk_max_retries_reached_total",
			Help:      "Number of times bulk indexing to elasticsearch reached its maximum number of retries.",
		},
	)

	m.ElasticsearchConnectionFailures = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "elasticsearch_connection_failures_total",
			Help:      "Number of times connecting to elasticsearch failed.",
		},
	)

	m.AvailableBatchRoutines = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "available_batch_routines",
			Help:      "The current number of available batch goroutines to send to elasticsearch",
		},
	)

	prometheus.Unregister(m.BulkErrors)
	prometheus.MustRegister(m.BulkErrors)

	prometheus.Unregister(m.BulkIndividualErrors)
	prometheus.MustRegister(m.BulkIndividualErrors)

	prometheus.Unregister(m.BulkProcessTime)
	prometheus.MustRegister(m.BulkProcessTime)

	prometheus.Unregister(m.BulkTimeouts)
	prometheus.MustRegister(m.BulkTimeouts)

	prometheus.Unregister(m.BulkMaxRetriesReached)
	prometheus.MustRegister(m.BulkMaxRetriesReached)

	prometheus.Unregister(m.IndexErrors)
	prometheus.MustRegister(m.IndexErrors)

	prometheus.Unregister(m.ElasticsearchConnectionFailures)
	prometheus.MustRegister(m.ElasticsearchConnectionFailures)

	prometheus.Unregister(m.AvailableBatchRoutines)
	prometheus.MustRegister(m.AvailableBatchRoutines)
}
