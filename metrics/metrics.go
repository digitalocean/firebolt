package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds prometheus collectors for firebolt core, the configured source, and all configured nodes.   This is
// managed as a singleton, do not create instances in your code.   Use the Init() and Get() methods instead.
type Metrics struct {
	AppMetricsPrefix string
	sourceMetrics    SourceMetrics
	nodeMetrics      NodeMetrics
	messageMetrics   MessageMetrics
}

// SourceMetrics encapsulates the prometheus collectors used by firebolt to record metrics about the source
type SourceMetrics struct {
	EventsEmitted prometheus.Counter
}

// NodeMetrics encapsulates the prometheus collectors used by firebolt to record metrics about each node
type NodeMetrics struct {
	EventsReceived   *prometheus.CounterVec
	Successes        *prometheus.CounterVec
	Failures         *prometheus.CounterVec
	Filtered         *prometheus.CounterVec
	BufferedEvents   *prometheus.GaugeVec
	ProcessTime      *prometheus.HistogramVec
	DiscardedEvents  *prometheus.CounterVec
	BufferFullEvents *prometheus.CounterVec
}

// MessageMetrics encapsulates the prometheus collectos used to record metrics about messages sent and received
type MessageMetrics struct {
	MessagesSent          *prometheus.CounterVec
	MessagesReceived      *prometheus.CounterVec
	MessagesSentBytes     *prometheus.CounterVec
	MessagesReceivedBytes *prometheus.CounterVec
}

var singleton *Metrics
var once sync.Once

// Init instantiates the metrics singleton and registers core metrics
func Init(appMetricsPrefix string) {
	once.Do(func() {
		singleton = &Metrics{
			AppMetricsPrefix: appMetricsPrefix,
		}

		singleton.registerSourceMetrics()
		singleton.registerNodeMetrics()
		singleton.registerMessageMetrics()
	})
}

// Get returns the singleton instance of this metrics manager
func Get() *Metrics {
	if singleton == nil {
		panic("illegal attempt to access metrics before initialization, be sure to call Init()")
	}
	return singleton
}

// Source returns the SourceMetrics for the configured source
func Source() SourceMetrics {
	if singleton == nil {
		panic("illegal attempt to access metrics before initialization, be sure to call Init()")
	}
	return singleton.sourceMetrics
}

// Node returns the NodeMetrics for the node identified by 'nodeId'.   If no metrics have been registered for this
// node they are lazy-initialized and returned.
func Node() NodeMetrics {
	if singleton == nil {
		panic("illegal attempt to access metrics before initialization, be sure to call Init()")
	}
	return singleton.nodeMetrics
}

// Message returns the MessageMetrics for this firebolt application
func Message() MessageMetrics {
	if singleton == nil {
		panic("illegal attempt to access metrics before initialization, be sure to call Init()")
	}
	return singleton.messageMetrics
}

func (m *Metrics) registerSourceMetrics() {
	m.sourceMetrics = SourceMetrics{
		EventsEmitted: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: m.AppMetricsPrefix,
				Name:      "source_events_emitted_total",
				Help:      "The total number of events emitted by this application's source",
			},
		),
	}

	prometheus.MustRegister(m.sourceMetrics.EventsEmitted)
}

func (m *Metrics) registerNodeMetrics() {
	m.nodeMetrics = NodeMetrics{
		EventsReceived: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: m.AppMetricsPrefix,
				Name:      "node_received_events_total",
				Help:      "The total number of events received by this node",
			},
			[]string{"node_id"},
		),
		Successes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: m.AppMetricsPrefix,
				Name:      "node_processed_events_total",
				Help:      "The total number of events processed successfully by this node",
			},
			[]string{"node_id"},
		),
		Failures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: m.AppMetricsPrefix,
				Name:      "node_failed_events_total",
				Help:      "The total number of events processed with errors by this node",
			},
			[]string{"node_id"},
		),
		Filtered: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: m.AppMetricsPrefix,
				Name:      "node_filtered_events_total",
				Help:      "The total number of events filtered out by this node",
			},
			[]string{"node_id"},
		),
		BufferedEvents: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: m.AppMetricsPrefix,
				Name:      "node_buffered_events",
				Help:      "The current number of events waiting in the input buffer for this node",
			},
			[]string{"node_id"},
		),
		ProcessTime: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: m.AppMetricsPrefix,
				Name:      "node_processing_time_sec",
				Help:      "Processing time per event in seconds",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 4, 8),
			},
			[]string{"node_id"},
		),
		DiscardedEvents: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: m.AppMetricsPrefix,
				Name:      "discarded_events_total",
				Help:      "The total number of events discarded because the node's buffer was full",
			},
			[]string{"node_id"},
		),
		BufferFullEvents: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: m.AppMetricsPrefix,
				Name:      "buffer_full_events_total",
				Help:      "The total number of events that caused blocking because the node's buffer was full",
			},
			[]string{"node_id"},
		),
	}

	// i would really prefer "register or replace" semantics
	// nolint:errcheck
	prometheus.Register(m.nodeMetrics.EventsReceived)
	prometheus.Register(m.nodeMetrics.Successes)
	prometheus.Register(m.nodeMetrics.Failures)
	prometheus.Register(m.nodeMetrics.Filtered)
	prometheus.Register(m.nodeMetrics.BufferedEvents)
	prometheus.Register(m.nodeMetrics.ProcessTime)
	prometheus.Register(m.nodeMetrics.DiscardedEvents)
	prometheus.Register(m.nodeMetrics.BufferFullEvents)
}

func (m *Metrics) registerMessageMetrics() {
	m.messageMetrics = MessageMetrics{
		MessagesSent: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: m.AppMetricsPrefix,
				Name:      "messages_sent_total",
				Help:      "The total number of messages sent for each message type",
			},
			[]string{"transport", "message_type", "ack"},
		),
		MessagesReceived: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: m.AppMetricsPrefix,
				Name:      "messages_received_total",
				Help:      "The total number of messages received for each message type",
			},
			[]string{"transport", "message_type", "ack"},
		),
		MessagesSentBytes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: m.AppMetricsPrefix,
				Name:      "messages_sent_bytes_total",
				Help:      "The total bytes of messages sent for each message type",
			},
			[]string{"transport", "message_type", "ack"},
		),
		MessagesReceivedBytes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: m.AppMetricsPrefix,
				Name:      "messages_received_bytes_total",
				Help:      "The total bytes of messages received for each message type",
			},
			[]string{"transport", "message_type", "ack"},
		),
	}

	// nolint:errcheck
	prometheus.Register(m.messageMetrics.MessagesSent)
	prometheus.Register(m.messageMetrics.MessagesReceived)
	prometheus.Register(m.messageMetrics.MessagesSentBytes)
	prometheus.Register(m.messageMetrics.MessagesReceivedBytes)
}
