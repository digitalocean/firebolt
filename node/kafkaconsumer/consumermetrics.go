package kafkaconsumer

import (
	"encoding/json"
	"fmt"

	"errors"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/digitalocean/firebolt/metrics"
)

// Metrics encapsulates the prometheus collectors used to record metrics about the consumer
type Metrics struct {
	EventsConsumed     *prometheus.CounterVec
	StoredOffset       *prometheus.GaugeVec
	LowWatermark       *prometheus.GaugeVec
	HighWatermark      *prometheus.GaugeVec
	ConsumerLag        *prometheus.GaugeVec
	ConsumerLagStored  *prometheus.GaugeVec
	RecoveryEvents     *prometheus.CounterVec
	RecoveryRemaining  *prometheus.GaugeVec
	RecoveryPartitions prometheus.Gauge
}

// PartitionStats is a struct for holding the statistics emitted by the librdkafka consumer that underlies confluent-kafka-go
// after they are parsed from their original JSON
// https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md#partitions
type PartitionStats struct {
	id                float64
	storedOffset      float64
	lowWatermark      float64
	highWatermark     float64
	consumerLag       float64
	consumerLagStored float64
}

// RegisterConsumerMetrics initializes gauges for tracking consumer state and registers them with the prometheus client
func (m *Metrics) RegisterConsumerMetrics() {
	m.EventsConsumed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "events_consumed_total",
			Help:      "Total count of events consumed by the default consumer for this partition",
		},
		[]string{"partition_id"},
	)

	m.StoredOffset = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "consumer_stored_offset",
			Help:      "The most recently stored (already consumed) offset for this partition",
		},
		[]string{"partition_id"},
	)

	m.LowWatermark = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "consumer_low_watermark",
			Help:      "The lowest available offset for this partition",
		},
		[]string{"partition_id"},
	)

	m.HighWatermark = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "consumer_high_watermark",
			Help:      "The highest available offset for this partition",
		},
		[]string{"partition_id"},
	)

	m.ConsumerLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "consumer_offset_lag",
			Help:      "Approximate consume lag from the committed_offset (number of events behind the high watermark) for this partition",
		},
		[]string{"partition_id"},
	)

	m.ConsumerLagStored = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "consumer_offset_lag_stored",
			Help:      "Approximate consume lag from the stored_offset (offset to be committed) (number of events behind the high watermark) for this partition",
		},
		[]string{"partition_id"},
	)

	m.RecoveryEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "recovered_events_total",
			Help:      "Total count of events parallel recovery has processed for this partition",
		},
		[]string{"partition_id"},
	)

	m.RecoveryRemaining = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "recovery_events_remaining",
			Help:      "Approximate number of remaining events to recover for this partition",
		},
		[]string{"partition_id"},
	)

	m.RecoveryPartitions = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.Get().AppMetricsPrefix,
			Name:      "active_recovery_partitions",
			Help:      "Current number of partitions in active recovery",
		},
	)

	_ = prometheus.Register(m.EventsConsumed)
	_ = prometheus.Register(m.StoredOffset)
	_ = prometheus.Register(m.LowWatermark)
	_ = prometheus.Register(m.HighWatermark)
	_ = prometheus.Register(m.ConsumerLag)
	_ = prometheus.Register(m.ConsumerLagStored)
	_ = prometheus.Register(m.RecoveryEvents)
	_ = prometheus.Register(m.RecoveryRemaining)
	_ = prometheus.Register(m.RecoveryPartitions)
}

// UpdateConsumerMetrics takes the JSON stats reported by the librdkafka consumer and parses it then updates the prometheus
// gauges.
func (m *Metrics) UpdateConsumerMetrics(statsJSON string, topic string) {
	partitionsStats := m.extractPartitionStats(statsJSON, topic)

	for _, partitionStats := range partitionsStats {
		partitionStr := fmt.Sprintf("%.0f", partitionStats.id)
		m.StoredOffset.WithLabelValues(partitionStr).Set(partitionStats.storedOffset)
		m.LowWatermark.WithLabelValues(partitionStr).Set(partitionStats.lowWatermark)
		m.HighWatermark.WithLabelValues(partitionStr).Set(partitionStats.highWatermark)
		m.ConsumerLag.WithLabelValues(partitionStr).Set(partitionStats.consumerLag)
		m.ConsumerLagStored.WithLabelValues(partitionStr).Set(partitionStats.consumerLagStored)
	}
}

// ExtractPartitionStats parses the `librdkafka` consumer statistics and extracts only consumer offset & lag data per partition,
// excluding the '-1' composite partition stats so that only 'real' partitions are returned.
func (m *Metrics) extractPartitionStats(jsonStr string, topicName string) []PartitionStats {
	var consumerStats map[string]interface{}
	err := json.Unmarshal(([]byte)(jsonStr), &consumerStats)
	if err != nil {
		log.WithError(err).WithField("stats_json", jsonStr).Error("consumermetrics: failed to parse consumer stats")
		return nil
	}

	topics, ok := consumerStats["topics"].(map[string]interface{})
	if !ok {
		log.WithField("stats_json", jsonStr).WithField("extract_failure", "topics").Error("consumermetrics: failed to extract field from consumer stats")
		return nil
	}

	topic, ok := topics[topicName].(map[string]interface{})
	if !ok {
		log.WithField("stats_json", jsonStr).WithField("extract_failure", topicName).Error("consumermetrics: failed to extract field from consumer stats")
		return nil
	}

	partitions, ok := topic["partitions"].(map[string]interface{})
	if !ok {
		log.WithField("stats_json", jsonStr).WithField("extract_failure", "partitions").Error("consumermetrics: failed to extract field from consumer stats")
		return nil
	}

	// get stats from each partition
	partitionsStats := make([]PartitionStats, 0)
	for _, partition := range partitions {
		partitionsStats = m.addPartitionStats(partition, partitionsStats)
	}

	return partitionsStats
}

// addPartitionStats parses a single partition's statistics, appends those to the passed partitionStats array, and
// returns the updated array
func (m *Metrics) addPartitionStats(partition interface{}, partitionsStats []PartitionStats) []PartitionStats {
	partitionMap, ok := partition.(map[string]interface{})
	if !ok {
		log.WithField("extract_failure", "partition").Error("consumermetrics: failed to extract field from consumer stats")
	} else {
		// each instance will only be active for some subset of partitions; statistics like lag will be incorrect for the
		// partitions for which it is not active so let those instances report those metrics
		if partitionMap["fetch_state"] == "active" {
			partitionStats, err := buildPartitionStats(partitionMap)
			if err != nil {
				log.WithError(err).Error("consumermetrics: failed to extract partition stats")
			} else {
				if partitionStats.id != -1 {
					partitionsStats = append(partitionsStats, *partitionStats)
				}
			}
		} else {
			if partitionMap["partition"] != nil {
				partitionStats := m.buildUnassignedPartitionStats(partitionMap["partition"].(float64))
				if partitionStats.id != -1 {
					partitionsStats = append(partitionsStats, partitionStats)
				}
			}
		}
	}
	return partitionsStats
}

func (m *Metrics) buildUnassignedPartitionStats(partitionID float64) PartitionStats {
	// In OB-1238, we found that on consumer rebalance the instance that formerly was assigned the partition will
	// keep reporting its last metric values forever; we explicitly set negative values for non-assigned partitions
	// here to fix that.   Because librdkafka reports -1001 in some cases for unassigned values, we've had to use
	// a condition to filter out negative values anyway, so this won't harm any of our dashboards or alerts.
	return PartitionStats{
		id:                partitionID,
		storedOffset:      -1,
		lowWatermark:      -1,
		highWatermark:     -1,
		consumerLag:       -1,
		consumerLagStored: -1,
	}
}

func buildPartitionStats(partitionMap map[string]interface{}) (*PartitionStats, error) {
	var id, storedOffset, lowWatermark, highWatermark, consumerLag, consumerLagStored float64
	if partitionMap["partition"] != nil {
		id = partitionMap["partition"].(float64)
	} else {
		return nil, errors.New("failed to parse consumer stats json: missing 'partition' field")
	}
	if partitionMap["stored_offset"] != nil {
		storedOffset = partitionMap["stored_offset"].(float64)
	} else {
		return nil, errors.New("failed to parse consumer stats json: missing 'stored_offset' field")
	}
	if partitionMap["lo_offset"] != nil {
		lowWatermark = partitionMap["lo_offset"].(float64)
	} else {
		return nil, errors.New("failed to parse consumer stats json: missing 'lo_offset' field")
	}
	if partitionMap["hi_offset"] != nil {
		highWatermark = partitionMap["hi_offset"].(float64)
	} else {
		return nil, errors.New("failed to parse consumer stats json: missing 'hi_offset' field")
	}
	if partitionMap["consumer_lag"] != nil {
		consumerLag = partitionMap["consumer_lag"].(float64)
	} else {
		return nil, errors.New("failed to parse consumer stats json: missing 'consumer_lag' field")
	}
	if partitionMap["consumer_lag_stored"] != nil {
		consumerLagStored = partitionMap["consumer_lag_stored"].(float64)
	} else {
		return nil, errors.New("failed to parse consumer stats json: missing 'consumer_lag_stored' field")
	}

	partitionStats := &PartitionStats{
		id:                id,
		storedOffset:      storedOffset,
		lowWatermark:      lowWatermark,
		highWatermark:     highWatermark,
		consumerLag:       consumerLag,
		consumerLagStored: consumerLagStored,
	}

	return partitionStats, nil
}
