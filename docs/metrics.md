## Metrics

Firebolt exposes prometheus-compatible metrics on an HTTP endpoint; to enable this service just set `metricsport` in your
config file and you will find the metrics at `/metrics`.   

All metrics will be prefixed (namespaced in prometheus terms) with the `metricsprefix` from your config.

#### Source Metrics
 * counter `source_events_emitted_total` The total number of events emitted by this application's source

#### KafkaConsumer Metrics
All `kafkaconsumer` metrics are vectors with a label `partition_id` identifying the kafka partition the metric applies to.
 * counter `events_consumed_total` Total count of events consumed by the default consumer for this partition
 * counter `consumer_stored_offset` The most recently stored (already consumed) offset for this partition
 * gauge `consumer_low_watermark` The lowest available offset for this partition
 * gauge `consumer_high_watermark` The highest available offset for this partition
 * gauge `consumer_offset_lag` Approximate consume lag (number of events behind the high watermark) for this partition
 * counter `recovered_events_total` Total count of events parallel recovery has processed for this partition
 * gauge `recovery_events_remaining` Approximate number of remaining events to recover for this partition

#### Node Metrics
All node metrics are vectors with a label `node_id` identifying the node that it measures.
 * counter `node_received_events_total` The total number of events received by this node
 * counter `node_processed_events_total` The total number of events processed successfully by this node
 * counter `node_failed_events_total` The total number of events processed with errors by this node
 * counter `node_filtered_events_total` The total number of events filtered out by this node
 * gauge `node_buffered_events` The current number of events waiting in the input buffer for this node
 * histogram `node_processing_time_sec` Processing time per event in seconds
 * counter `discarded_events_total` The total number of events discarded because the node's buffer was full
 * counter `buffer_full_events_total` The total number of events that caused blocking because the node's buffer was full
