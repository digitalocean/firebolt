package kafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

// MessageConsumer is an interface for 'kafka.Consumer' to make it mockable.
// if that interface changes, the mock can be regenerated from this dir with:
//      mockery -name MessageConsumer -inpkg .
type MessageConsumer interface {
	Subscribe(string, kafka.RebalanceCb) error
	Events() chan kafka.Event
	Assign(partitions []kafka.TopicPartition) error
	Unassign() (err error)
	Committed(partitions []kafka.TopicPartition, timeoutMs int) (offsets []kafka.TopicPartition, err error)
	QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error)
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error)
	Close() (err error)
}
