package firebolt

// ProduceRequest is a request to produce a single message to a topic in a messaging system (AMQP, Kafka, ZMQ, etc).
type ProduceRequest interface {
	Topic() string
	Message() []byte
}

// SimpleProduceRequest is a default implementation of ProduceRequest that can be used in simple cases to request that
// a message be produced.
type SimpleProduceRequest struct {
	TargetTopic  string
	MessageBytes []byte
}

// Topic returns the target topic in the destination messaging system to which this message should be sent.
func (s *SimpleProduceRequest) Topic() string {
	return s.TargetTopic
}

// Message returns the raw message bytes.
func (s *SimpleProduceRequest) Message() []byte {
	return s.MessageBytes
}
