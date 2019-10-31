package fbcontext

// Message is the context representation of a 'message.Message'.   Message depends on node.kafkaconsumer, so node must not depend on message.
type Message struct {
	MessageType string
	Key         string `json:"key"`
	Payload     []byte `json:"payload"`
}
