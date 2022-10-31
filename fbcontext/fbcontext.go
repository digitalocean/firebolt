package fbcontext

import "errors"

// FBContext is a firebolt context.  It provides a source/node with access to inter-node or global operations.  Currently
// it supports sending messages to other source/nodes (across the cluster) and acknowledging receipt of those messages.
// After any changes, regenerate the mock with:
//
//	mockery -name FBContext -inpkg .
type FBContext interface {
	ConfigureMessaging(send MessageFunc, ack MessageFunc)
	ConfigureLeader(leader func() bool)
	SendMessage(msg Message) error
	AckMessage(msg Message) error
	IsLeader() bool
	InstanceID() string
}

// Context is the default implementation of a firebolt context.
type Context struct {
	sendFunc       MessageFunc
	ackFunc        MessageFunc
	leaderFunc     func() bool
	instanceIDFunc func() string
}

// NewFBContext creates a firebolt context with default/dummy implementations
func NewFBContext(instanceID func() string) FBContext {
	return &Context{
		sendFunc:       notImplementedMessageFunc,
		ackFunc:        notImplementedMessageFunc,
		leaderFunc:     func() bool { return false },
		instanceIDFunc: instanceID,
	}
}

// MessageFunc is a function supplied by Executor, which uses them to relay messages to the target source/node
type MessageFunc func(msg Message) error

// ConfigureMessaging adds send and ack handlers for delivering and acknowledging messages.
func (c *Context) ConfigureMessaging(send MessageFunc, ack MessageFunc) {
	c.sendFunc = send
	c.ackFunc = ack
}

// ConfigureLeader adds handler for returning leader election results.
func (c *Context) ConfigureLeader(leader func() bool) {
	c.leaderFunc = leader
}

// SendMessage sends a Message using the configured internaldata transport.
func (c *Context) SendMessage(msg Message) error {
	return c.sendFunc(msg)
}

// AckMessage acknowledges receipt of a Message; it will not be redelivered.
func (c *Context) AckMessage(msg Message) error {
	return c.ackFunc(msg)
}

// IsLeader indicates whether this instance is currently the cluster leader as determined by a leader election.   If
// zookeeper was not configured, this will always return false.
func (c *Context) IsLeader() bool {
	return c.leaderFunc()
}

// InstanceID returns the unique ID of this application instance.
func (c *Context) InstanceID() string {
	return c.instanceIDFunc()
}

// ContextAware is intended to be embedded in Node types to provide the node's application code with access to execution context.
type ContextAware struct {
	ID           string
	messageTypes []string
	Ctx          FBContext
}

// Init provides a new Source/Node with its execution context.
func (c *ContextAware) Init(id string, ctx FBContext) {
	c.ID = id
	c.Ctx = ctx
}

// Subscribe sets the message types that this node will accept.
func (c *ContextAware) Subscribe(messageTypes []string) {
	c.messageTypes = messageTypes
}

// AcceptsMessage is used when delivering messages to determine if a Source/Node requests delivery of messages with a given
// messageType.
func (c *ContextAware) AcceptsMessage(messageType string) bool {
	for _, t := range c.messageTypes {
		if messageType == t {
			return true
		}
	}
	return false
}

func notImplementedMessageFunc(_ Message) error {
	return errors.New("executor: internaldata not configured, messaging not available")
}
