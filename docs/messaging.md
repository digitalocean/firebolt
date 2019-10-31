## Messaging

In a firebolt application it is sometimes valuable to be able to pass `messages` to the nodes in your application, across
all running instances.   Messages are different from the `events` that flow down your node hierarchy; they can be used to:

* send a command to a node
* change a node's configuration at runtime
* share data across the instances of your application

A transport is needed to deliver messages to all running instances of your application; a `kafka` transport is provided and
other transports may be added in the future.

The source & nodes in your application can `Subscribe()` to message types they are interested in; matching messages will be
delivered to the nodes `Receive()` method.

### Sending Messages

Within the code for a `node`, ensure that you have access to messaging by embedding `fbcontext.ContextAware`, then you can
send messages as follows:

```go
    msg := &fbcontext.Message{
        MessageType: "MyMessageType",
        Key:         "0",
        Payload:     myPayloadBytes,
    }
    err := f.Ctx.SendMessage(msg)
```

You can also send messages from your application code 'outside' firebolt, using the executor:

```go
    msg := message.Message{
        MessageType: "MyMessageType",
        Key:         "0",
        Payload:     []byte(payload),
    }
    err := ex.SendMessage(msg)
```

Messages will be delivered to all nodes that have subcribed to the message type, across all running instances of your app.


### Receiving Messages

To receive any messages, your source/node must subscribe to one or more MessageTypes.   A MessageType is just a unique (within
your application) string identifying a category of messages.

Typically your subscriptions would not change during a node's lifetime, so it's appropriate to subscribe within your node's
`Setup()` method:

```go
    func (f *FilterNode) Setup(config map[string]string) error {
        f.Subscribe([]string{"SomeMessage", "SomeOtherMessage"})
        return nil
    }
```

Now you can receive messages by implementing `Receive()`:

```go
    func (f *FilterNode) Receive(msg fbcontext.Message) error {
        println("received message of type " + msg.MessageType)
        return nil
    }
```

When an instance of your firebolt application starts, nodes will receive **all outstanding messages** for their subscribed
messageTypes.   To acknowledge a message as complete so that it will not be redelivered, use 'Ctx.AckMessage()'.   Note that 
this is inherently racy and your message processing should be idempotent, see the next section:

```go
func (f *FilterNode) acknowledge(msg fbcontext.Message) error {
	return f.Ctx.AckMessage(msg)
}
```

### Idempotence

Currently the only provided message transport is Kafka.   Because we don't use any locking or transactions, message delivery
is inherently at-least-once.   Your message processing must be designed to be idempotent; repeated message delivery is
expected particularly around instance restarts.
