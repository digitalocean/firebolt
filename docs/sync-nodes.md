## Synchronous Nodes

Most nodes are synchronous; they receive an event and are able to process it and return a result immediately.  
Synchronous `nodes` must implement the interface `node.SyncNode`:

```go
type SyncNode interface {
	Setup(config map[string]string) error
	Process(event *firebolt.Event) (*firebolt.Event, error)
	Shutdown() error
	Receive(msg fbcontext.Message) error
}
```

There are additional methods (Init, AcceptsMessage) in the `Node` interface that are omitted from this doc; custom nodes 
should embed the type `fbcontext.ContextAware` to get a default implementation of these methods which are used to 
provide messaging support and access to firebolt methods.

The `Setup()` method is called once at application startup, and passed a map of all `params` that are defined for this 
node in your config file.  

`Process()` is called with each individual event.   It should return `result, nil` to transform or pass through the event, 
`nil, nil` to filter out this event (child nodes will not be invoked), or `nil, error` to indicate an error condition.

`Receive(msg)` is called when a message (see: [messaging](messaging.md)) is received of a messageType that your node 
subscribes to.   To subscribe to a messagetype, in your `Setup()` method you may call the `Subscribe()` method on your 
node, which was provided by embedding `fbcontext.ContextAware`.

The `Shutdown()` method gives your node an opportunity to clean up any resources during system shutdown.

### Error Handling
When an error occurs during `Process()`, return `nil, error`.  If additional processing of errors is necessary, you can
set up an `error_handler` node in your [config](config.md) which will receive and handle those errors.

Returning a normal go `error` is fine, or you can return the optional `firebolt.FBError` type which includes a separate error 
code and message.

An `error_handler` is just a special child node that receives the errors returned by it's parent.  It must accept a 
`firebolt.EventError` passed to Process().   Firebolt wraps the event and the error returned by the parent in this 
`EventError` structure, which includes a timestamp marking when the error occurred.

We often use an `error_handler` to send errors to a Kafka topic for retry or analysis.

### Processing
Because the payload `event.Payload` is of type  `interface{}`, the initial code in Process() will typically be a type assertion.   
In this example, events that don't match a pattern are filtered out:

```go
    func (f *FilterNode) Process(event *firebolt.Event) (*firebolt.Event, error) {
        str, ok := event.Payload.(string)
        if !ok {
            // return nil, errors.New("failed type assertion for conversion to string")
            // In this example, we use FBError to get structured errors in our error_handler output
            return nil, firebolt.NewFBError("ERR_CONVERSION_FAILED", "failed type assertion for conversion to string")
        }
        if strings.HasPrefix(str, "filter") {
            // return a nil event, child nodes will not be invoked for this event
            return nil, nil
        }
        // return an event, child nodes will be invoked with this event
        return event.WithPayload("new payload"), nil
    }
```
