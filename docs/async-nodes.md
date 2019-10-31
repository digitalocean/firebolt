## Asynchronous Nodes

Some nodes need to return results asynchronously.   One example would be persisting data to an external datastore; you may
wish to fire off an operation against the datastore in a goroutine and return immediately, passing back a transfomed 
event or an error later.

Async `nodes` must implement the interface `node.AsyncNode`:

```go
type AsyncNode interface {
	Setup(config map[string]string) error
	ProcessAsync(event *firebolt.AsyncEvent)
	Shutdown() error
	Receive(msg fbcontext.Message) error
}
```

There are a couple of additional methods (Init, AcceptsMessage) in the `AsyncNode` interface that are omitted from this doc; most 
node types will satisfy the requirement for those methods by embedding the type `fbcontext.ContextAware` to get a 
default implementation of messaging support and access to firebolt methods.

The `Setup()` method is called once at application startup, and passed a map of all `params` that are defined for this 
node in your config file.  

`ProcessAsync()` is called with each individual event and does not return a value; your code is expected to return quickly.  
Should it block for an extended period of time, it's channel will fill and back up through the application.

When processing is complete, invoke one of the callback functions on the `AsyncEvent` based on the result:
 * to pass an event to children, `event.ReturnEvent(transformedEvent)`
 * to return an error, `event.ReturnError(err)`
 * to filter out this event from the stream passed to children, `event.ReturnFiltered()`

`Receive(msg)` is called when a message (see: [messaging](messaging.md)) is received of a messageType that your node 
subscribes to.   To subscribe to a messagetype, in your `Setup()` method you may call the `Subscribe()` method on your 
node, which was provided by embedding `fbcontext.ContextAware`.

The `Shutdown()` method gives your node an opportunity to clean up any resources during system shutdown.

### Error Handling
When an error occurs during `ProcessAsync()`, return the error with `event.ReturnError(error)`.  If additional processing 
of errors is necessary, you can set up an `error_handler` node in your [config](config.md) which will receive and handle 
those errors.

Returning a normal go `error` is fine, or you can return the optional `firebolt.FBError` type which includes a separate error 
code and message.

An `error_handler` is just a special child node that receives the errors returned by it's parent.  It must accept a 
`firebolt.EventError` passed to `Process()` (or `ProcessAsync()`, if your error handler is asynchronous).   Firebolt wraps 
the event and the error returned by the parent in this `EventError` structure, which includes a timestamp marking when 
the error occurred.

We often use an `error_handler` to send errors to a Kafka topic for retry or analysis.

### Processing
Because the payload `event.Payload` is of type  `interface{}`, the initial code in ProcessAsync() will typically be a type 
assertion.   In this example, events that don't match a pattern are filtered out.   For an async operation, a more 
realistic example would run the processing on a separate goroutine:

```go
    func (f *FilterNode) ProcessAsync(event *firebolt.AsyncEvent) {
        str, ok := event.Payload.(string)
        if !ok {
            event.ReturnError(firebolt.NewFBError("ERR_CONVERSION_FAILED", "failed type assertion for conversion to string"))
        }
        if strings.HasPrefix(str, "filter") {
            event.ReturnFiltered()
        }
        // return an event, child nodes will be invoked with this event
        event.ReturnEvent(event.WithPayload("new payload"))
    }
```
