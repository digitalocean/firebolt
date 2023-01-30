## Fanout Nodes

Fanout nodes are synchronous; they differ from `node.SyncNode` in that they return a slice of results.  Returning an
empty slice is equivalent to filtering out the inbound event.

Fanout `nodes` must implement the interface `node.FanoutNode`:

```go
type FanoutNode interface {
	Setup(config map[string]string) error
	Process(event *firebolt.Event) ([]firebolt.Event, error)
	Shutdown() error
	Receive(msg fbcontext.Message) error
}
```

There are additional methods (Init, AcceptsMessage) in the `Node` interface that are omitted from this doc; custom nodes 
should embed the type `fbcontext.ContextAware` to get a default implementation of these methods which are used to 
provide messaging support and access to firebolt methods.

The `Setup()` method is called once at application startup, and passed a map of all `params` that are defined for this 
node in your config file.  

`Process()` is called with each individual event.   It should return `[]result, nil` to transform or pass through the event, 
`[]result{}, nil` i.e. an empty slice to filter out this event (child nodes will not be invoked), or `nil, error` to indicate 
an error condition.

`Receive(msg)` is called when a message (see: [messaging](messaging.md)) is received of a messageType that your node 
subscribes to.   To subscribe to a messagetype, in your `Setup()` method you may call the `Subscribe()` method on your 
node, which was provided by embedding `fbcontext.ContextAware`.

The `Shutdown()` method gives your node an opportunity to clean up any resources during system shutdown.

### Error Handling
see [Error Handling for sync nodes](./sync-nodes.md#error-handling)


### Processing
similar to [Procesing for sync nodes](./sync-nodes.md#processing) with the exception of returning a slice of `firebolt.Event`
