## Registry

Firebolt uses an internal registry to create instances of the nodes used in your application.

All custom types **must** be registered before the configuration file is loaded.   Firebolt will validate your configuration
file, checking that:
 * all source/node types are registered
 * each node returns the same datatype expected by it's children

Built in sources (`kafkaconsumer`) and nodes (`kafkaproducer`, `elasticsearch`) are registered by Firebolt itself and
do not need to be registered by your application.

When registering custom types, you must provide:
 * the name of the type, this will be the `name:` you use in the config file
 * a factory function that creates instances of your type
 * the `reflect.Type` that it expects to receive in Process()'s firebolt.Event.Payload
 * the `reflect.Type` that it expects to return from Process() in firebolt.Event.Payload, or `nil` for a sink that does not return data
 
### Registering a Node
This example registers a node type that expects event payloads of type `[]byte` and returns `string`:

```go
    node.GetRegistry().RegisterNodeType("filternode", func() node.Node {
        return &FilterNode{}
    }, reflect.TypeOf(([]byte)(nil)), reflect.TypeOf(""))
```

### Registering a Source
This example registers a source that emits event payloads of type `&RawMsg`

```go
    node.GetRegistry().RegisterSourceType("tcpreceiver", func() node.Source {
        return &TCPReceiver{}
    }, reflect.TypeOf(&RawMsg{}))
```
