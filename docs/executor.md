# Execution

Firebolt's `executor` runs your application based on the config file's contents.   The `node.Source` emits events, which
are passed to each of the root `node.Node`s, and the events emitted by each node are passed to its children.

For each event that arrives at a node, the node may:
 * emit a result event that will be passed to all children
 * return an `error` to indicate the event could not be processed
 * or return `nil` to filter the event from further processing
 
Errors are counted in metrics and passed to the node's `error_handler` (just another `node.Node`) if one is configured. 
Any `nil`s that are returned will not be passed down to children, providing a simple way to write a filter.

## Options

A few options are available to configure how the Firebolt `executor` runs your pipeline:

Param                     | Required | Default | Description              
--------------------------|:--------:|:-------:|--------------
workers                   |          | 1       | number of goroutines to use to process this node's events in each application instance
buffersize                |          | 1       | buffer this node draws work from
disabled                  |          | `false` | disable a node (and all children) from receiving events
discard_on_full_buffer    |          | `false` | discard events destined for this node (and children) when its buffer is full

An example of using these in a config file:
```yaml
...
  name: elasticsearch
  workers: 3
  buffersize: 100
  discard_on_full_buffer: true
  disabled: false
```

For each node, you may configure the size of its input channel buffer `buffersize` and the number of `workers` (goroutines) that
will be used to process events on that channel.  Large buffers tend to only be useful before nodes that do I/O or significant
computation.

At a sufficiently high data rate backpressure can cascade through the system and the Source can fall behind.   This isn't the
desired behavior for all applications; if some or all of your data processing is "best-effort" you can use the configuration
`discard_on_full_buffer: true` on a node to accept the loss of data in that node and its descendants while preventing that
subtree from propagating backpressure to the node's parent.

The firebolt executor exposes Prometheus metrics intended to help monitor and tune your application; see the [Metrics](metrics.md) document
for more information.
