## Configuration

Firebolt is configured with a YAML file that defines the flow of data from a source and through each node.

Every node must have a unique ID - Firebolt validates unique IDs on startup, and will exit with an error if duplicates 
exist.   The default value for a node's ID is the `name`, but you can override this value by explicitly setting `id` to 
ensure that it is unique.   This is only required when your application uses the same node type more than once.

Each node name used in the config must be [registered](registry.md) before the config is used.

This example is somewhat complex because it demonstrates many of firebolt's features:

```yaml
application: sampleapp
metricsprefix: sample                   # all prometheus metrics will be prefixed with 'sample_'
metricsport: 4321                       # an HTTP server exposing prometheus-compatible metrics will be started on this port
zookeeper: 127.0.0.1:2181               # a comma-separated list of Zookeeper nodes; optional but leader election is disabled without
zkleaderelectionpath: /leaderelection   # a zookeeper node path to be used for cluster leader election
internaldata:                           # kafka configuration used to store internal firebolt data such as messages; required by the 'kafkaconsumer' source
  transport: kafka                      # kafka is currently the only supported messaging transport
  params:
    brokers: ${KAFKA_BROKERS}           # kafka cluster is required
    messagetopic: firebolt-messages     # kafka compact topic to use for communicating messages to source/nodes, required
source:                                 # one and only one source is required
  name: kafkaconsumer
  params:
    brokers: ${KAFKA_BROKERS}           # environment variables are supported
    consumergroup: testapp
    topic: logs-all
    buffersize: 1000                    # sources do not normally need buffering; this value is a pass-thru to the underlying kafka consumer
nodes:
  - name: firstnode
    workers: 1                          # each node can be configured to run any number of workers (goroutines), the default is 1
    buffersize: 100                     # each node has a buffered input channel for the data that is ready to be processed, default size is 1
    params:                             # params are passed as a map to the node's Setup() during initialization
      param1.1: value1.1
      param1.2: value1.2
    children:                           # a node may have many children, the events returned by the node are passed to all child node's input channels
      - name: secondnode
        error_handler:                  # errors returned by 'secondnode' will be passed to this error handler
          name: errorkafkaproducer      # we provide built-in 'errorkafkaproducer' that writes JSON error reports to a Kafka topic
          buffersize: 100
          discard_on_full_buffer: true  # if the buffer is full discard messages to avoid sending backpressure downstream for a low priority function
        children:
          - name: thirdnode
            id: third-node-id           # you can use the same node type in your hierarchy twice, but its id (defaults to name) must be unique
            workers: 3
            buffersize: 300
            params:
              param3.1: value3.1
              param3.2: value3.2
  - name: fourthnode                    # there can be more than one node at the root level; they will all get all events from the source
    disabled: true                      # a node may be disabled, which will disable all children as well
    workers: 4
    buffersize: 400
```
