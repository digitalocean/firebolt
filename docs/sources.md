# Sources

A source is a special type of node that acts as the root of the processing hierarchy and the source of all events
that flow through a Firebolt application.

Sources must implement a simple interface:

```go
type Source interface {
	Setup(config map[string]string, recordsch chan []byte) error
	Start() error
	Shutdown() error
	Receive(msg fbcontext.Message) error
}
```

There are a couple of additional methods (Init, AcceptsMessage) in the `Source` interface that are omitted from this doc; most 
custom sources will satisfy the requirement for those methods by embedding the type `fbcontext.ContextAware` to get a 
default implementation of messaging support and access to firebolt methods.

The `Setup()` method is passed a map of all `params` that are defined for this node in your config file, and the channel
onto which your source should produce event data.   It should not produce any messages onto the channel until Start()
is called.

`Start()` should initiate your source emitting messages onto `recordschan`.   Firebolt will supervise and restart your source
on failure, which is detected as any return from `Start()`.   For this reason your source should not return from Start()
as long as it is functioning properly, and should only return during system shutdown or because it has encountered an
internal failure.

The `Shutdown()` method gives your node an opportunity to clean up any resources during system shutdown.

`Receive(msg)` is called when a message is received of a messageType that your source subscribes to.   To subscribe to a messagetype,
in your `Setup()` method you may call the `Subscribe()` method on your source, which was provided by embedding `fbcontext.ContextAware`.


## Kafka Consumer

One built-in `node.Source` is provided named `kafkaconsumer` and based on the confluent-kafka-go & librdkafka kafka client.

This source wraps the `confluent-kafka-go` consumer, adding some offset management features that are helpful in recovering
from outages without operator intervention.

By default most kafka consumer support using the stored offsets plus "earliest" (aka low watermark) and 
"latest" (aka highwatermark) as initial values; for real-world applications these options are often inadequate.

Use `maxpartitionlag` to reduce the initial lag behind latest for each partition if your application needs to resume processing
realtime data promptly after an outage.   

This means that the gap between the stored offsets and the new data would be lost; you can use the parallel recovery config
values shown below to automatically run throttled "fill-in" recovery for those events.  


## Kafka Consumer Configuration

The available configuration parameters are demonstrated here:

```yaml
source:                                
  name: kafkaconsumer
  params:
    brokers: kafka01:9092,kafka02:9092    # environment variables e.g. '${KAFKA_BROKERS}' are supported
    consumergroup: testapp                # the 'group.id' for your consumer group
    topic: logs-all                       # the kafka topic name to consume from
    buffersize: 1000                      # we use the `confluent-kafka-go` channel based consumer, this sets that channel's buffer size
    librdkafka.session.timeout.ms: 90000  # use the `librdkafka.` prefix to pass arbitrary config to librdkafka, which underlies `confluent-kafka-go`
    maxpartitionlag: 500                  # on startup when partitions are assigned, limit the initial lag behind the highwatermark for a partition to ensure that we "catch up" to realtime quickly
    parallelrecoveryenabled: true         # [default: false] when `maxpartitionlag` limits initial lag, start a parallel consumer to fill-in the window of data that would otherwise be lost
    parallelrecoverymaxrecords: 100000    # sets a limit on the total # of events *per partition* that will be recovered
    parallelrecoverymaxrate: 1500         # max rate in events/sec that parallel recovery will process *per instance* of your application, to avoid overwhelming downstream infrastructure
```

## Developing Sources

To develop your own `node.Source` you need to implement the `node.Source` interface and register your new source before
starting your application.

```go
type Source interface {
	Setup(config map[string]string, eventschan chan []byte) error
	Start() error
	Shutdown() error
	Receive(msg fbcontext.Message) error
}
```

Your `Setup()` method will be called once after the instance of the source is constructed.   It is passed `config` a map of
the `params` associated with your source in the config file, and `eventschan` which is the channel that you should write
events to.   Save that channel in a struct and don't start writing to it until Start() is called.

When `Start()` is called it's time to start putting events on the `eventschan`; so if you wanted to run a TCP listener for
example it would need to start accepting connections here.   This method **should not return** because Firebolt considers
that a sign that your source has failed, and the supervisor will recreate and restart your source when that happens.

If a signal like SIGINT or SIGHUP is received, Firebolt will start a shutdown sequence that includes calling `Shutdown()`.
 Cleanup any resources and return from the `Start()` call at this time to enable a clean shutdown.
 