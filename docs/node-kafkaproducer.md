# Kafka Producer

 ||||
 |--------------|:--------:|--------|
 | **Accepts:** | `[]byte` | Parent node should serialize to the byte representation that will be put on the kafka topic |
 | **Returns:** |  n/a     | All events are filtered; this node acts as a sink |


The `kafkaproducer` is a built-in node type for producing events onto a kafka topic.   Any encoding can be used, but you
must perform the encoding and convert to `[]byte` in the parent node.

Internally, `kafkaproducer` uses an async producer based on the `confluent-kafka-go` client.

## Configuration

Param                     | Required | Default | Description              
--------------------------|:--------:|---------|--------------
brokers                   |  *       |         | comma-separated list of Kafka brokers to use for initial cluster connection
topic                     |  *       |         | destination topic name 


In addition to these parameters, you can use any `librdkafka` [configuration parameter](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
by prefixing with `librdkafka.` as in this example, which configures zstandard compression (firebolt uses snappy by default):

```yaml
  params:
    brokers: 127.0.0.1:9092
    topic: testtopic
    librdkafka.compression.codec: zstd
```
