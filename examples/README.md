# Examples

To run any example in this directory, use the run.sh script provided which will start the required infrastructure (kafka,
etc.) using docker compose and then run the program, e.g.:

```
./run.sh kafkatokafka
```

Prerequisites:  you'll need `docker-compose`, `go 1.13+`, and `librdkafka` (see: [Developing](../README.md#developing)) 
installed on your workstation to run these examples.

## Kafka to Kafka
see `examples/kafkatokafka`

In this example, syslog log lines are read from a kafka topic, a few fields are parsed and formatted as JSON, and the
result is written to a second kafka topic.

This uses one source and three nodes:

**kafkaconsumer** (built-in source)  -> **parser** (custom node) -> **jsonbuilder** (custom node) -> **kafkaproducer** (built-in node)

## Logging
see `examples/logging`

This example is a simple logging pipeline.   Logs lines in syslog format are read from a kafka topic 'logs-raw', and 
bulk indexed to Elasticsearch.  

This uses one source and two nodes:

**kafkaconsumer** (built-in source) -> **parser** (custom node) -> **docbuilder** (custom node) -> **elasticsearch** (built-in node)

If any errors occur during parsing, those records and the associated error are sent to the kafka topic 'logs-errors' via the
**errorkafkaproducer** built-in node.
