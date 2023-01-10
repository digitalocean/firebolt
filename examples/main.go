package main

import (
	"os"
	"reflect"

	"github.com/digitalocean/captainslog"
	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/examples/kafkatokafka"
	"github.com/digitalocean/firebolt/examples/logging"
	"github.com/digitalocean/firebolt/examples/shared"
	"github.com/digitalocean/firebolt/executor"
	"github.com/digitalocean/firebolt/metrics"
	"github.com/digitalocean/firebolt/node"
	"github.com/digitalocean/firebolt/node/elasticsearch"
	"github.com/digitalocean/firebolt/testutil"
)

const (
	kafkatokafkaExample = "kafkatokafka"
	loggingExample      = "logging"
)

// choose an example to run; see 'runKafkaToKafka()' or 'runLogging()' for the actual example code
func main() {
	exampleName := os.Args[1]
	testutil.WaitForPort(nil, 9092) // wait for kafka to be ready
	testutil.WaitForPort(nil, 9200) // wait for elasticsearch to be ready

	// run the selected example
	if exampleName == kafkatokafkaExample {
		runKafkaToKafka()

		shared.ProduceTestData("ktk-source", 10)
		shared.ConsumeTestData("ktk-dest", 10)
	} else if exampleName == loggingExample {
		runLogging()

		shared.ProduceTestData("logs-raw", 120)
		shared.ConsumeTestData("logs-errors", 20)
		shared.CheckElasticsearchDocuments("logs", 100)
	} else {
		panic("unknown example: " + exampleName)
	}
}

func runKafkaToKafka() {
	metrics.Init(kafkatokafkaExample)

	// looking at the config file 'kafkatokafka/firebolt.yaml' for this example, there are four nodes in a chain:
	//   kafkaconsumer -> parser -> jsonbuilder -> kafkaproducer
	// the first and last are built-in to firebolt, but parser and jsonbuilder are custom nodes
	//
	// for each custom node type, we need to register a factory method with firebolt, using the same
	// name as in 'firebolt.yaml':
	//

	// register the parser node
	node.GetRegistry().RegisterNodeType("parser", func() node.Node {
		return &kafkatokafka.Parser{}
	}, reflect.TypeOf([]byte(nil)), reflect.TypeOf(captainslog.SyslogMsg{}))

	// register the jsonbuilder node
	node.GetRegistry().RegisterNodeType("jsonbuilder", func() node.Node {
		return &kafkatokafka.JSONBuilder{}
	}, reflect.TypeOf(captainslog.SyslogMsg{}), reflect.TypeOf((*firebolt.ProduceRequest)(nil)).Elem())

	// read the config file and start the firebolt executor
	ex, err := executor.New(executor.WithConfigFile("kafkatokafka/firebolt.yaml"))
	if err != nil {
		os.Exit(1)
	}
	go ex.Execute()
}

func runLogging() {
	metrics.Init(loggingExample)

	// looking at the config file 'logging/firebolt.yaml' for this example, there are four nodes in a chain:
	//   kafkaconsumer -> parser -> docbuilder -> elasticsearch
	// the first and last are built-in to firebolt, but parser and docbuilder are custom nodes
	//
	// there is also an 'error_handler' attached to the 'elasticsearch' node - any indexing errors will be passed to
	// this 'errorkafkaproducer' (a built-in node type) and produced to a kafka topic, and this example forces 20
	// elastisearch mapping errors to occur to demonstrate this feature
	//
	// for each custom node type, we need to register a factory method with firebolt, using the same
	// name as in 'firebolt.yaml':
	//

	// register the parser node
	node.GetRegistry().RegisterNodeType("parser", func() node.Node {
		return &logging.Parser{}
	}, reflect.TypeOf([]byte(nil)), reflect.TypeOf(captainslog.SyslogMsg{}))

	// register the docbuilder node
	node.GetRegistry().RegisterNodeType("docbuilder", func() node.Node {
		return &logging.DocBuilder{}
	}, reflect.TypeOf(captainslog.SyslogMsg{}), reflect.TypeOf(elasticsearch.IndexRequest{}))

	// read the config file and start the firebolt executor
	ex, err := executor.New(executor.WithConfigFile("logging/firebolt.yaml"))
	if err != nil {
		os.Exit(1)
	}
	go ex.Execute()
}
