package main

import (
	"os"
	"reflect"

	"github.com/digitalocean/captainslog"
	"github.com/digitalocean/firebolt/examples/kafkatokafka"
	"github.com/digitalocean/firebolt/examples/shared"
	"github.com/digitalocean/firebolt/executor"
	"github.com/digitalocean/firebolt/metrics"
	"github.com/digitalocean/firebolt/node"
	"github.com/digitalocean/firebolt/util"
)

func main() {
	exampleName := os.Args[1]
	util.WaitForPort(nil, 9092)

	// run the selected example
	if exampleName == "kafkatokafka" {
		runKafkaToKafka()
	} else {
		panic("unknown example: " + exampleName)
	}

	// produce test data, consume any results
	shared.ProduceTestData(10)
	shared.ConsumeTestData("ktk-dest", 10)
}

func runKafkaToKafka() {
	metrics.Init("kafkatokafka")

	// looking at the config file 'firebolt.yaml' for this example, there are four nodes in a chain:
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
	}, reflect.TypeOf(captainslog.SyslogMsg{}), reflect.TypeOf([]byte(nil)))

	// read the config file and start the firebolt executor
	ex, err := executor.New(executor.WithConfigFile("kafkatokafka/firebolt.yaml"))
	if err != nil {
		os.Exit(1)
	}
	go ex.Execute()
}
