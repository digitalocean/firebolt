package main

import (
	"os"
	"reflect"

	"github.com/digitalocean/firebolt/util"

	"github.com/digitalocean/firebolt/examples/shared"

	"github.com/digitalocean/firebolt/executor"
	"github.com/digitalocean/firebolt/metrics"

	"github.com/digitalocean/firebolt/examples/kafkatokafka"

	"github.com/digitalocean/captainslog"

	"github.com/digitalocean/firebolt/node"
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

	// register the parser node
	node.GetRegistry().RegisterNodeType("parser", func() node.Node {
		return &kafkatokafka.Parser{}
	}, reflect.TypeOf([]byte(nil)), reflect.TypeOf(captainslog.SyslogMsg{}))

	// register the jsonbuilder node
	node.GetRegistry().RegisterNodeType("jsonbuilder", func() node.Node {
		return &kafkatokafka.JSONBuilder{}
	}, reflect.TypeOf(captainslog.SyslogMsg{}), reflect.TypeOf([]byte(nil)))

	// start the firebolt executor
	ex, err := executor.New(executor.WithConfigFile("kafkatokafka/firebolt.yaml"))
	if err != nil {
		os.Exit(1)
	}
	go ex.Execute()
}
