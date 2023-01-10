package config_test

import (
	"testing"

	"github.com/digitalocean/firebolt/node/kafkaproducer"

	"strings"

	"reflect"

	"os"

	"github.com/stretchr/testify/assert"

	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/config"
	"github.com/digitalocean/firebolt/node"
	"github.com/digitalocean/firebolt/node/kafkaconsumer"
	"github.com/digitalocean/firebolt/node/syslogparser"
)

func TestParseConfig(t *testing.T) {
	registerTestNodeTypes()

	// test ENV substitution
	_ = os.Setenv("KAFKA_BROKERS", "kafkabroker:9092")

	c, err := config.Read("../testdata/testconfig_sampleapp.yaml")
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, "sampleapp", c.ApplicationName)
	assert.Equal(t, "sample", c.MetricsConfig.Prefix)
	assert.Equal(t, 4321, c.MetricsConfig.Port)
	assert.Equal(t, "127.0.0.1:2181", c.Zookeeper)
	assert.Equal(t, "/leaderelection", c.ZkElectionPath)

	assert.Equal(t, "kafka", c.InternalData.Transport)
	assert.Equal(t, "firebolt-messages", c.InternalData.Params["messagetopic"])

	assert.Equal(t, "kafkaconsumer", c.Source.Name)
	assert.Equal(t, "kafkabroker:9092", c.Source.Params["brokers"])
	assert.Equal(t, "testapp", c.Source.Params["consumergroup"])
	assert.Equal(t, "logs-all", c.Source.Params["topic"])
	assert.Equal(t, "1000", c.Source.Params["buffersize"])

	assert.Equal(t, 2, len(c.Nodes))

	n1 := c.Nodes[0]
	assert.Equal(t, "firstnode", n1.Name)
	assert.Equal(t, "firstnode", n1.ID)
	assert.Equal(t, 1, n1.Workers)
	assert.Equal(t, 100, n1.BufferSize)
	assert.Equal(t, 1, len(n1.Children))
	assert.Equal(t, 2, len(n1.Params))
	assert.Equal(t, "value1.1", n1.Params["param1.1"])
	assert.Equal(t, "value1.2", n1.Params["param1.2"])
	assert.Equal(t, false, n1.DiscardOnFullBuffer)
	assert.Nil(t, n1.ErrorHandler)

	n1c1 := n1.Children[0]
	assert.Equal(t, "secondnode", n1c1.Name)
	assert.Equal(t, "secondnode", n1c1.ID)
	assert.Equal(t, 1, n1c1.Workers)
	assert.Equal(t, 1, n1c1.BufferSize)
	assert.Equal(t, 1, len(n1c1.Children))
	assert.Equal(t, 0, len(n1c1.Params))
	assert.Equal(t, "errorkafkaproducer", n1c1.ErrorHandler.Name)
	assert.Equal(t, 1, n1c1.ErrorHandler.Workers)
	assert.Equal(t, 100, n1c1.ErrorHandler.BufferSize)
	assert.Equal(t, true, n1c1.ErrorHandler.DiscardOnFullBuffer)

	n1c1c1 := n1c1.Children[0]
	assert.Equal(t, "thirdnode", n1c1c1.Name)
	assert.Equal(t, "third-node-id", n1c1c1.ID)
	assert.Equal(t, 3, n1c1c1.Workers)
	assert.Equal(t, 300, n1c1c1.BufferSize)
	assert.Equal(t, 0, len(n1c1c1.Children))
	assert.Equal(t, 2, len(n1c1c1.Params))
	assert.Equal(t, "value3.1", n1c1c1.Params["param3.1"])
	assert.Equal(t, "value3.2", n1c1c1.Params["param3.2"])
	assert.Nil(t, n1c1c1.ErrorHandler)

	n2 := c.Nodes[1]
	assert.Equal(t, "fourthnode", n2.Name)
	assert.Equal(t, 4, n2.Workers)
	assert.Equal(t, 400, n2.BufferSize)
	assert.Equal(t, 0, len(n2.Children))
	assert.Equal(t, 0, len(n2.Params))
	assert.Equal(t, true, n2.Disabled)
}

func registerTestNodeTypes() {
	// built-ins
	node.GetRegistry().RegisterSourceType("kafkaconsumer", func() node.Source {
		return &kafkaconsumer.KafkaConsumer{}
	}, reflect.TypeOf(([]byte)(nil)))

	node.GetRegistry().RegisterNodeType("kafkaproducer", func() node.Node {
		return &kafkaproducer.KafkaProducer{}
	}, reflect.TypeOf((*firebolt.ProduceRequest)(nil)).Elem(), nil)

	node.GetRegistry().RegisterNodeType("errorkafkaproducer", func() node.Node {
		return &kafkaproducer.ErrorProducer{}
	}, reflect.TypeOf(&firebolt.EventError{}), nil)

	// test types
	node.GetRegistry().RegisterNodeType("firstnode", func() node.Node {
		return &syslogparser.SyslogParser{}
	}, reflect.TypeOf(([]byte)(nil)), reflect.TypeOf(""))
	node.GetRegistry().RegisterNodeType("secondnode", func() node.Node {
		return &syslogparser.SyslogParser{}
	}, reflect.TypeOf(""), reflect.TypeOf(""))
	node.GetRegistry().RegisterNodeType("thirdnode", func() node.Node {
		return &syslogparser.SyslogParser{}
	}, reflect.TypeOf(""), reflect.TypeOf(""))
	node.GetRegistry().RegisterNodeType("fourthnode", func() node.Node {
		return &syslogparser.SyslogParser{}
	}, reflect.TypeOf(([]byte)(nil)), reflect.TypeOf(""))
	node.GetRegistry().RegisterNodeType("errorhandlernode", func() node.Node {
		return &syslogparser.SyslogParser{}
	}, reflect.TypeOf(&firebolt.EventError{}), nil)
}

func TestParseMissingFile(t *testing.T) {
	c, err := config.Read("../testdata/does-not-exist.yaml")
	if err == nil {
		t.Error("expected an error, config file does not exist")
	}
	if c != nil {
		t.Error("expected config to be nil, config file does not exist")
	}
}

func TestDuplicateIDValues(t *testing.T) {
	c, err := config.Read("../testdata/testconfig_errorDupID.yaml")
	if err == nil {
		t.Error("expected an error, duplicate ID values are present")
	}
	if c != nil {
		t.Error("expected config to be nil, config file does not exist")
	}
	assert.Equal(t, "multiple nodes exist with the same id myveryfirstnode; set an explicit 'id' in your config to make them unique", err.Error())
}

func TestInvalidYaml(t *testing.T) {
	c, err := config.Read("../testdata/testconfig_invalidYaml.yaml")
	if err == nil {
		t.Error("expected an error, unparseable YAML")
	}
	if c != nil {
		t.Error("expected config to be nil, unparseable YAML")
	}
	assert.True(t, strings.HasPrefix(err.Error(), "yaml: unmarshal errors"))
}

func TestErrorHandlerMustNotHaveChildren(t *testing.T) {
	readConfigAndExpectError(t, "../testdata/testconfig_errorHandlerHasChildren.yaml", "error_handler nodes may not have children for node errorhandlernode")
}

func TestErrorHandlerMustNotHaveErrorHandler(t *testing.T) {
	readConfigAndExpectError(t, "../testdata/testconfig_errorHandlerHasErrorHandler.yaml", "error_handler nodes may not have an error_handler of their own for node errorhandlernode")
}

func TestRootNodeTypeNotRegistered(t *testing.T) {
	readConfigAndExpectError(t, "../testdata/testconfig_rootNodeTypeNotRegistered.yaml", "node type not-registered-node-type not registered")
}

func TestChildNodeTypeNotRegistered(t *testing.T) {
	readConfigAndExpectError(t, "../testdata/testconfig_childNodeTypeNotRegistered.yaml", "node type not-registered-node-type not registered")
}

func TestErrorHandlerTypeNotRegistered(t *testing.T) {
	readConfigAndExpectError(t, "../testdata/testconfig_errorHandlerTypeNotRegistered.yaml", "error_handler node type not-registered-err-handler not registered")
}

func TestErrorHandlerConsumesWrongType(t *testing.T) {
	readConfigAndExpectError(t, "../testdata/testconfig_errorHandlerConsumesWrongType.yaml", "error_handler node type secondnode must consume EventError, actually consumes string")
}

func TestSourceToRootNodeTypeMismatch(t *testing.T) {
	readConfigAndExpectError(t, "../testdata/testconfig_sourceToRootNodeTypeMismatch.yaml", "source type kafkaconsumer produces []uint8, but root node secondnode consumes incompatible type string")
}

func TestNodeToChildNodeTypeMismatch(t *testing.T) {
	readConfigAndExpectError(t, "../testdata/testconfig_nodeToChildNodeTypeMismatch.yaml", "node type firstnode produces string, but child firstnode consumes incompatible type []uint8")
}

// internalData is not required
func TestNoInternalData(t *testing.T) {
	registerTestNodeTypes()
	c, err := config.Read("../testdata/testconfig_noInternalData.yaml")
	assert.Nil(t, err)
	assert.Nil(t, c.InternalData)
}

func TestInvalidInternalData(t *testing.T) {
	readConfigAndExpectError(t, "../testdata/testconfig_invalidInternalData.yaml", "internal data transport cassandra not supported")
}

func readConfigAndExpectError(t *testing.T, configFile string, expectedMsg string) {
	registerTestNodeTypes()

	c, err := config.Read(configFile)
	if err == nil {
		t.Error("expected an error")
	}
	if c != nil {
		t.Error("expected config to be nil")
	}
	assert.Equal(t, expectedMsg, err.Error())
}

func TestParseConfig_WithLegacyMetrics(t *testing.T) {
	registerTestNodeTypes()
	c, err := config.Read("../testdata/testconfig_withLegacyMetrics.yaml")
	assert.Nil(t, err)
	assert.Equal(t, "legacy", c.MetricsConfig.Prefix)
	assert.Equal(t, 4321, c.MetricsConfig.Port)
	assert.Equal(t, true, c.MetricsConfig.Enabled)
}
