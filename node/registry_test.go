package node

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/node/kafkaconsumer"
	"github.com/digitalocean/firebolt/node/kafkaproducer"
)

func registerNodeTypes() {
	// built-ins
	GetRegistry().RegisterSourceType("kafkaconsumer", func() Source {
		return &kafkaconsumer.KafkaConsumer{}
	}, reflect.TypeOf(([]byte)(nil)))

	GetRegistry().RegisterNodeType("kafkaproducer", func() Node {
		return &kafkaproducer.KafkaProducer{}
	}, reflect.TypeOf(([]byte)(nil)), nil)

	GetRegistry().RegisterNodeType("errorkafkaproducer", func() Node {
		return &kafkaproducer.ErrorProducer{}
	}, reflect.TypeOf(&firebolt.EventError{}), nil)
}

func TestRegistry(t *testing.T) {
	registerNodeTypes()
	registry := GetRegistry()

	// singleton should always return the same instance
	if registry != GetRegistry() {
		t.Error("different instances of registry found")
	}

	// default source types should be found & instantiated
	source := registry.InstantiateSource("kafkaconsumer")
	assert.NotNil(t, source, "source kafkaconsumer should have been created")

	// default node types should be found & instantiated
	producer := registry.InstantiateNode("kafkaproducer")
	assert.NotNil(t, producer, "node kafkaproducer should have been created")

	// a second instance of a node type should be a new, independent instance
	producer2 := registry.InstantiateNode("kafkaproducer")
	assert.False(t, &producer == &producer2)
}

func TestCustomTypes(t *testing.T) {
	registry := GetRegistry()

	// custom source types should be able to be registered & instantiated
	registry.RegisterSourceType("customsourcetype", func() Source {
		return &kafkaconsumer.KafkaConsumer{}
	}, reflect.TypeOf(""))
	customsource := registry.InstantiateSource("customsourcetype")
	assert.NotNil(t, customsource, "custom source should have been created")

	// custom node types should be able to be registered & instantiated
	registry.RegisterNodeType("customnodetype", func() Node {
		return &kafkaproducer.KafkaProducer{}
	}, reflect.TypeOf(""), nil)
	customnode := registry.InstantiateNode("customnodetype")
	assert.NotNil(t, customnode, "custom node should have been created")
}

func TestInvalidSourceType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on attempt to instantiate invalid source type")
		}
	}()

	registry := GetRegistry()
	registry.InstantiateSource("source-that-does-not-exist")
}

func TestInvalidNodeType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on attempt to instantiate invalid node type")
		}
	}()

	registry := GetRegistry()
	registry.InstantiateNode("node-that-does-not-exist")
}
