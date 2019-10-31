package node

import (
	"reflect"
	"sync"

	log "github.com/sirupsen/logrus"
)

// Registry is the singleton containing all source and node types registered in firebolt and therefore available to use
// in your `firebolt.yaml` configuration.   Built-in types can be used immediately, while any custom sources or nodes
// that your application provides will need to be registered with `registry.Get().RegisterSourceType` or
// `registry.Get().RegisterNodeType` respectively.
type Registry struct {
	sourceTypes map[string]*SourceRegistration
	nodeTypes   map[string]*Registration
}

// SourceRegistration is the record representing a source type and metadata about that source
type SourceRegistration struct {
	factory  func() Source
	Produces reflect.Type
}

// Registration is the record representing a node type and metadata about that node
type Registration struct {
	factory  func() Node
	Consumes reflect.Type
	Produces reflect.Type
}

var singleton *Registry
var once sync.Once

// GetRegistry returns the singleton instance of the source/node registry.   The registry is lazily initialized on the first
// invocation.
func GetRegistry() *Registry {
	once.Do(func() {
		singleton = &Registry{
			make(map[string]*SourceRegistration),
			make(map[string]*Registration),
		}
	})
	return singleton
}

// GetSourceRegistration returns the SourceRegistration associated with a source type name, or nil if no registration exists
func (r *Registry) GetSourceRegistration(sourceType string) *SourceRegistration {
	return r.sourceTypes[sourceType]
}

// GetNodeRegistration returns the NodeRegistration associated with a node type name, or nil if no match exists
func (r *Registry) GetNodeRegistration(nodeType string) *Registration {
	return r.nodeTypes[nodeType]
}

// InstantiateSource creates and returns a Source instance of the passed type.   This method will panic in the event
// that the source type does not exist.
func (r *Registry) InstantiateSource(sourceType string) Source {
	registration := r.sourceTypes[sourceType]
	if registration == nil {
		log.Errorf("no source registered for type %s", sourceType)
		panic("no source registered for type")
	}

	return registration.factory()
}

// InstantiateNode creates and returns a Node instance of the passed type.   This method will panic in the event
// that the node type does not exist.
func (r *Registry) InstantiateNode(nodeType string) Node {
	registration := r.nodeTypes[nodeType]
	if registration == nil {
		log.Errorf("no node registered for type %s", nodeType)
		panic("no node registered for type")
	}

	return registration.factory()
}

// RegisterNodeType registers a new node type on behalf of application code; all node types referenced in the configuration
// must be registered before the executor is invoked.
func (r *Registry) RegisterNodeType(nodeType string, factory func() Node, consumes reflect.Type, produces reflect.Type) {
	reg := &Registration{
		factory:  factory,
		Consumes: consumes,
		Produces: produces,
	}
	log.WithField("node_name", nodeType).Debug("registered node type")
	r.nodeTypes[nodeType] = reg
}

// RegisterSourceType registers a new source type on behalf of application code; the source type referenced in the
// configuration must be registered before the executor is invoked.
func (r *Registry) RegisterSourceType(sourceType string, factory func() Source, produces reflect.Type) {
	reg := &SourceRegistration{
		factory:  factory,
		Produces: produces,
	}
	log.WithField("source_name", sourceType).Debug("registered source type")
	r.sourceTypes[sourceType] = reg
}
