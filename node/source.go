package node

import (
	"github.internal.digitalocean.com/observability/firebolt"
	"github.internal.digitalocean.com/observability/firebolt/fbcontext"
)

// Source is a root node that generates messages to be processed or receives them from an external system.
type Source interface {
	Init(id string, ctx fbcontext.FBContext)
	AcceptsMessage(messageType string) bool
	Setup(config map[string]string, eventchan chan firebolt.Event) error
	Start() error
	Shutdown() error
	Receive(msg fbcontext.Message) error
}

// SourceConfig is the set of configuration values for this Source
type SourceConfig struct {
	Name   string            `yaml:"name"`
	ID     string            `yaml:"id"`
	Params map[string]string `yaml:"params"`
}
