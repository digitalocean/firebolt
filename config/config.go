package config

import (
	"fmt"

	"os"
	"reflect"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"

	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/node"
)

// Config is the firebolt application configuration, including the configured source and all processing nodes
type Config struct {
	Version         string              `yaml:"version"`
	ApplicationName string              `yaml:"application"`
	MetricsConfig   *MetricsConfig      `yaml:"metrics"`
	MetricsPrefix   string              `yaml:"metricsprefix"`
	MetricsPort     int                 `yaml:"metricsport"`
	Zookeeper       string              `yaml:"zookeeper"`
	ZkElectionPath  string              `yaml:"zkleaderelectionpath"`
	InternalData    *InternalDataConfig `yaml:"internaldata"`
	Source          *node.SourceConfig  `yaml:"source"`
	Nodes           []*node.Config      `yaml:"nodes"`
	ShutdownTimeOut int                 `yaml:"shutdowntimeout"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Prefix  string `yaml:"prefix"`
	Port    int    `yaml:"port"`
}

// InternalDataTransportKafka is the transport name for using Kafka as the internal data transport.
const InternalDataTransportKafka = "kafka"

// InternalDataConfig is the set of configuration values for all consumers of the internal datastore
type InternalDataConfig struct {
	Transport string            `yaml:"transport"`
	Params    map[string]string `yaml:"params"`
}

// Read reads the file at the provided path and parses a firebolt configuration.   It may return an error if there
// is any failure while reading or parsing this file.
func Read(file string) (*Config, error) {
	c := Config{}

	data, err := os.ReadFile(file)
	if err != nil {
		log.WithError(err).Error("failed to read config file")
		return nil, err
	}

	// resolve ENV vars
	configBytes := []byte(os.ExpandEnv(string(data)))

	err = yaml.Unmarshal(configBytes, &c)
	if err != nil {
		log.Errorf("config file parse error: %v", err)
		return nil, err
	}

	setDefaults(&c)

	err = validate(c)
	if err != nil {
		log.Error("startup failed due to configuration validation errors")
		return nil, err
	}

	if c.ShutdownTimeOut <= 0 {
		c.ShutdownTimeOut = 10
	}

	log.WithField("configfile", file).Info("read config file")
	return &c, nil
}

func validate(config Config) error {
	// validate that there are no duplicate IDs first; this ensures that any errors later will be identifiable to a particular node
	allIDs := make(map[string]struct{})
	for _, n := range config.Nodes {
		err := validateUniqueID(allIDs, n)
		if err != nil {
			return err
		}
	}

	//TODO: validate metrics prefix does not contain illegal chars like dash, must adhere to this regex: [a-zA-Z_:]([a-zA-Z0-9_:])*, also must be short enough not to exceed the length limit

	// validate internaldata configuration
	err := validateInternalDataConfig(config)
	if err != nil {
		return err
	}

	// validate source
	err = validateSourceConfig(config)
	if err != nil {
		return err
	}

	// validate individual node configuration
	for _, n := range config.Nodes {
		err := validateNodeConfig(n)
		if err != nil {
			return err
		}
	}

	return nil
}

func validateInternalDataConfig(c Config) error {
	// if internalData is configured, it must have a transport and currently only 'kafka' is supported
	if c.InternalData != nil {
		if c.InternalData.Transport != "kafka" {
			return fmt.Errorf("internal data transport %s not supported", c.InternalData.Transport)
		}
	}

	return nil
}

func validateSourceConfig(c Config) error {
	// source type must exist in registry
	r := node.GetRegistry().GetSourceRegistration(c.Source.Name)
	if r == nil {
		return fmt.Errorf("source type %s not registered", c.Source.Name)
	}

	// source output type must match all root nodes input type
	for _, n := range c.Nodes {
		nodeReg := node.GetRegistry().GetNodeRegistration(n.Name)
		if nodeReg == nil {
			return fmt.Errorf("node type %s not registered", n.Name)
		}

		if r.Produces != nodeReg.Consumes {
			return fmt.Errorf("source type %s produces %s, but root node %s consumes incompatible type %s", c.Source.Name, r.Produces.String(),
				n.Name, nodeReg.Consumes.String())
		}
	}

	return nil
}

func validateUniqueID(allIDs map[string]struct{}, node *node.Config) error {
	if _, ok := allIDs[node.ID]; ok {
		return fmt.Errorf("multiple nodes exist with the same id %s; set an explicit 'id' in your config to make them unique", node.ID)
	}
	allIDs[node.ID] = struct{}{}

	for _, child := range node.Children {
		return validateUniqueID(allIDs, child)
	}
	return nil
}

func validateNodeConfig(n *node.Config) error {
	// node type must exist in registry
	r := node.GetRegistry().GetNodeRegistration(n.Name)
	if r == nil {
		return fmt.Errorf("node type %s not registered", n.Name)
	}

	// node output type must match children's input type
	for _, child := range n.Children {
		childRegistration := node.GetRegistry().GetNodeRegistration(child.Name)
		if childRegistration == nil {
			return fmt.Errorf("node type %s not registered", child.Name)
		}

		if r.Produces != childRegistration.Consumes {
			return fmt.Errorf("node type %s produces %s, but child %s consumes incompatible type %s", n.Name, r.Produces.String(),
				child.Name, childRegistration.Consumes.String())
		}
	}

	// check the error_handler config
	if n.ErrorHandler != nil {
		err := validateErrorHandlerConfig(n.ErrorHandler)
		if err != nil {
			return err
		}
	}

	// and recurse down the node hierarchy
	for _, child := range n.Children {
		err := validateNodeConfig(child)
		if err != nil {
			return err
		}
	}
	return nil
}

func validateErrorHandlerConfig(n *node.Config) error {
	if n.Children != nil {
		return fmt.Errorf("error_handler nodes may not have children for node %s", n.ID)
	}
	if n.ErrorHandler != nil {
		return fmt.Errorf("error_handler nodes may not have an error_handler of their own for node %s", n.ID)
	}
	r := node.GetRegistry().GetNodeRegistration(n.Name)
	if r == nil {
		return fmt.Errorf("error_handler node type %s not registered", n.Name)
	}

	// validate error handler data type; firebolt will always pass EventError to an error_handler
	if r.Consumes != reflect.TypeOf(&firebolt.EventError{}) {
		return fmt.Errorf("error_handler node type %s must consume EventError, actually consumes %s", n.Name, r.Consumes.String())
	}

	return nil
}

// setDefaults recursively visits each node, assigning defaults for missing config values
func setDefaults(c *Config) {

	if c.MetricsConfig == nil {
		enabled := c.MetricsPort > 0
		c.MetricsConfig = &MetricsConfig{
			Prefix:  c.MetricsPrefix,
			Port:    c.MetricsPort,
			Enabled: enabled,
		}
	}

	for _, n := range c.Nodes {
		assignNodeConfigDefaults(n)
	}
}

func assignNodeConfigDefaults(n *node.Config) {
	if n.ID == "" {
		n.ID = n.Name
	}
	if n.Workers == 0 {
		n.Workers = 1
	}
	if n.BufferSize == 0 {
		n.BufferSize = 1
	}

	if n.ErrorHandler != nil {
		assignNodeConfigDefaults(n.ErrorHandler)
	}

	for _, child := range n.Children {
		assignNodeConfigDefaults(child)
	}
}
