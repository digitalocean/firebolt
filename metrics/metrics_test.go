package metrics_test

import (
	"testing"

	"github.com/digitalocean/firebolt/metrics"

	"github.com/stretchr/testify/assert"
)

// this test must always be first, before Init() has been called
func TestFailToInit(t *testing.T) {
	assert.Panics(t, func() { metrics.Get() }, "panic was expected, metrics not initialized")
	assert.Panics(t, func() { metrics.Source() }, "panic was expected, metrics not initialized")
	assert.Panics(t, func() { metrics.Node() }, "panic was expected, metrics not initialized")
}

func TestMetricsSingleton(t *testing.T) {
	metrics.Init("testy")
	m1 := metrics.Get()
	m2 := metrics.Get()
	assert.Equal(t, m1, m2)
}

func TestNodeMetrics(t *testing.T) {
	metrics.Init("node")
	n := metrics.Node()
	assert.NotNil(t, n)
}

func TestSourceMetrics(t *testing.T) {
	metrics.Init("source")
	s := metrics.Source()
	assert.NotNil(t, s)
}
