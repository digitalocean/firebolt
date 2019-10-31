package metrics_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.internal.digitalocean.com/observability/firebolt/metrics"

	"github.com/stretchr/testify/assert"
)

func TestStartAndShutdownServer(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	go metrics.StartServer(ctx, 56123)
	time.Sleep(1 * time.Second)

	// make sure it's listening
	conn, err := net.Dial("tcp", "127.0.0.1:56123")
	assert.NotNil(t, conn)
	assert.Nil(t, err)

	cancel()
	time.Sleep(1 * time.Second)

	// and make sure it shut down
	conn, err = net.Dial("tcp", "127.0.0.1:56123")
	assert.Nil(t, conn)
	assert.NotNil(t, err)
}
