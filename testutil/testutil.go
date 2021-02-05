package testutil

import (
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/digitalocean/firebolt/util"
)

// AwaitCondition runs the passed 'cond' function every period until it returns true, returning an error if maxWait is
// exceeded.
func AwaitCondition(cond util.Condition, period time.Duration, maxWait time.Duration) error {
	started := time.Now()
	for !cond() {
		if time.Since(started) >= maxWait {
			return fmt.Errorf("awaitcondition failed after %s", maxWait)
		}
		time.Sleep(period)
	}
	return nil
}

// WaitForPort waits for the passed port number to start accepting connections.   This can be used in integration tests
// to ensure that infrastructure services are available before tests that depend on them start to run.   It's necessary
// because docker-compose returns when the configured containers are created, which doesn't ensure that the services they
// run are ready.d
func WaitForPort(t *testing.T, port int) error {
	err := AwaitCondition(func() bool {
		conn, err := net.Dial("tcp", "localhost:"+strconv.Itoa(port))
		if err != nil {
			conn.Close()
		}
		return err != nil
	}, 500*time.Millisecond, 60*time.Second)
	return err
}
