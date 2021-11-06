//go:build integration
// +build integration

package leader

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSingleNodeLeaderElection(t *testing.T) {
	l := NewLeader("myinstance", "127.0.0.1:2181", "/an-election")
	time.Sleep(1000 * time.Millisecond)
	assert.Equal(t, true, l.IsLeader())
	l.stopElection()
}

func TestAnotherSingleNodeLeaderElection(t *testing.T) {
	l := NewLeader("myinstance", "127.0.0.1:2181", "/an-election")
	time.Sleep(1000 * time.Millisecond)
	assert.Equal(t, true, l.IsLeader())
	l.stopElection()
}

// nolint: gocyclo
func TestMultiNodeLeadershipChange(t *testing.T) {
	l1 := NewLeader("myinstance1", "127.0.0.1:2181", "/an-election")
	time.Sleep(1000 * time.Millisecond)
	assert.Equal(t, true, l1.IsLeader())

	// second node should be a follower
	l2 := NewLeader("myinstance2", "127.0.0.1:2181", "/an-election")
	time.Sleep(1000 * time.Millisecond)
	assert.Equal(t, false, l2.IsLeader())

	// more nodes
	l3 := NewLeader("myinstance3", "127.0.0.1:2181", "/an-election")
	l4 := NewLeader("myinstance4", "127.0.0.1:2181", "/an-election")
	l5 := NewLeader("myinstance5", "127.0.0.1:2181", "/an-election")
	l6 := NewLeader("myinstance6", "127.0.0.1:2181", "/an-election")
	l7 := NewLeader("myinstance7", "127.0.0.1:2181", "/an-election")
	time.Sleep(1000 * time.Millisecond)
	assert.Equal(t, false, l3.IsLeader() || l4.IsLeader() || l5.IsLeader() || l6.IsLeader() || l7.IsLeader())

	println("resigning l1")
	l1.stopElection()
	time.Sleep(1000 * time.Millisecond)
	assert.Equal(t, true, l2.IsLeader() || l3.IsLeader() || l4.IsLeader() || l5.IsLeader() || l6.IsLeader() || l7.IsLeader())

	// resign a few more
	l2.stopElection()
	l3.stopElection()
	l4.stopElection()
	time.Sleep(1000 * time.Millisecond)
	assert.Equal(t, true, l5.IsLeader() || l6.IsLeader() || l7.IsLeader())

	// and more, now only l5 remains
	l6.stopElection()
	l7.stopElection()
	time.Sleep(1000 * time.Millisecond)
	assert.Equal(t, true, l5.IsLeader())
}

// This test is very slow by definition (about 17m); keep it commented out normally to avoid long test runs.
//
//func TestLongTermLeadershipChange(t *testing.T) {
//
//	// lLeadership should not change over longer periods without membership changes
//	l1 := NewLeader("myinstance1", "127.0.0.1:2181", "/long-election")
//	time.Sleep(1000 * time.Millisecond)
//	assert.Equal(t, true, l1.IsLeader())
//
//	// second node should be a follower
//	l2 := NewLeader("myinstance2", "127.0.0.1:2181", "/long-election")
//	time.Sleep(1000 * time.Millisecond)
//	assert.Equal(t, false, l2.IsLeader())
//
//	// over 1000s the leader should not change
//	for i := 0; i < 100; i++ {
//		assert.Equal(t, true, l1.IsLeader())
//		assert.Equal(t, false, l2.IsLeader())
//		time.Sleep(10 * time.Second)
//	}
//
//	// after all that time it should still elect a new leader
//	l1.stopElection()
//	time.Sleep(1000 * time.Millisecond)
//	assert.Equal(t, false, l1.IsLeader())
//	assert.Equal(t, true, l2.IsLeader())
//
//	l2.stopElection()
//}
