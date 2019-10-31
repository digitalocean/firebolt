package leader

import (
	"strings"
	"sync"
	"time"

	"github.com/Comcast/go-leaderelection"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

// Leader uses zookeeper to conduct leadership elections and expose whether or not the current node is a leader to applications.
type Leader struct {
	zk                      *zk.Conn
	zkPath                  string
	instanceID              string
	election                *leaderelection.Election
	leader                  bool
	initialElectionComplete bool
	leaderLock              sync.RWMutex
	stopped                 bool
}

// NewLeader creates a new leader election and starts the election process.   Only a single level zk path is supported at
// this time; node creation will fail if a multi-level node like '/foo/bar' is used.
func NewLeader(instanceID string, zkHosts string, zkPath string) *Leader {
	zookeeper, _ := connectZk(zkHosts)
	createPath(zookeeper, zkPath)

	l := &Leader{
		zk:         zookeeper,
		zkPath:     zkPath,
		instanceID: instanceID,
		leaderLock: sync.RWMutex{},
	}

	go l.startElection()

	return l
}

// IsLeader returns an indication of whether this instance is the leader in the cluster.
func (l *Leader) IsLeader() bool {
	l.leaderLock.RLock()
	defer l.leaderLock.RUnlock()

	return l.leader
}

// startElection continuously starts and monitors elections
func (l *Leader) startElection() {
	for !l.stopped {
		log.WithField("instance_id", l.instanceID).Info("leader: starting election")
		election, err := leaderelection.NewElection(l.zk, l.zkPath, l.instanceID)
		l.election = election
		if err != nil {
			log.WithError(err).Error("leader: failed to start new election, sleeping 5 seconds before retry...")
			time.Sleep(5 * time.Second)
		} else {
			go election.ElectLeader()
			l.monitorElection(election)
		}
	}
}

// stopElection resigns this instance from the current election
func (l *Leader) stopElection() {
	l.stopped = true
	l.leader = false
	l.initialElectionComplete = false
	l.election.Resign()
}

func (l *Leader) monitorElection(election *leaderelection.Election) {
	var status leaderelection.Status
	var ok bool

	for {
		select {
		case status, ok = <-election.Status():
			if !ok {
				log.Error("leader: channel was closed, restarting election")
				return
			}
			if status.Err != nil {
				log.WithError(status.Err).Errorf("leader: election failed for instance %s", l.instanceID)
				election.Resign()
				return
			}

			log.WithField("election_status", status).Info("leader: election returned status")
			l.initialElectionComplete = true
			l.updateLeadership(status)
		case <-time.After(time.Second * 120):
			if !l.initialElectionComplete {
				log.WithField("instance_id", l.instanceID).Error("leader: election timed out and will restart")
				election.Resign()
				return
			}
			log.Debug("leader: waiting for leadership status changes")
		}
	}
}

func (l *Leader) updateLeadership(status leaderelection.Status) {
	l.leaderLock.Lock()
	defer l.leaderLock.Unlock()

	if status.Role == leaderelection.Leader {
		log.WithField("instance_id", l.instanceID).Info("leader: instance elected leader")
		l.leader = true
	} else if status.Role == leaderelection.Follower {
		log.WithField("instance_id", l.instanceID).Info("leader: instance is a follower")
		l.leader = false
	}
}

// connectZk establishes a Zookeeper client connection.  The underlying client library does not block on creating the initial
// conection, so no error will be returned from zk.Connect if zookeeper is not available.
func connectZk(zkHosts string) (*zk.Conn, <-chan zk.Event) {
	zks := strings.Split(zkHosts, ",")

	conn, connEvtChan, err := zk.Connect(zks, 5*time.Second)
	if err != nil {
		log.WithError(err).Error("leader: failed to create Zookeeper connection")
		panic("leader: failed to create Zookeeper connection; cannot continue")
	}
	return conn, connEvtChan
}

// createPath ensures that the leader election path exists in Zookeeper.  Because leader election (and thus the application)
// cannot continue without this path, it blocks and retries forever.
func createPath(conn *zk.Conn, path string) {
	for {
		exists, _, err := conn.Exists(path)
		if err != nil {
			log.WithError(err).Errorf("leader: failed check zookeeper for election path %s, sleeping 5 seconds before retry...", path)
			time.Sleep(5 * time.Second)
			continue
		}

		if !exists {
			flags := int32(0)
			acl := zk.WorldACL(zk.PermAll)
			_, err := conn.Create(path, []byte("data"), flags, acl)
			if err != nil {
				log.WithError(err).Errorf("leader: failed create election path %s, sleeping 5 seconds before retry...", path)
				time.Sleep(5 * time.Second)
				continue
			}
		}

		return // success
	}
}
