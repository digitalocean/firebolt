package executor

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/digitalocean/firebolt"

	"github.com/digitalocean/firebolt/util"

	"github.com/digitalocean/firebolt/leader"

	"github.com/digitalocean/firebolt/config"
	"github.com/digitalocean/firebolt/fbcontext"
	"github.com/digitalocean/firebolt/message"
	"github.com/digitalocean/firebolt/metrics"
	"github.com/digitalocean/firebolt/node"

	log "github.com/sirupsen/logrus"
)

// Executor is the main processing engine in firebolt.   It starts your data `source`, and pipes the data from that
// source through the configured processing `nodes`.   The executor is responsible for _managing_ the source and the
// nodes, the nodes are responsible for _performing_ the application's data processing stages.
type Executor struct {
	config          config.Config
	rootNodes       []*node.Context
	wg              sync.WaitGroup
	sigCh           chan os.Signal
	source          node.Source
	sourceCh        chan firebolt.Event
	messageReceiver message.Receiver
	fbContext       fbcontext.FBContext
	instanceID      string
	leader          *leader.Leader
}

// Opt is an option type that can be passed to New to tell it how to set up the returned *Executor
type Opt func(*Executor) (*Executor, error)

// WithConfig is an Opt that sets up an Executor with c
func WithConfig(c config.Config) Opt {

	// build the hierarchy of nodes below each root node defined in the config
	var roots []*node.Context
	for _, rootNodeConfig := range c.Nodes {
		nodeContext := node.InitNodeContextHierarchy(rootNodeConfig)
		if nodeContext != nil {
			roots = append(roots, nodeContext)
		}
	}

	// prometheus metrics server
	metrics.Init(c.MetricsPrefix)
	if c.MetricsPort != 0 {
		go func() {
			err := metrics.StartServer(context.Background(), c.MetricsPort)
			if err != nil {
				log.WithError(err).Error("executor: failed to start metrics http server")
			}
		}()
	}

	return func(e *Executor) (*Executor, error) {
		newE := &Executor{
			config:     c,
			rootNodes:  roots,
			sigCh:      make(chan os.Signal, 1),
			sourceCh:   make(chan firebolt.Event),
			source:     e.source,
			instanceID: util.BuildInstanceID(),
		}

		// signal handling
		signal.Notify(newE.sigCh, syscall.SIGINT, syscall.SIGTERM)

		newE.fbContext = fbcontext.NewFBContext(func() string { return newE.instanceID })

		// zookeeper-based leader election; if nil then leader election is disabled
		if c.Zookeeper != "" {
			newE.leader = leader.NewLeader(newE.instanceID, c.Zookeeper, c.ZkElectionPath)
			newE.fbContext.ConfigureLeader(func() bool { return newE.leader.IsLeader() })
		}

		// initialize messaging; this must be *before* node setup to ensure that the fbcontext is ready to send/recv messages
		err := newE.InitMessaging(c)
		if err != nil {
			return nil, err
		}

		newE.prepareSource()

		// invoke Setup for all nodes
		for _, rootNode := range newE.rootNodes {
			newE.setupNodes(rootNode)
		}

		// start messages flowing; this must be *after* node setup when all nodes are ready
		newE.StartMessaging()

		return newE, nil
	}
}

// WithConfigFile is an option type that parses a config file and sets up an Executor with the file.
func WithConfigFile(path string) Opt {
	return func(e *Executor) (*Executor, error) {
		// register built-in nodetypes
		RegisterBuiltinSourceTypes()
		RegisterBuiltinNodeTypes()

		c, err := config.Read(path)
		if err != nil {
			log.WithField("config_file", path).WithError(err).Error("executor: failed to read config file")
			return nil, err
		}
		return WithConfig(*c)(e)
	}
}

// New instantiates the firebolt executor based on the passed functional options.
func New(opts ...Opt) (*Executor, error) {
	var (
		e   = &Executor{}
		err error
	)
	for _, opt := range opts {
		e, err = opt(e)
		if err != nil {
			return nil, err
		}
	}
	return e, nil
}

// Execute starts processing based on your application's source and node configuration.   This call will block forever
// while the application runs, unless an unrecoverable error is encountered or the source exits.
func (e *Executor) Execute() {
	// start the source, which will now start producing events onto sourceCh
	e.superviseSource()

	// start node worker goroutines: spin up the configured number of goroutines for each node to:
	//  * read from its input channel
	//  * invoke its processor
	//  * put its output on each child node's input channel
	for _, curNode := range e.rootNodes {
		e.startWorkers(curNode)
	}

	// use the current goroutine to read from the sourceCh and write to all root node input channels
	done := false
	for !done {
		select {
		case sig := <-e.sigCh:
			log.WithField("signal", sig).Info("executor: caught signal, starting shutdown")
			e.Shutdown()
			// source will stop producing records onto sourceCh, we keep reading from the closed channel to process buffered data
		case sourceEvent, ok := <-e.sourceCh:
			if !ok {
				// channel has been closed
				log.Info("executor: source channel has been closed, main channel reader loop can now exit")
				done = true
				continue
			}

			log.WithField("raw_event", sourceEvent).Debug("executor: received event from source")

			// In this trivial implementation, used below in runNode as well, one branch of the DAG that is slow or inadequately
			// buffered will block the send, holding up all children of the same parent.   The buffers above the blockage will
			// then also start to fill, so buffer sizes should be chosen to prevent this case - which is expected - from causing
			// an OOM.
			//
			// It would be possible to use a ringbuffer so that slow nodes lose messages and fast nodes are never blocked, and
			// we should offer this option in the future. For now we want the backpressure to flow back to the Kafka consumer, which
			// will fall behind.   We will create alerts on consumer lag so that we know when there is a throughput issue
			// somewhere in the system and can address it.   Metrics on buffer size are critical to finding the source of the
			// blockage.
			//
			for _, rootNode := range e.rootNodes {
				rootNode.Ch <- sourceEvent
				metrics.Node().BufferedEvents.WithLabelValues(rootNode.Config.ID).Set(float64(len(rootNode.Ch)))
			}
		}
	}

	// we close the root node channels here; as each node hits the !ok closed event it will close it's own children's
	// channels, so that they close in a 'cascade' down the hierarchy as the channels empty
	log.Info("executor: main source channel reader loop exited, closing root node channels")
	for _, rootNode := range e.rootNodes {
		log.WithField("node_id", rootNode.Config.ID).Info("executor: closing node channel")
		close(rootNode.Ch)
	}

	if waitTimeout(&e.wg, 10*time.Second) {
		// there was a timeout, resort to forcible shutdown
		for _, rootNode := range e.rootNodes {
			e.stopWorkers(rootNode)
		}
	}

	// only shut down message sender *after* all nodes have stopped so that nodes can send messages until they are done
	message.ShutdownKafkaSender()
}

// waitTimeout waits for the waitgroup for the specified max timeout
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case <-c:
		log.Info("executor: waitgroup done, normal exit")
		return false
	case <-time.After(timeout):
		log.Warn("executor: waitgroup timeout, possible data loss")
		return true
	}
}

func (e *Executor) prepareSource() {
	// create a new instance of the source
	log.WithField("source_name", e.config.Source.Name).Info("executor: initializing source")
	e.source = node.GetRegistry().InstantiateSource(e.config.Source.Name)

	e.source.Init(e.config.Source.ID, e.fbContext)

	// invoke Setup, which must not start producing records onto sourceCh *yet*
	err := e.source.Setup(e.config.Source.Params, e.sourceCh)
	if err != nil {
		log.WithError(err).Error("executor: failed to initialize source")
		os.Exit(1)
	}
}

// superviseSource starts the configured source and acts as a supervisor, restarting the source whenever it fails
// with an error.   The source returning nil indicates that the source is 'done' and should not be restarted; system
// shutdown will begin.
func (e *Executor) superviseSource() {
	// start the source and act as a supervisor to keep it running
	go func() {
		initialRun := true
		defer close(e.sourceCh)
		for {
			// on the initial run, the source has been prepared during construction of the executor to make it available for messaging
			// at the same time as the nodes become available
			if initialRun {
				initialRun = false
			} else {
				e.prepareSource()
			}

			err := e.source.Start()
			if err == nil {
				break // source requested shutdown
			}
			log.WithError(err).Error("executor: source failed, retrying in 10s")
			time.Sleep(10 * time.Second)
		}
		log.Info("executor: source stopped, closing source channel")
	}()
}

// Shutdown stops the source and allows the nodes to finish processing before the Execute() call returns
func (e *Executor) Shutdown() (done chan struct{}) {
	log.Info("executor: starting executor shutdown")
	if e.source != nil {
		err := e.source.Shutdown()
		if err != nil {
			log.WithError(err).Error("executor: failed to shutdown source")
		}
	}

	if e.messageReceiver != nil {
		e.messageReceiver.Shutdown()
	}

	// shutting down the source will cause a well-behaved source to return from its Start() method, and then
	// sourceCh will be closed, which will cause Execute() to return

	done = make(chan struct{})
	go func() {
		//e.wg.Wait()
		done <- struct{}{}
	}()
	return done
}

func (e *Executor) setupNodes(node *node.Context) {
	node.NodeProcessor.Init(node.Config.ID, e.fbContext)
	err := node.NodeProcessor.Setup(node.Config.Params)
	if err != nil {
		log.WithError(err).WithField("node_id", node.Config.ID).Error("executor: failed to setup node")
		os.Exit(1)
	}
	log.WithField("node_id", node.Config.ID).Info("executor: setup node")

	// error handler
	if node.ErrorHandler != nil {
		e.setupNodes(node.ErrorHandler)
	}

	// recurse for the children
	for _, child := range node.Children {
		e.setupNodes(child)
	}
}

func (e *Executor) startWorkers(node *node.Context) {
	// current node
	node.WaitGroup.Add(node.Config.Workers)
	for i := 0; i < node.Config.Workers; i++ {
		e.wg.Add(1)
		log.WithField("worker-index", i).WithField("node_id", node.Config.ID).Debug("executor: starting goroutine for node")
		go e.runNode(node)
	}

	// error handler
	if node.ErrorHandler != nil {
		e.startWorkers(node.ErrorHandler)
	}

	// recurse for the children
	for _, child := range node.Children {
		e.startWorkers(child)
	}
}

func (e *Executor) stopWorkers(node *node.Context) {
	for i := 0; i < node.Config.Workers; i++ {
		node.StopCh <- true
	}

	log.WithField("node_id", node.Config.ID).Info("executor: shutting down node")
	err := node.NodeProcessor.Shutdown()
	if err != nil {
		log.WithError(err).WithField("node_id", node.Config.ID).Error("executor: error during node shutdown")
	}

	for _, child := range node.Children {
		e.stopWorkers(child)
	}
}

// runNode is invoked in a new goroutine for each worker; it reads events from the node's input channel and invokes the
// node's processing.   It also handles both clean and forcible shutdown and resource cleanup.
func (e *Executor) runNode(node *node.Context) {
	nodeID := node.Config.ID
	log.WithField("node_id", nodeID).Debug("executor: running node")
	defer e.wg.Done()

	for {
		select {
		case <-node.StopCh:
			log.WithField("node_id", nodeID).Info("executor: forcibly stopping node")
			return
		case event, ok := <-node.Ch:
			if !ok {
				node.WaitGroup.Done()
				log.WithField("node_id", nodeID).Info("executor: node channel closed, waiting for all workers for this node to complete")
				node.WaitGroup.Wait()

				// only after the last worker has finished should we close child channels, and we should only close them once
				node.ShutdownOnce.Do(func() {
					for _, child := range node.Children {
						log.WithField("node_id", child.Config.ID).Info("executor: closing channel")
						close(child.Ch)
					}
					if node.ErrorHandler != nil {
						close(node.ErrorHandler.Ch)
					}
				})
				return
			}
			node.ProcessEvent(&event)
		}
	}
}

// GetSource returns the source configured for this firebolt application.
func (e *Executor) GetSource() *node.Source {
	return &e.source
}

// SendMessage broadcasts the passed message to all nodes that are subscribed to the MessageType.
func (e *Executor) SendMessage(msg message.Message) error {
	return message.GetSender().Send(msg)
}

// FindNodeByID finds the node with the specified ID, or returns 'nil' if no node is configured with that ID.
func (e *Executor) FindNodeByID(id string) *node.Context {
	for _, rootNode := range e.rootNodes {
		match := findMatchingNode(rootNode, id)
		if match != nil {
			return match
		}
	}

	return nil
}

func findMatchingNode(node *node.Context, id string) *node.Context {
	if node.Config.ID == id {
		return node
	}

	for _, child := range node.Children {
		match := findMatchingNode(child, id)
		if match != nil {
			return match
		}
	}

	return nil
}
