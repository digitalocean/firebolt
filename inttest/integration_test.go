// +build integration

package inttest

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.internal.digitalocean.com/observability/firebolt"
	"github.internal.digitalocean.com/observability/firebolt/executor"
	"github.internal.digitalocean.com/observability/firebolt/internal"
	"github.internal.digitalocean.com/observability/firebolt/node/elasticsearch"
	"github.internal.digitalocean.com/observability/firebolt/node/kafkaconsumer"
	"github.internal.digitalocean.com/observability/firebolt/node/kafkaproducer"
	"github.internal.digitalocean.com/observability/firebolt/util"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const testTopic = "firebolt-inttest"
const recordCount = 100

func TestEndToEnd(t *testing.T) {
	internal.RegisterTestNodeTypes()

	ex, err := executor.New(executor.WithConfigFile("testconfig.yaml"))
	assert.Nil(t, err)

	util.WaitForPort(t, 9200)                  // wait for infra (kafka, elasticsearch) to be available - we wait for es since it takes longer to startup
	err = elasticsearch.CreateIndex("inttest") // create the ES target index
	assert.NoError(t, err)
	go ex.Execute()
	produceTestData(recordCount)
	time.Sleep(10 * time.Second)
	ex.Shutdown()
	log.Info("endtoend: executor shutdown complete")

	// run a kafkaconsumer to consume 'firebolt-inttest-out'
	maxToConsume := 100000
	successCh := make(chan firebolt.Event, maxToConsume)
	config := make(map[string]string)
	config["brokers"] = "localhost:9092"
	config["consumergroup"] = "inttest-success"
	config["topic"] = "firebolt-inttest-out"
	config["buffersize"] = strconv.Itoa(maxToConsume)
	config["maxpartitionlag"] = strconv.Itoa(maxToConsume)
	successConsumer := &kafkaconsumer.KafkaConsumer{}
	err = successConsumer.Setup(config, successCh)
	assert.Nil(t, err)
	go successConsumer.Start()
	err = waitForEvents(t, 90, successCh)
	assert.NoError(t, err)
	go successConsumer.Shutdown()
	assert.Equal(t, 90, len(successCh))
	close(successCh)

	// run a kafkaconsumer to consume 'firebolt-inttest-err'
	errCh := make(chan firebolt.Event, maxToConsume)
	config = make(map[string]string)
	config["brokers"] = "localhost:9092"
	config["consumergroup"] = "inttest-error"
	config["topic"] = "firebolt-inttest-err"
	config["buffersize"] = strconv.Itoa(maxToConsume)
	config["maxpartitionlag"] = strconv.Itoa(maxToConsume)
	errorConsumer := &kafkaconsumer.KafkaConsumer{}
	err = errorConsumer.Setup(config, errCh)
	assert.Nil(t, err)
	go errorConsumer.Start()
	err = waitForEvents(t, 4, errCh)
	assert.NoError(t, err)
	go errorConsumer.Shutdown()
	assert.Equal(t, 4, len(errCh))
	close(errCh)

	// run a kafkaconsumer to consumer 'firebolt-inttest-out-async'
	asyncSuccessCh := make(chan firebolt.Event, maxToConsume)
	config = make(map[string]string)
	config["brokers"] = "localhost:9092"
	config["consumergroup"] = "inttest-success-async"
	config["topic"] = "firebolt-inttest-out-async"
	config["buffersize"] = strconv.Itoa(maxToConsume)
	config["maxpartitionlag"] = strconv.Itoa(maxToConsume)
	asyncSuccessConsumer := &kafkaconsumer.KafkaConsumer{}
	err = asyncSuccessConsumer.Setup(config, asyncSuccessCh)
	assert.Nil(t, err)
	go asyncSuccessConsumer.Start()
	err = waitForEvents(t, 90, asyncSuccessCh)
	assert.NoError(t, err)
	go asyncSuccessConsumer.Shutdown()
	assert.Equal(t, 90, len(asyncSuccessCh))
	close(asyncSuccessCh)

	// filternode
	assert.Equal(t, 6, len(internal.FilteredEvents))

	// errornode: used twice in this config, so it gets double the events
	assert.Equal(t, 8, len(internal.ErrorEvents))

	// asyncfilternode
	assert.Equal(t, 94, len(internal.AsyncPassedEvents)) // success and errors
	assert.Equal(t, 6, len(internal.AsyncFilteredEvents))

	// elasticsearch
	hits, err := elasticsearch.SearchAllDocuments("inttest")
	assert.NoError(t, err)
	assert.Equal(t, int64(94), hits.TotalHits.Value) // all 94 events that were not filtered should be indexed
}

func waitForEvents(t *testing.T, expected int, ch chan firebolt.Event) error {
	err := util.AwaitCondition(func() bool {
		return len(ch) >= expected
	}, 250*time.Millisecond, 60*time.Second)
	fmt.Printf("failed, only consumed %d messages\n", len(ch))
	assert.NoError(t, err, "consumed %d messages", len(ch))
	return err
}

func TestRecovery(t *testing.T) {
	internal.RegisterTestNodeTypes()

	// clear the FilteredEvents channel so that data from previous tests don't interfere with this test
	for len(internal.FilteredEvents) > 0 {
		<-internal.FilteredEvents
	}

	// to test recovery, produce records before startup
	produceTestData(4000) // approx 1k per partition after random partition assignment

	ex, err := executor.New(executor.WithConfigFile("testconfig-withRecovery.yaml"))
	assert.Nil(t, err)
	go ex.Execute()
	time.Sleep(30 * time.Second) // give the executor time to process

	ex.Shutdown()
	log.Info("endtoend: executor shutdown complete")

	// we expect to get 400 records (100 maxpartitionlag * 4 partitions) from kafkaconsumer
	// and to recover 2000 (500 parallelrecoverymaxrecords * 4 partitions) via recoveryconsumer
	// for a total of 2400 * 90% successful = 2160 success expected
	// 2400 * 3% errors = 72 errors expected
	// 2400 * 7% filtered = 168 errors filtered
	// but there can be some skew within the *individual* numbers based on the offset boundaries recovered, so our assertions
	// below can only enforce that the *total* must be 2400

	// use metrics to audit per-partition recovery counts
	source := *ex.GetSource()
	kc := source.(*kafkaconsumer.KafkaConsumer)
	countervec := kc.GetMetrics().RecoveryEvents
	counterval, err := util.GetCounterVecValue(countervec, "0")
	assert.Nil(t, err)
	assert.Equal(t, 500.0, counterval, "partition 0 expected to recovery 500")
	fmt.Printf("partition %d recovered %f\n", 0, counterval)
	counterval, err = util.GetCounterVecValue(countervec, "1")
	assert.Nil(t, err)
	assert.Equal(t, 500.0, counterval, "partition 1 expected to recovery 500")
	fmt.Printf("partition %d recovered %f\n", 1, counterval)
	counterval, err = util.GetCounterVecValue(countervec, "2")
	assert.Nil(t, err)
	assert.Equal(t, 500.0, counterval, "partition 2 expected to recovery 500")
	fmt.Printf("partition %d recovered %f\n", 2, counterval)
	counterval, err = util.GetCounterVecValue(countervec, "3")
	assert.Nil(t, err)
	assert.Equal(t, 500.0, counterval, "partition 3 expected to recovery 500")
	fmt.Printf("partition %d recovered %f\n", 3, counterval)

	// run a kafkaconsumer to consume 'firebolt-inttest-out'
	maxToConsume := 100000
	successCh := make(chan firebolt.Event, maxToConsume)
	config := make(map[string]string)
	config["brokers"] = "localhost:9092"
	config["consumergroup"] = "inttest-success"
	config["topic"] = "firebolt-inttest-out"
	config["buffersize"] = strconv.Itoa(maxToConsume)
	config["maxpartitionlag"] = strconv.Itoa(maxToConsume)
	successConsumer := &kafkaconsumer.KafkaConsumer{}
	err = successConsumer.Setup(config, successCh)
	assert.Nil(t, err)
	go successConsumer.Start()
	time.Sleep(10 * time.Second)
	go successConsumer.Shutdown()
	close(successCh)

	// run a kafkaconsumer to consume 'firebolt-inttest-err'
	errCh := make(chan firebolt.Event, maxToConsume)
	config = make(map[string]string)
	config["brokers"] = "localhost:9092"
	config["consumergroup"] = "inttest-error"
	config["topic"] = "firebolt-inttest-err"
	config["buffersize"] = strconv.Itoa(maxToConsume)
	config["maxpartitionlag"] = strconv.Itoa(maxToConsume)
	errorConsumer := &kafkaconsumer.KafkaConsumer{}
	err = errorConsumer.Setup(config, errCh)
	assert.Nil(t, err)
	go errorConsumer.Start()
	err = waitForEvents(t, 2100, successCh)
	time.Sleep(5 * time.Second) // we still sleep a bit to make sure we got 'em all; the 2100 on the line above isn't exact
	assert.NoError(t, err)
	go errorConsumer.Shutdown()
	close(errCh)

	fmt.Printf("inttest: recovery founc %d successful, %d errors, %d filtered", len(successCh), len(errCh), len(internal.FilteredEvents))
	totalEvents := len(successCh) + len(errCh) + len(internal.FilteredEvents)
	assert.Equal(t, 2400, totalEvents)
}

func produceTestData(count int) {
	// might as well use the producer we already have right
	kp := &kafkaproducer.KafkaProducer{}
	config := make(map[string]string)
	config["brokers"] = "localhost:9092"
	config["topic"] = testTopic
	err := kp.Setup(config)
	if err != nil {
		log.WithError(err).Info("failed to setup producer")
	}

	for i := 0; i < count; i++ {
		if i%10 == 0 {
			if i%30 == 0 {
				// 3% of records will be errors: this one gets zero first so (0, 30, 60, 90) == 4
				kp.Process(&firebolt.Event{
					Payload: []byte("error time"),
				})
			} else {
				// 7% of records will be filtered (10, 20, 40, 50, 70, 80) == 6
				kp.Process(&firebolt.Event{
					Payload: []byte("filter me"),
				})
			}
		} else {
			// 90% of records will be successful () == 90
			kp.Process(&firebolt.Event{
				Payload: []byte("<191>2006-01-02T15:04:05.999999-07:00 host.example.org test: @cee:{\"a\":\"b\"}\n"),
			})
		}
	}

	err = kp.Shutdown()
	if err != nil {
		log.WithError(err).Info("failed to shutdown producer")
	}
}
