package executor_test

import (
	"testing"

	"time"

	"github.com/stretchr/testify/assert"

	"github.internal.digitalocean.com/observability/firebolt/executor"
	"github.internal.digitalocean.com/observability/firebolt/internal"
)

func TestNewInvalidConfig(t *testing.T) {
	ex, err := executor.New(executor.WithConfigFile("filethatdoesnotexist.yaml"))
	if err == nil {
		t.Error("expected an error, config file does not exist")
	}
	if ex != nil {
		t.Error("expected executor to be nil")
	}
}

func TestExecutor(t *testing.T) {
	internal.RegisterTestNodeTypes()

	ex, err := executor.New(executor.WithConfigFile("testconfig.yaml"))
	assert.Nil(t, err)
	go ex.Execute()

	// let the executor run, then shut it down
	time.Sleep(3 * time.Second)
	ex.Shutdown()

	assert.Equal(t, 10, len(internal.SuccessEvents))
	assert.Equal(t, 5, len(internal.FilteredEvents))
	assert.Equal(t, 3, len(internal.ErrorEvents))
	assert.Equal(t, 3, len(internal.ErrorHandlerEvents))
}

func TestBuildNodeHierarchy(t *testing.T) {
	internal.RegisterTestNodeTypes()

	ex, err := executor.New(executor.WithConfigFile("testconfig.yaml"))
	assert.Nil(t, err)

	rootNode := ex.FindNodeByID("filternode")
	assert.NotNil(t, rootNode)
	assert.Equal(t, "filternode", rootNode.Config.ID)
	assert.Equal(t, 1, len(rootNode.Children))

	errorNode := ex.FindNodeByID("errornode")
	assert.NotNil(t, errorNode)
	assert.Equal(t, "errornode", errorNode.Config.ID)
	assert.Equal(t, 1, len(errorNode.Children))
	assert.NotNil(t, errorNode.ErrorHandler)

	resultsNode := ex.FindNodeByID("resultsnode")
	assert.NotNil(t, resultsNode)
	assert.Equal(t, "resultsnode", resultsNode.Config.ID)
	assert.Equal(t, 0, len(resultsNode.Children))
}

func TestUncleanShutdown(t *testing.T) {
	// simplesource produces 18 records, 10 are successful and will reach 'slownode', which waits 5s per for a total of 50s delay
	internal.RegisterTestNodeTypes()

	ex, err := executor.New(executor.WithConfigFile("testconfig-uncleanShutdown.yaml"))
	if err != nil {
		t.Error(err)
	}
	go ex.Execute()

	// let the executor run, then shut it down
	time.Sleep(5 * time.Second)
	done := ex.Shutdown()

	// wait for a forced shutdown to happen
	<-done
	//TODO: how to assert that a dirty shutdown occurred? maybe add a method to executor that waits and returns an exit code?
}
