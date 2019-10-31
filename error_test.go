package firebolt_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.internal.digitalocean.com/observability/firebolt"

	"github.com/stretchr/testify/assert"
)

func TestFBErrorMarshal(t *testing.T) {
	fberr := firebolt.NewFBError("mycode", "An Error Message")
	bytes, err := json.Marshal(fberr)
	assert.Nil(t, err)
	assert.Equal(t, "{\"code\":\"mycode\",\"message\":\"An Error Message\"}", string(bytes))
}

func TestFBErrorString(t *testing.T) {
	fberr := firebolt.NewFBError("mycode", "An Error Message")
	assert.Equal(t, "mycode: An Error Message", fberr.Error())
}

func TestEventErrorMarshalGoError(t *testing.T) {
	eventerr := firebolt.NewEventError(&firebolt.Event{Payload: "myevent"}, errors.New("error doing thing"))
	// override timestamp to make the test stable
	eventerr.Timestamp = time.Date(2001, 1, 1, 1, 0, 0, 0, time.UTC)
	bytes, err := json.Marshal(eventerr)
	assert.Nil(t, err)
	assert.Equal(t, "{\"timestamp\":\"2001-01-01T01:00:00Z\",\"event\":\"myevent\",\"error\":{\"code\":\"ERR_UNKNOWN\",\"message\":\"error doing thing\"}}", string(bytes))
}

func TestEventErrorMarshalFBError(t *testing.T) {
	eventerr := firebolt.NewEventError(&firebolt.Event{Payload: "myevent"}, firebolt.NewFBError("code", "error doing thing"))
	// override timestamp to make the test stable
	eventerr.Timestamp = time.Date(2001, 1, 1, 1, 0, 0, 0, time.UTC)
	bytes, err := json.Marshal(eventerr)
	assert.Nil(t, err)
	assert.Equal(t, "{\"timestamp\":\"2001-01-01T01:00:00Z\",\"event\":\"myevent\",\"error\":{\"code\":\"code\",\"message\":\"error doing thing\"}}", string(bytes))
}

func TestEventErrorMarshalWithInfo(t *testing.T) {
	i := indexErrorReport{Error: "1", LogMessage: "2", Index: "3", Program: "4"}
	eventerr := firebolt.NewEventError(&firebolt.Event{Payload: "myevent"}, firebolt.NewFBError("code", "error indexing", firebolt.WithInfo(i)))
	// override timestamp to make the test stable
	eventerr.Timestamp = time.Date(2001, 1, 1, 1, 0, 0, 0, time.UTC)
	bytes, err := json.Marshal(eventerr)
	assert.Nil(t, err)
	assert.Equal(t, "{\"timestamp\":\"2001-01-01T01:00:00Z\",\"event\":\"myevent\",\"error\":{\"code\":\"code\",\"message\":\"error indexing\",\"errorinfo\":{\"error\":\"1\",\"logmessage\":\"2\",\"esindex\":\"3\",\"program\":\"4\"}}}", string(bytes))
}

type indexErrorReport struct {
	Error      string `json:"error"`
	LogMessage string `json:"logmessage"`
	Index      string `json:"esindex"`
	Program    string `json:"program"`
}
