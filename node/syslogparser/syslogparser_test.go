package syslogparser

import (
	"testing"
	"time"

	"github.internal.digitalocean.com/observability/firebolt"

	"github.com/digitalocean/captainslog"
	"github.com/stretchr/testify/assert"
)

func TestSyslogParser(t *testing.T) {
	s := &SyslogParser{}
	err := s.Setup(make(map[string]string))
	assert.Nil(t, err)

	result, err := s.Process(&firebolt.Event{
		Payload: []byte("<191>2006-01-02T15:04:05.999999-07:00 host.example.org test: @cee:{\"a\":\"b\"}\n"),
		Created: time.Now(),
	})
	assert.Nil(t, err)
	syslogMsg, ok := result.Payload.(captainslog.SyslogMsg)
	assert.True(t, ok)
	assert.Equal(t, "host.example.org", syslogMsg.Host)
	assert.Equal(t, "test", syslogMsg.Tag.Program)
	assert.True(t, syslogMsg.IsCee)

	err = s.Shutdown()
	assert.Nil(t, err)
}

func TestWrongType(t *testing.T) {
	s := &SyslogParser{}
	s.Setup(make(map[string]string))
	_, err := s.Process(&firebolt.Event{
		Payload: "should be a byte array not a string",
		Created: time.Now(),
	})
	assert.NotNil(t, err)
}

func TestInvalidSyslogMsg(t *testing.T) {
	s := &SyslogParser{}
	s.Setup(make(map[string]string))
	_, err := s.Process(&firebolt.Event{
		Payload: []byte("<no>not a valid syslog message in any way"),
		Created: time.Now(),
	})
	assert.NotNil(t, err)
}
