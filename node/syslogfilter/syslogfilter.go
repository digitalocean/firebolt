package syslogfilter

import (
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/digitalocean/captainslog"
	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/fbcontext"
	"github.internal.digitalocean.com/observability/elauneind/v2/enum"
)

// SyslogFilter is a firebolt node for filtering messages from the SyslogParser
// Considering replacing `severity` `programName` and `hostName` fields with a single
// `field` field, and refactor that to an enum within logtale2 itself
type SyslogFilter struct {
	fbcontext.ContextAware
	hostName    string
	programName string
	severity    string
	matchExp    string

	matchType enum.MatchType
}

// TODO set up enum for matchTypes and make a switch clause so that we can avoid
// conditionally assigning all those empty strings
func (f *SyslogFilter) Setup(config map[string]string) error {

	err := f.checkConfig(config)
	if err != nil {
		return err
	}

	f.hostName = config["host"]
	f.matchExp = config["matchExp"]

	if config["programName"] != "" {
		f.programName = config["programName"]
	} else {
		f.programName = ""
	}

	f.matchType.FromString()

	log.Info("created syslog filter")
	return nil
}

func (f *SyslogFilter) Process(event *firebolt.Event) (*firebolt.Event, error) {

	// make sure the incoming `msg` is a valid SyslogMsg
	syslogMsg, ok := event.Payload.(captainslog.SyslogMsg)
	if !ok {
		return nil, errors.New("failed type assertion for captainslog.SyslogMsg")
	}

	Match(syslogMsg, f.matchExp)

}

func (f *SyslogFilter) checkConfig(config map[string]string) error {
	if config["host"] == "" {
		return fmt.Errorf("syslogfilter: missing or invalid value for config 'host': [%s]", config["host"])
	}

	if config["matchExp"] == "" {
		return fmt.Errorf("syslogfilter: missing or invalid value for config 'matchExp': [%s]", config["matchExp"])
	}

	if config["matchType"] == "" {
		return fmt.Errorf("syslogfilter: missing or invalid value for config 'matchType': [%s]", config["matchType"])
	}

	return nil
}

func Shutdown() error {

}

func Receive(msg fbcontext.Message) error {

}

func Match(log captainslog.SyslogMsg, matcher string) {
}
