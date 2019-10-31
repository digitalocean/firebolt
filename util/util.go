package util

import (
	"fmt"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Condition is a function that returns a boolean indicating whether to stop waiting
type Condition func() bool

// ApplyLibrdkafkaConf overlays librdkafka config values from the provided firebolt config onto the consumer config; in the firebolt config
// these should be key prefixed with 'librdkafka.' and here we remove the prefix and pass 'em through
// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
func ApplyLibrdkafkaConf(config map[string]string, configMap *kafka.ConfigMap) error {
	for k, v := range config {
		if strings.HasPrefix(k, "librdkafka.") {
			err := configMap.SetKey(strings.TrimPrefix(k, "librdkafka."), v)
			if err != nil {
				// this error is never returned from the confluent SetKey() code, so logging it seems adequate
				fmt.Printf("failed to populate kafka config map with librdkafka values for key %s\n", k)
				return err
			}
		}
	}
	return nil
}

// GetIPAddress finds and returns the first non-loopback IP address formatted as a string.
func GetIPAddress() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addrs {
		// prevent returning a loopback addr
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", nil
}

var seeded = false
var randAlphabet = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandString creates and returns a pseudorandom string from randAlphabet of length n.
func RandString(n int) string {
	if !seeded {
		rand.Seed(time.Now().UTC().UnixNano())
		seeded = true
	}

	b := make([]rune, n)
	for i := range b {
		b[i] = randAlphabet[rand.Intn(len(randAlphabet))]
	}
	return string(b)
}

// BuildInstanceID creates a unique ID for this instance of the running application by appending the host IP address and
// a random string.
func BuildInstanceID() string {
	myIP, err := GetIPAddress()
	if err != nil {
		myIP = "addr-lookup-failed"
	}
	return myIP + "-" + RandString(6)
}
