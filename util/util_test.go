package util

import (
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetIPAddress(t *testing.T) {
	ip, err := GetIPAddress()
	assert.Nil(t, err)
	assert.NotNil(t, ip)
	//println(ip)
	ipAddr := net.ParseIP(ip)
	assert.NotNil(t, ipAddr)
}

func TestGetRandString(t *testing.T) {
	r1 := RandString(6)
	r2 := RandString(6)
	r3 := RandString(6)
	r4 := RandString(6)
	println(r1)
	println(r2)
	println(r3)
	println(r4)
	assert.NotEqual(t, r1, r2)
	assert.NotEqual(t, r1, r3)
	assert.NotEqual(t, r1, r4)
	assert.NotEqual(t, r2, r3)
	assert.NotEqual(t, r2, r4)
	assert.NotEqual(t, r3, r4)
}

func TestBuildInstanceId(t *testing.T) {
	id := BuildInstanceID()
	assert.NotNil(t, id)
	assert.True(t, strings.ContainsRune(id, '.'))
	assert.True(t, len(id) >= 14)
	println("id: " + id)
}
