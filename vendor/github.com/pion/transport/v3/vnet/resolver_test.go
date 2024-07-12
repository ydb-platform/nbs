// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package vnet

import (
    "net"
    "testing"

    "github.com/pion/logging"
    "github.com/stretchr/testify/assert"
)

func TestResolver(t *testing.T) {
    loggerFactory := logging.NewDefaultLoggerFactory()
    log := loggerFactory.NewLogger("test")

    t.Run("Standalone", func(t *testing.T) {
        r := newResolver(&resolverConfig{
            LoggerFactory: loggerFactory,
        })

        // should have localhost by default
        name := "localhost"
        ipAddr := "127.0.0.1"
        ip := net.ParseIP(ipAddr)

        resolved, err := r.lookUp(name)
        assert.NoError(t, err, "should succeed")
        assert.True(t, resolved.Equal(ip), "should match")

        name = "abc.com"
        ipAddr = demoIP
        ip = net.ParseIP(ipAddr)
        log.Debugf("adding %s %s", name, ipAddr)

        err = r.addHost(name, ipAddr)
        assert.NoError(t, err, "should succeed")

        resolved, err = r.lookUp(name)
        assert.NoError(t, err, "should succeed")
        assert.True(t, resolved.Equal(ip), "should match")
    })

    t.Run("Cascaded", func(t *testing.T) {
        r0 := newResolver(&resolverConfig{
            LoggerFactory: loggerFactory,
        })
        name0 := "abc.com"
        ipAddr0 := demoIP
        ip0 := net.ParseIP(ipAddr0)
        err := r0.addHost(name0, ipAddr0)
        assert.NoError(t, err, "should succeed")

        r1 := newResolver(&resolverConfig{
            LoggerFactory: loggerFactory,
        })
        name1 := "myserver.local"
        ipAddr1 := "10.1.2.5"
        ip1 := net.ParseIP(ipAddr1)
        err = r1.addHost(name1, ipAddr1)
        assert.NoError(t, err, "should succeed")
        r1.setParent(r0)

        resolved, err := r1.lookUp(name0)
        assert.NoError(t, err, "should succeed")
        assert.True(t, resolved.Equal(ip0), "should match")

        resolved, err = r1.lookUp(name1)
        assert.NoError(t, err, "should succeed")
        assert.True(t, resolved.Equal(ip1), "should match")

        // should fail if the name does not exist
        _, err = r1.lookUp("bad.com")
        assert.NotNil(t, err, "should fail")
    })
}
