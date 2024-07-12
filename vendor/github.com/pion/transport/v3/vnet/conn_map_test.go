// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package vnet

import (
    "net"
    "testing"

    "github.com/stretchr/testify/assert"
)

type myConnObserver struct{}

func (obs *myConnObserver) write(Chunk) error {
    return nil
}

func (obs *myConnObserver) onClosed(net.Addr) {
}

func (obs *myConnObserver) determineSourceIP(net.IP, net.IP) net.IP {
    return net.IP{}
}

func TestUDPConnMap(t *testing.T) {
    // log := logging.NewDefaultLoggerFactory().NewLogger("test")

    t.Run("insert an UDPConn and remove it", func(t *testing.T) {
        connMap := newUDPConnMap()

        obs := &myConnObserver{}
        connIn, err := newUDPConn(&net.UDPAddr{
            IP:   net.ParseIP("127.0.0.1"),
            Port: 1234,
        }, nil, obs)
        assert.NoError(t, err, "should succeed")

        err = connMap.insert(connIn)
        assert.NoError(t, err, "should succeed")

        connOut, ok := connMap.find(connIn.LocalAddr())
        assert.True(t, ok, "should succeed")
        assert.Equal(t, connIn, connOut, "should match")
        assert.Equal(t, 1, len(connMap.portMap), "should match")

        err = connMap.delete(connIn.LocalAddr())
        assert.NoError(t, err, "should succeed")
        assert.Equal(t, 0, len(connMap.portMap), "should match")

        err = connMap.delete(connIn.LocalAddr())
        assert.Error(t, err, "should fail")
    })

    t.Run("insert an UDPConn on 0.0.0.0 and remove it", func(t *testing.T) {
        connMap := newUDPConnMap()

        obs := &myConnObserver{}
        connIn, err := newUDPConn(&net.UDPAddr{
            IP:   net.ParseIP("0.0.0.0"),
            Port: 1234,
        }, nil, obs)
        assert.NoError(t, err, "should succeed")

        err = connMap.insert(connIn)
        assert.NoError(t, err, "should succeed")

        connOut, ok := connMap.find(connIn.LocalAddr())
        assert.True(t, ok, "should succeed")
        assert.Equal(t, connIn, connOut, "should match")
        assert.Equal(t, 1, len(connMap.portMap), "should match")

        err = connMap.delete(connIn.LocalAddr())
        assert.NoError(t, err, "should succeed")

        err = connMap.delete(connIn.LocalAddr())
        assert.Error(t, err, "should fail")
    })

    t.Run("find UDPConn on 0.0.0.0 by specified IP", func(t *testing.T) {
        connMap := newUDPConnMap()

        obs := &myConnObserver{}
        connIn, err := newUDPConn(&net.UDPAddr{
            IP:   net.ParseIP("0.0.0.0"),
            Port: 1234,
        }, nil, obs)
        assert.NoError(t, err, "should succeed")

        err = connMap.insert(connIn)
        assert.NoError(t, err, "should succeed")

        connOut, ok := connMap.find(&net.UDPAddr{
            IP:   net.ParseIP("192.168.0.1"),
            Port: 1234,
        })
        assert.True(t, ok, "should succeed")
        assert.Equal(t, connIn, connOut, "should match")
        assert.Equal(t, 1, len(connMap.portMap), "should match")
    })

    t.Run("insert many IPs with the same port", func(t *testing.T) {
        connMap := newUDPConnMap()

        obs := &myConnObserver{}
        connIn1, err := newUDPConn(&net.UDPAddr{
            IP:   net.ParseIP("10.1.2.1"),
            Port: 5678,
        }, nil, obs)
        assert.NoError(t, err, "should succeed")
        err = connMap.insert(connIn1)
        assert.NoError(t, err, "should succeed")

        connIn2, err := newUDPConn(&net.UDPAddr{
            IP:   net.ParseIP("10.1.2.2"),
            Port: 5678,
        }, nil, obs)
        assert.NoError(t, err, "should succeed")
        err = connMap.insert(connIn2)
        assert.NoError(t, err, "should succeed")

        connOut1, ok := connMap.find(&net.UDPAddr{
            IP:   net.ParseIP("10.1.2.1"),
            Port: 5678,
        })
        assert.True(t, ok, "should succeed")
        assert.Equal(t, connIn1, connOut1, "should match")

        connOut2, ok := connMap.find(&net.UDPAddr{
            IP:   net.ParseIP("10.1.2.2"),
            Port: 5678,
        })
        assert.True(t, ok, "should succeed")
        assert.Equal(t, connIn2, connOut2, "should match")

        assert.Equal(t, 1, len(connMap.portMap), "should match")
    })

    t.Run("already in-use when inserting 0.0.0.0", func(t *testing.T) {
        connMap := newUDPConnMap()

        obs := &myConnObserver{}
        connIn1, err := newUDPConn(&net.UDPAddr{
            IP:   net.ParseIP("10.1.2.1"),
            Port: 5678,
        }, nil, obs)
        assert.NoError(t, err, "should succeed")
        err = connMap.insert(connIn1)
        assert.NoError(t, err, "should succeed")

        connIn2, err := newUDPConn(&net.UDPAddr{
            IP:   net.ParseIP("0.0.0.0"),
            Port: 5678,
        }, nil, obs)
        assert.NoError(t, err, "should succeed")

        err = connMap.insert(connIn2)
        assert.Error(t, err, "should fail")
    })

    t.Run("already in-use when inserting a specified IP", func(t *testing.T) {
        connMap := newUDPConnMap()

        obs := &myConnObserver{}
        connIn1, err := newUDPConn(&net.UDPAddr{
            IP:   net.ParseIP("0.0.0.0"),
            Port: 5678,
        }, nil, obs)
        assert.NoError(t, err, "should succeed")
        err = connMap.insert(connIn1)
        assert.NoError(t, err, "should succeed")

        connIn2, err := newUDPConn(&net.UDPAddr{
            IP:   net.ParseIP("192.168.0.1"),
            Port: 5678,
        }, nil, obs)
        assert.NoError(t, err, "should succeed")

        err = connMap.insert(connIn2)
        assert.Error(t, err, "should fail")
    })

    t.Run("already in-use when inserting the same specified IP", func(t *testing.T) {
        connMap := newUDPConnMap()

        obs := &myConnObserver{}
        connIn1, err := newUDPConn(&net.UDPAddr{
            IP:   net.ParseIP("192.168.0.1"),
            Port: 5678,
        }, nil, obs)
        assert.NoError(t, err, "should succeed")
        err = connMap.insert(connIn1)
        assert.NoError(t, err, "should succeed")

        connIn2, err := newUDPConn(&net.UDPAddr{
            IP:   net.ParseIP("192.168.0.1"),
            Port: 5678,
        }, nil, obs)
        assert.NoError(t, err, "should succeed")

        err = connMap.insert(connIn2)
        assert.Error(t, err, "should fail")
    })

    t.Run("find failure 1", func(t *testing.T) {
        connMap := newUDPConnMap()

        obs := &myConnObserver{}
        connIn, err := newUDPConn(&net.UDPAddr{
            IP:   net.ParseIP("192.168.0.1"),
            Port: 5678,
        }, nil, obs)
        assert.NoError(t, err, "should succeed")
        err = connMap.insert(connIn)
        assert.NoError(t, err, "should succeed")

        _, ok := connMap.find(&net.UDPAddr{
            IP:   net.ParseIP("192.168.0.2"),
            Port: 5678,
        })
        assert.False(t, ok, "should fail")
    })

    t.Run("find failure 2", func(t *testing.T) {
        connMap := newUDPConnMap()

        obs := &myConnObserver{}
        connIn, err := newUDPConn(&net.UDPAddr{
            IP:   net.ParseIP("192.168.0.1"),
            Port: 5678,
        }, nil, obs)
        assert.NoError(t, err, "should succeed")
        err = connMap.insert(connIn)
        assert.NoError(t, err, "should succeed")

        _, ok := connMap.find(&net.UDPAddr{
            IP:   net.ParseIP("192.168.0.1"),
            Port: 1234,
        })
        assert.False(t, ok, "should fail")
    })

    t.Run("insert two UDPConns on the same port, then remove them", func(t *testing.T) {
        connMap := newUDPConnMap()

        obs := &myConnObserver{}

        connIn1, err := newUDPConn(&net.UDPAddr{
            IP:   net.ParseIP("192.168.0.1"),
            Port: 5678,
        }, nil, obs)
        assert.NoError(t, err, "should succeed")
        err = connMap.insert(connIn1)
        assert.NoError(t, err, "should succeed")

        connIn2, err := newUDPConn(&net.UDPAddr{
            IP:   net.ParseIP("192.168.0.2"),
            Port: 5678,
        }, nil, obs)
        assert.NoError(t, err, "should succeed")
        err = connMap.insert(connIn2)
        assert.NoError(t, err, "should succeed")

        err = connMap.delete(connIn1.LocalAddr())
        assert.NoError(t, err, "should succeed")

        err = connMap.delete(connIn2.LocalAddr())
        assert.NoError(t, err, "should succeed")
    })
}
