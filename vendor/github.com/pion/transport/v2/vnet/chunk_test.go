// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package vnet

import (
    "net"
    "strings"
    "testing"

    "github.com/pion/logging"
    "github.com/stretchr/testify/assert"
)

func TestTCPFragString(t *testing.T) {
    f := tcpFIN
    assert.Equal(t, "FIN", f.String(), "should match")
    f = tcpSYN
    assert.Equal(t, "SYN", f.String(), "should match")
    f = tcpRST
    assert.Equal(t, "RST", f.String(), "should match")
    f = tcpPSH
    assert.Equal(t, "PSH", f.String(), "should match")
    f = tcpACK
    assert.Equal(t, "ACK", f.String(), "should match")
    f = tcpSYN | tcpACK
    assert.Equal(t, "SYN-ACK", f.String(), "should match")
}

func TestChunk(t *testing.T) {
    loggerFactory := logging.NewDefaultLoggerFactory()
    log := loggerFactory.NewLogger("test")

    t.Run("ChunkUDP", func(t *testing.T) {
        src := &net.UDPAddr{
            IP:   net.ParseIP("192.168.0.2"),
            Port: 1234,
        }
        dst := &net.UDPAddr{
            IP:   net.ParseIP(demoIP),
            Port: 5678,
        }

        var c Chunk = newChunkUDP(src, dst)
        str := c.String()
        log.Debugf("chunk: %s", str)
        assert.Equal(t, udp, c.Network(), "should match")
        assert.True(t, strings.Contains(str, src.Network()), "should include network type")
        assert.True(t, strings.Contains(str, src.String()), "should include address")
        assert.True(t, strings.Contains(str, dst.String()), "should include address")
        assert.True(t, c.getSourceIP().Equal(src.IP), "ip should match")
        assert.True(t, c.getDestinationIP().Equal(dst.IP), "ip should match")

        // Test timestamp
        ts := c.setTimestamp()
        assert.Equal(t, ts, c.getTimestamp(), "timestamp should match")

        uc := c.(*chunkUDP) //nolint:forcetypeassert
        uc.userData = []byte("Hello")

        cloned := c.Clone().(*chunkUDP) //nolint:forcetypeassert

        // Test setSourceAddr
        err := uc.setSourceAddr("2.3.4.5:4000")
        assert.Nil(t, err, "should succeed")
        assert.Equal(t, "2.3.4.5:4000", uc.SourceAddr().String())

        // Test Tag()
        assert.True(t, len(uc.tag) > 0, "should not be empty")
        assert.Equal(t, uc.tag, uc.Tag(), "should match")

        // Verify cloned chunk was not affected by the changes to original chunk
        uc.userData[0] = []byte("!")[0] // original: "Hello" -> "Hell!"
        assert.Equal(t, "Hello", string(cloned.userData), "should match")
        assert.Equal(t, "192.168.0.2:1234", cloned.SourceAddr().String())
        assert.True(t, cloned.getSourceIP().Equal(src.IP), "ip should match")
        assert.True(t, cloned.getDestinationIP().Equal(dst.IP), "ip should match")
    })

    t.Run("ChunkTCP", func(t *testing.T) {
        src := &net.TCPAddr{
            IP:   net.ParseIP("192.168.0.2"),
            Port: 1234,
        }
        dst := &net.TCPAddr{
            IP:   net.ParseIP(demoIP),
            Port: 5678,
        }

        var c Chunk = newChunkTCP(src, dst, tcpSYN)
        str := c.String()
        log.Debugf("chunk: %s", str)
        assert.Equal(t, "tcp", c.Network(), "should match")
        assert.True(t, strings.Contains(str, src.Network()), "should include network type")
        assert.True(t, strings.Contains(str, src.String()), "should include address")
        assert.True(t, strings.Contains(str, dst.String()), "should include address")
        assert.True(t, c.getSourceIP().Equal(src.IP), "ip should match")
        assert.True(t, c.getDestinationIP().Equal(dst.IP), "ip should match")

        tcp, ok := c.(*chunkTCP)
        assert.True(t, ok, "type should match")
        assert.Equal(t, tcp.flags, tcpSYN, "flags should match")

        // Test timestamp
        ts := c.setTimestamp()
        assert.Equal(t, ts, c.getTimestamp(), "timestamp should match")

        tc := c.(*chunkTCP) //nolint:forcetypeassert
        tc.userData = []byte("Hello")

        cloned := c.Clone().(*chunkTCP) //nolint:forcetypeassert

        // Test setSourceAddr
        err := tc.setSourceAddr("2.3.4.5:4000")
        assert.Nil(t, err, "should succeed")
        assert.Equal(t, "2.3.4.5:4000", tc.SourceAddr().String())

        // Test Tag()
        assert.True(t, len(tc.tag) > 0, "should not be empty")
        assert.Equal(t, tc.tag, tc.Tag(), "should match")

        // Verify cloned chunk was not affected by the changes to original chunk
        tc.userData[0] = []byte("!")[0] // original: "Hello" -> "Hell!"
        assert.Equal(t, "Hello", string(cloned.userData), "should match")
        assert.Equal(t, "192.168.0.2:1234", cloned.SourceAddr().String())
        assert.True(t, cloned.getSourceIP().Equal(src.IP), "ip should match")
        assert.True(t, cloned.getDestinationIP().Equal(dst.IP), "ip should match")

        // Test setDestinationAddr
        err = tc.setDestinationAddr("3.4.5.6:7000")
        assert.Nil(t, err, "should succeed")
        assert.Equal(t, "3.4.5.6:7000", tc.DestinationAddr().String())
    })
}
