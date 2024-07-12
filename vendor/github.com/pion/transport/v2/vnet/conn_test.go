// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package vnet

import (
    "errors"
    "net"
    "sync/atomic"
    "testing"
    "time"

    "github.com/pion/logging"
    "github.com/stretchr/testify/assert"
)

var errFailedToCovertToChuckUDP = errors.New("failed to convert chunk to chunkUDP")

type dummyObserver struct {
    onWrite    func(Chunk) error
    onOnClosed func(net.Addr)
}

func (o *dummyObserver) write(c Chunk) error {
    return o.onWrite(c)
}

func (o *dummyObserver) onClosed(addr net.Addr) {
    o.onOnClosed(addr)
}

func (o *dummyObserver) determineSourceIP(locIP, _ net.IP) net.IP {
    return locIP
}

func TestUDPConn(t *testing.T) {
    log := logging.NewDefaultLoggerFactory().NewLogger("test")

    t.Run("WriteTo ReadFrom", func(t *testing.T) {
        var nClosed int32
        var conn *UDPConn
        var err error
        data := []byte("Hello")
        srcAddr := &net.UDPAddr{
            IP:   net.ParseIP("127.0.0.1"),
            Port: 1234,
        }
        dstAddr := &net.UDPAddr{
            IP:   net.ParseIP("127.0.0.1"),
            Port: 5678,
        }

        obs := &dummyObserver{
            onWrite: func(c Chunk) error {
                uc, ok := c.(*chunkUDP)
                if !ok {
                    return errFailedToCovertToChuckUDP
                }
                chunk := newChunkUDP(uc.DestinationAddr().(*net.UDPAddr), uc.SourceAddr().(*net.UDPAddr)) //nolint:forcetypeassert
                chunk.userData = make([]byte, len(uc.userData))
                copy(chunk.userData, uc.userData)
                conn.readCh <- chunk // echo back
                return nil
            },
            onOnClosed: func(addr net.Addr) {
                atomic.AddInt32(&nClosed, 1)
            },
        }
        conn, err = newUDPConn(srcAddr, nil, obs)
        assert.NoError(t, err, "should succeed")

        rcvdCh := make(chan struct{})
        doneCh := make(chan struct{})

        go func() {
            buf := make([]byte, 1500)

            for {
                n, addr, err2 := conn.ReadFrom(buf)
                if err2 != nil {
                    log.Debug("conn closed. exiting the read loop")
                    break
                }
                log.Debug("read data")
                assert.Equal(t, len(data), n, "should match")
                assert.Equal(t, string(data), string(data), "should match")
                assert.Equal(t, dstAddr.String(), addr.String(), "should match")
                rcvdCh <- struct{}{}
            }

            close(doneCh)
        }()

        var n int
        n, err = conn.WriteTo(data, dstAddr)
        if !assert.Nil(t, err, "should succeed") {
            return
        }
        assert.Equal(t, len(data), n, "should match")

    loop:
        for {
            select {
            case <-rcvdCh:
                log.Debug("closing conn..")
                err2 := conn.Close()
                assert.Nil(t, err2, "should succeed")
            case <-doneCh:
                break loop
            }
        }

        assert.Equal(t, int32(1), atomic.LoadInt32(&nClosed), "should be closed once")
    })

    t.Run("Write Read", func(t *testing.T) {
        var nClosed int32
        var conn *UDPConn
        var err error
        data := []byte("Hello")
        srcAddr := &net.UDPAddr{
            IP:   net.ParseIP("127.0.0.1"),
            Port: 1234,
        }
        dstAddr := &net.UDPAddr{
            IP:   net.ParseIP("127.0.0.1"),
            Port: 5678,
        }

        obs := &dummyObserver{
            onWrite: func(c Chunk) error {
                uc, ok := c.(*chunkUDP)
                if !ok {
                    return errFailedToCovertToChuckUDP
                }
                //nolint:forcetypeassert
                chunk := newChunkUDP(
                    uc.DestinationAddr().(*net.UDPAddr),
                    uc.SourceAddr().(*net.UDPAddr),
                )
                chunk.userData = make([]byte, len(uc.userData))
                copy(chunk.userData, uc.userData)
                conn.readCh <- chunk // echo back
                return nil
            },
            onOnClosed: func(addr net.Addr) {
                atomic.AddInt32(&nClosed, 1)
            },
        }
        conn, err = newUDPConn(srcAddr, nil, obs)
        assert.NoError(t, err, "should succeed")
        conn.remAddr = dstAddr

        rcvdCh := make(chan struct{})
        doneCh := make(chan struct{})

        go func() {
            buf := make([]byte, 1500)

            for {
                n, err2 := conn.Read(buf)
                if err2 != nil {
                    log.Debug("conn closed. exiting the read loop")
                    break
                }
                log.Debug("read data")
                assert.Equal(t, len(data), n, "should match")
                assert.Equal(t, string(data), string(data), "should match")
                rcvdCh <- struct{}{}
            }

            close(doneCh)
        }()

        var n int
        n, err = conn.Write(data)
        if !assert.Nil(t, err, "should succeed") {
            return
        }

        assert.Equal(t, len(data), n, "should match")

    loop:
        for {
            select {
            case <-rcvdCh:
                log.Debug("closing conn..")
                err = conn.Close()
                assert.Nil(t, err, "should succeed")
            case <-doneCh:
                break loop
            }
        }

        assert.Equal(t, int32(1), atomic.LoadInt32(&nClosed), "should be closed once")
    })

    deadlineTest := func(t *testing.T, readOnly bool) {
        var nClosed int32
        var conn *UDPConn
        var err error
        srcAddr := &net.UDPAddr{
            IP:   net.ParseIP("127.0.0.1"),
            Port: 1234,
        }
        obs := &dummyObserver{
            onOnClosed: func(addr net.Addr) {
                atomic.AddInt32(&nClosed, 1)
            },
        }
        conn, err = newUDPConn(srcAddr, nil, obs)
        assert.NoError(t, err, "should succeed")

        doneCh := make(chan struct{})

        if readOnly {
            err = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
        } else {
            err = conn.SetDeadline(time.Now().Add(50 * time.Millisecond))
        }
        assert.Nil(t, err, "should succeed")

        go func() {
            buf := make([]byte, 1500)
            _, _, err := conn.ReadFrom(buf)
            assert.NotNil(t, err, "should return error")
            var ne *net.OpError
            if errors.As(err, &ne) {
                assert.True(t, ne.Timeout(), "should be a timeout")
            } else {
                assert.True(t, false, "should be an net.OpError")
            }

            assert.Nil(t, conn.Close(), "should succeed")
            close(doneCh)
        }()

        <-doneCh

        assert.Equal(t, int32(1), atomic.LoadInt32(&nClosed), "should be closed once")
    }

    t.Run("SetReadDeadline", func(t *testing.T) {
        deadlineTest(t, true)
    })

    t.Run("SetDeadline", func(t *testing.T) {
        deadlineTest(t, false)
    })

    t.Run("Inbound during close", func(t *testing.T) {
        var conn *UDPConn
        var err error
        srcAddr := &net.UDPAddr{
            IP:   net.ParseIP("127.0.0.1"),
            Port: 1234,
        }
        obs := &dummyObserver{
            onOnClosed: func(net.Addr) {},
        }

        for i := 0; i < 1000; i++ { // nolint:staticcheck // (false positive detection)
            conn, err = newUDPConn(srcAddr, nil, obs)
            assert.NoError(t, err, "should succeed")

            chDone := make(chan struct{})
            go func() {
                time.Sleep(20 * time.Millisecond)
                assert.NoError(t, conn.Close())
                close(chDone)
            }()
            tick := time.NewTicker(10 * time.Millisecond)
            for {
                defer tick.Stop()
                select {
                case <-chDone:
                    return
                case <-tick.C:
                    conn.onInboundChunk(nil)
                }
            }
        }
    })
}
