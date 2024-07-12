// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

//go:build !js
// +build !js

package udp

import (
    "bytes"
    "errors"
    "fmt"
    "io"
    "net"
    "sync"
    "sync/atomic"
    "testing"
    "time"

    "github.com/pion/transport/v2/test"
    "github.com/stretchr/testify/assert"
)

var errHandshakeFailed = errors.New("handshake failed")

// Note: doesn't work since closing isn't propagated to the other side
// func TestNetTest(t *testing.T) {
//    lim := test.TimeOut(time.Minute*1 + time.Second*10)
//    defer lim.Stop()
//
//    nettest.TestConn(t, func() (c1, c2 net.Conn, stop func(), err error) {
//        listener, c1, c2, err = pipe()
//        if err != nil {
//            return nil, nil, nil, err
//        }
//        stop = func() {
//            c1.Close()
//            c2.Close()
//            listener.Close(1 * time.Second)
//        }
//        return
//    })
//}

func TestStressDuplex(t *testing.T) {
    // Limit runtime in case of deadlocks
    lim := test.TimeOut(time.Second * 20)
    defer lim.Stop()

    // Check for leaking routines
    report := test.CheckRoutines(t)
    defer report()

    // Run the test
    stressDuplex(t)
}

func stressDuplex(t *testing.T) {
    listener, ca, cb, err := pipe()
    if err != nil {
        t.Fatal(err)
    }

    defer func() {
        err = ca.Close()
        if err != nil {
            t.Fatal(err)
        }
        err = cb.Close()
        if err != nil {
            t.Fatal(err)
        }
        err = listener.Close()
        if err != nil {
            t.Fatal(err)
        }
    }()

    opt := test.Options{
        MsgSize:  2048,
        MsgCount: 1, // Can't rely on UDP message order in CI
    }

    err = test.StressDuplex(ca, cb, opt)
    if err != nil {
        t.Fatal(err)
    }
}

func TestListenerCloseTimeout(t *testing.T) {
    // Limit runtime in case of deadlocks
    lim := test.TimeOut(time.Second * 20)
    defer lim.Stop()

    // Check for leaking routines
    report := test.CheckRoutines(t)
    defer report()

    listener, ca, _, err := pipe()
    if err != nil {
        t.Fatal(err)
    }

    err = listener.Close()
    if err != nil {
        t.Fatal(err)
    }

    // Close client after server closes to cleanup
    err = ca.Close()
    if err != nil {
        t.Fatal(err)
    }
}

func TestListenerCloseUnaccepted(t *testing.T) {
    // Limit runtime in case of deadlocks
    lim := test.TimeOut(time.Second * 20)
    defer lim.Stop()

    // Check for leaking routines
    report := test.CheckRoutines(t)
    defer report()

    const backlog = 2

    network, addr := getConfig()
    listener, err := (&ListenConfig{
        Backlog: backlog,
    }).Listen(network, addr)
    if err != nil {
        t.Fatal(err)
    }

    for i := 0; i < backlog; i++ {
        conn, dErr := net.DialUDP(network, nil, listener.Addr().(*net.UDPAddr))
        if dErr != nil {
            t.Error(dErr)
            continue
        }
        if _, wErr := conn.Write([]byte{byte(i)}); wErr != nil {
            t.Error(wErr)
        }
        if cErr := conn.Close(); cErr != nil {
            t.Error(cErr)
        }
    }

    time.Sleep(100 * time.Millisecond) // Wait all packets being processed by readLoop

    // Unaccepted connections must be closed by listener.Close()
    if err = listener.Close(); err != nil {
        t.Fatal(err)
    }
}

func TestListenerAcceptFilter(t *testing.T) {
    // Limit runtime in case of deadlocks
    lim := test.TimeOut(time.Second * 20)
    defer lim.Stop()

    // Check for leaking routines
    report := test.CheckRoutines(t)
    defer report()

    testCases := map[string]struct {
        packet []byte
        accept bool
    }{
        "CreateConn": {
            packet: []byte{0xAA},
            accept: true,
        },
        "Discarded": {
            packet: []byte{0x00},
            accept: false,
        },
    }

    for name, testCase := range testCases {
        testCase := testCase
        t.Run(name, func(t *testing.T) {
            network, addr := getConfig()
            listener, err := (&ListenConfig{
                AcceptFilter: func(pkt []byte) bool {
                    return pkt[0] == 0xAA
                },
            }).Listen(network, addr)
            if err != nil {
                t.Fatal(err)
            }

            var wgAcceptLoop sync.WaitGroup
            wgAcceptLoop.Add(1)
            defer func() {
                if lErr := listener.Close(); lErr != nil {
                    t.Fatal(lErr)
                }
                wgAcceptLoop.Wait()
            }()

            conn, err := net.DialUDP(network, nil, listener.Addr().(*net.UDPAddr))
            if err != nil {
                t.Fatal(err)
            }
            if _, err := conn.Write(testCase.packet); err != nil {
                t.Fatal(err)
            }
            defer func() {
                if err := conn.Close(); err != nil {
                    t.Error(err)
                }
            }()

            chAccepted := make(chan struct{})
            go func() {
                defer wgAcceptLoop.Done()

                conn, aArr := listener.Accept()
                if aArr != nil {
                    if !errors.Is(aArr, ErrClosedListener) {
                        t.Error(aArr)
                    }
                    return
                }
                close(chAccepted)
                if err := conn.Close(); err != nil {
                    t.Error(err)
                }
            }()

            var accepted bool
            select {
            case <-chAccepted:
                accepted = true
            case <-time.After(10 * time.Millisecond):
            }

            if accepted != testCase.accept {
                if testCase.accept {
                    t.Error("Packet should create new conn")
                } else {
                    t.Error("Packet should not create new conn")
                }
            }
        })
    }
}

func TestListenerConcurrent(t *testing.T) {
    // Limit runtime in case of deadlocks
    lim := test.TimeOut(time.Second * 20)
    defer lim.Stop()

    // Check for leaking routines
    report := test.CheckRoutines(t)
    defer report()

    const backlog = 2

    network, addr := getConfig()
    listener, err := (&ListenConfig{
        Backlog: backlog,
    }).Listen(network, addr)
    if err != nil {
        t.Fatal(err)
    }

    for i := 0; i < backlog+1; i++ {
        conn, dErr := net.DialUDP(network, nil, listener.Addr().(*net.UDPAddr))
        if dErr != nil {
            t.Error(dErr)
            continue
        }
        if _, wErr := conn.Write([]byte{byte(i)}); wErr != nil {
            t.Error(wErr)
        }
        if cErr := conn.Close(); cErr != nil {
            t.Error(cErr)
        }
    }

    time.Sleep(100 * time.Millisecond) // Wait all packets being processed by readLoop

    for i := 0; i < backlog; i++ {
        conn, lErr := listener.Accept()
        if lErr != nil {
            t.Error(lErr)
            continue
        }
        b := make([]byte, 1)
        n, lErr := conn.Read(b)
        if lErr != nil {
            t.Error(lErr)
        } else if !bytes.Equal([]byte{byte(i)}, b[:n]) {
            t.Errorf("Packet from connection %d is wrong, expected: [%d], got: %v", i, i, b[:n])
        }
        if lErr = conn.Close(); lErr != nil {
            t.Error(lErr)
        }
    }

    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        if conn, lErr := listener.Accept(); !errors.Is(lErr, ErrClosedListener) {
            t.Errorf("Connection exceeding backlog limit must be discarded: %v", lErr)
            if lErr == nil {
                _ = conn.Close()
            }
        }
    }()

    time.Sleep(100 * time.Millisecond) // Last Accept should be discarded
    err = listener.Close()
    if err != nil {
        t.Fatal(err)
    }

    wg.Wait()
}

func pipe() (net.Listener, net.Conn, *net.UDPConn, error) {
    // Start listening
    network, addr := getConfig()
    listener, err := Listen(network, addr)
    if err != nil {
        return nil, nil, nil, fmt.Errorf("failed to listen: %w", err)
    }

    // Open a connection
    var dConn *net.UDPConn
    dConn, err = net.DialUDP(network, nil, listener.Addr().(*net.UDPAddr))
    if err != nil {
        return nil, nil, nil, fmt.Errorf("failed to dial: %w", err)
    }

    // Write to the connection to initiate it
    handshake := "hello"
    _, err = dConn.Write([]byte(handshake))
    if err != nil {
        return nil, nil, nil, fmt.Errorf("failed to write to dialed Conn: %w", err)
    }

    // Accept the connection
    var lConn net.Conn
    lConn, err = listener.Accept()
    if err != nil {
        return nil, nil, nil, fmt.Errorf("failed to accept Conn: %w", err)
    }

    var n int
    buf := make([]byte, len(handshake))
    if n, err = lConn.Read(buf); err != nil {
        return nil, nil, nil, fmt.Errorf("failed to read handshake: %w", err)
    }

    result := string(buf[:n])
    if handshake != result {
        return nil, nil, nil, fmt.Errorf("%w: %s != %s", errHandshakeFailed, handshake, result)
    }

    return listener, lConn, dConn, nil
}

func getConfig() (string, *net.UDPAddr) {
    return "udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0}
}

func TestConnClose(t *testing.T) {
    lim := test.TimeOut(time.Second * 5)
    defer lim.Stop()

    t.Run("Close", func(t *testing.T) {
        // Check for leaking routines
        report := test.CheckRoutines(t)
        defer report()

        l, ca, cb, errPipe := pipe()
        if errPipe != nil {
            t.Fatal(errPipe)
        }
        if err := ca.Close(); err != nil {
            t.Errorf("Failed to close A side: %v", err)
        }
        if err := cb.Close(); err != nil {
            t.Errorf("Failed to close B side: %v", err)
        }
        if err := l.Close(); err != nil {
            t.Errorf("Failed to close listener: %v", err)
        }
    })
    t.Run("CloseError1", func(t *testing.T) {
        // Check for leaking routines
        report := test.CheckRoutines(t)
        defer report()

        l, ca, cb, errPipe := pipe()
        if errPipe != nil {
            t.Fatal(errPipe)
        }
        // Close l.pConn to inject error.
        if err := l.(*listener).pConn.Close(); err != nil { //nolint:forcetypeassert
            t.Error(err)
        }

        if err := cb.Close(); err != nil {
            t.Errorf("Failed to close A side: %v", err)
        }
        if err := ca.Close(); err != nil {
            t.Errorf("Failed to close B side: %v", err)
        }
        if err := l.Close(); err == nil {
            t.Errorf("Error is not propagated to Listener.Close")
        }
    })
    t.Run("CloseError2", func(t *testing.T) {
        // Check for leaking routines
        report := test.CheckRoutines(t)
        defer report()

        l, ca, cb, errPipe := pipe()
        if errPipe != nil {
            t.Fatal(errPipe)
        }
        // Close l.pConn to inject error.
        if err := l.(*listener).pConn.Close(); err != nil { //nolint:forcetypeassert
            t.Error(err)
        }

        if err := cb.Close(); err != nil {
            t.Errorf("Failed to close A side: %v", err)
        }
        if err := l.Close(); err != nil {
            t.Errorf("Failed to close listener: %v", err)
        }
        if err := ca.Close(); err == nil {
            t.Errorf("Error is not propagated to Conn.Close")
        }
    })
    t.Run("CancelRead", func(t *testing.T) {
        // Limit runtime in case of deadlocks
        lim := test.TimeOut(time.Second * 5)
        defer lim.Stop()

        // Check for leaking routines
        report := test.CheckRoutines(t)
        defer report()

        l, ca, cb, errPipe := pipe()
        if errPipe != nil {
            t.Fatal(errPipe)
        }

        errC := make(chan error, 1)
        go func() {
            buf := make([]byte, 1024)
            // This read will block because we don't write on the other side.
            // Calling Close must unblock the call.
            _, err := ca.Read(buf)
            errC <- err
        }()

        if err := ca.Close(); err != nil { // Trigger Read cancellation.
            t.Errorf("Failed to close B side: %v", err)
        }

        // Main test condition, Read should return
        // after ca.Close() by closing the buffer.
        if err := <-errC; !errors.Is(err, io.EOF) {
            t.Errorf("expected err to be io.EOF but got %v", err)
        }

        if err := cb.Close(); err != nil {
            t.Errorf("Failed to close A side: %v", err)
        }
        if err := l.Close(); err != nil {
            t.Errorf("Failed to close listener: %v", err)
        }
    })
}

func TestBatchIO(t *testing.T) {
    lc := ListenConfig{
        Batch: BatchIOConfig{
            Enable:             true,
            ReadBatchSize:      10,
            WriteBatchSize:     3,
            WriteBatchInterval: 5 * time.Millisecond,
        },
        ReadBufferSize:  64 * 1024,
        WriteBufferSize: 64 * 1024,
    }

    laddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 15678}
    listener, err := lc.Listen("udp", laddr)
    if err != nil {
        t.Fatal(err)
    }

    var serverConnWg sync.WaitGroup
    serverConnWg.Add(1)
    go func() {
        var exit int32
        defer func() {
            defer serverConnWg.Done()
            atomic.StoreInt32(&exit, 1)
        }()
        for {
            buf := make([]byte, 1400)
            conn, lerr := listener.Accept()
            if errors.Is(lerr, ErrClosedListener) {
                break
            }
            assert.NoError(t, lerr)
            serverConnWg.Add(1)
            go func() {
                defer func() {
                    _ = conn.Close()
                    serverConnWg.Done()
                }()
                for atomic.LoadInt32(&exit) != 1 {
                    _ = conn.SetReadDeadline(time.Now().Add(time.Second))
                    n, rerr := conn.Read(buf)
                    if rerr != nil {
                        assert.ErrorContains(t, rerr, "timeout")
                    } else {
                        _, rerr = conn.Write(buf[:n])
                        assert.NoError(t, rerr)
                    }
                }
            }()
        }
    }()

    raddr, _ := listener.Addr().(*net.UDPAddr)

    // test flush by WriteBatchInterval expired
    readBuf := make([]byte, 1400)
    cli, err := net.DialUDP("udp", nil, raddr)
    assert.NoError(t, err)
    flushStr := "flushbytimer"
    _, err = cli.Write([]byte("flushbytimer"))
    assert.NoError(t, err)
    n, err := cli.Read(readBuf)
    assert.NoError(t, err)
    assert.Equal(t, flushStr, string(readBuf[:n]))

    wgs := sync.WaitGroup{}
    cc := 3
    wgs.Add(cc)

    for i := 0; i < cc; i++ {
        sendStr := fmt.Sprintf("hello %d", i)
        go func() {
            defer wgs.Done()
            buf := make([]byte, 1400)
            client, err := net.DialUDP("udp", nil, raddr)
            assert.NoError(t, err)
            defer func() { _ = client.Close() }()
            for i := 0; i < 1; i++ {
                _, err := client.Write([]byte(sendStr))
                assert.NoError(t, err)
                err = client.SetReadDeadline(time.Now().Add(time.Second))
                assert.NoError(t, err)
                n, err := client.Read(buf)
                assert.NoError(t, err)
                assert.Equal(t, sendStr, string(buf[:n]), i)
            }
        }()
    }
    wgs.Wait()

    _ = listener.Close()
    serverConnWg.Wait()
}
