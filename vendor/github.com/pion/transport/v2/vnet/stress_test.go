// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package vnet

import (
    "fmt"
    "net"
    "sync"
    "testing"
    "time"

    "github.com/pion/logging"
    "github.com/pion/transport/v2/test"
    "github.com/stretchr/testify/assert"
)

func TestStressTestUDP(t *testing.T) {
    loggerFactory := logging.NewDefaultLoggerFactory()
    log := loggerFactory.NewLogger("test")

    t.Run("lan to wan", func(t *testing.T) {
        tt := test.TimeOut(30 * time.Second)
        defer tt.Stop()

        // WAN with a nic (net0)
        wan, err := NewRouter(&RouterConfig{
            CIDR:          "1.2.3.0/24",
            QueueSize:     1000,
            LoggerFactory: loggerFactory,
        })
        assert.NoError(t, err, "should succeed")

        net0, err := NewNet(&NetConfig{
            StaticIPs: []string{demoIP},
        })
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        err = wan.AddNet(net0)
        assert.NoError(t, err, "should succeed")

        // LAN with a nic (net1)
        lan, err := NewRouter(&RouterConfig{
            CIDR:          "192.168.0.0/24",
            QueueSize:     1000,
            LoggerFactory: loggerFactory,
        })
        assert.NoError(t, err, "should succeed")

        net1, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        err = lan.AddNet(net1)
        assert.NoError(t, err, "should succeed")

        err = wan.AddRouter(lan)
        assert.NoError(t, err, "should succeed")

        err = wan.Start()
        assert.NoError(t, err, "should succeed")
        defer func() {
            err = wan.Stop()
            assert.NoError(t, err, "should succeed")
        }()

        // Find IP address for net0
        ifs, err := net0.Interfaces()
        if !assert.NoError(t, err, "should succeed") {
            return
        }
        log.Debugf("num ifs: %d", len(ifs))

        var echoServerIP net.IP
    loop:
        for _, ifc := range ifs {
            log.Debugf("flags: %v", ifc.Flags)
            if ifc.Flags&net.FlagUp == 0 {
                continue
            }
            if ifc.Flags&net.FlagLoopback != 0 {
                continue
            }

            addrs, err2 := ifc.Addrs()
            if !assert.NoError(t, err2, "should succeed") {
                return
            }
            log.Debugf("num addrs: %d", len(addrs))
            for _, addr := range addrs {
                log.Debugf("addr: %s", addr.String())
                switch addr := addr.(type) {
                case *net.IPNet:
                    echoServerIP = addr.IP
                    break loop
                case *net.IPAddr:
                    echoServerIP = addr.IP
                    break loop
                }
            }
        }
        if !assert.NotNil(t, echoServerIP, "should have IP address") {
            return
        }

        log.Debugf("echo server IP: %s", echoServerIP.String())

        // Set up an echo server on WAN
        conn0, err := net0.ListenPacket(
            "udp4", fmt.Sprintf("%s:0", echoServerIP))
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        doneCh0 := make(chan struct{})
        go func() {
            buf := make([]byte, 1500)
            for {
                n, from, err2 := conn0.ReadFrom(buf)
                if err2 != nil {
                    break
                }
                // echo back
                _, err2 = conn0.WriteTo(buf[:n], from)
                if err2 != nil {
                    break
                }
            }
            close(doneCh0)
        }()

        var wg sync.WaitGroup

        runEchoTest := func() {
            // Set up a client
            var numRecvd int
            const numToSend int = 400
            const pktSize int = 1200
            conn1, err2 := net0.ListenPacket("udp4", "0.0.0.0:0")
            if !assert.NoError(t, err2, "should succeed") {
                return
            }

            doneCh1 := make(chan struct{})
            go func() {
                buf := make([]byte, 1500)
                for {
                    n, _, err3 := conn1.ReadFrom(buf)
                    if err3 != nil {
                        break
                    }

                    if n != pktSize {
                        break
                    }

                    numRecvd++
                }
                close(doneCh1)
            }()

            buf := make([]byte, pktSize)
            to := conn0.LocalAddr()
            for i := 0; i < numToSend; i++ {
                _, err3 := conn1.WriteTo(buf, to)
                assert.NoError(t, err3, "should succeed")
                time.Sleep(10 * time.Millisecond)
            }

            time.Sleep(time.Second)

            err2 = conn1.Close()
            assert.NoError(t, err2, "should succeed")

            <-doneCh1

            // allow some packet loss
            assert.True(t, numRecvd >= numToSend*8/10, "majority should received")
            if numRecvd < numToSend {
                log.Infof("lost %d packets", numToSend-numRecvd)
            }

            wg.Done()
        }

        // Run echo tests concurrently
        for i := 0; i < 20; i++ {
            wg.Add(1)
            go runEchoTest()
        }
        wg.Wait()

        err = conn0.Close()
        assert.NoError(t, err, "should succeed")
    })
}
