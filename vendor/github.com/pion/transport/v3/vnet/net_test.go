// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package vnet

import (
    "fmt"
    "net"
    "testing"

    "github.com/pion/logging"
    "github.com/pion/transport/v3"
    "github.com/stretchr/testify/assert"
)

func TestNetVirtual(t *testing.T) {
    loggerFactory := logging.NewDefaultLoggerFactory()
    log := logging.NewDefaultLoggerFactory().NewLogger("test")

    t.Run("tnet.Interfaces", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        intfs, err := nw.Interfaces()
        assert.Equal(t, 2, len(intfs), "should be one tnet.Interface")
        assert.NoError(t, err, "should succeed")

        for _, ifc := range intfs {
            switch ifc.Name {
            case lo0String:
                assert.Equal(t, 1, ifc.Index, "Index mismatch")
                assert.Equal(t, 16384, ifc.MTU, "MTU mismatch")
                assert.Equal(t,
                    net.HardwareAddr(nil),
                    ifc.HardwareAddr,
                    "HardwareAddr mismatch")
                assert.Equal(t,
                    net.FlagUp|net.FlagLoopback|net.FlagMulticast,
                    ifc.Flags,
                    "Flags mismatch")

                addrs, err := ifc.Addrs()
                assert.NoError(t, err, "should succeed")
                assert.Equal(t, 1, len(addrs), "should be one address")
            case "eth0":
                assert.Equal(t, 2, ifc.Index, "Index mismatch")
                assert.Equal(t, 1500, ifc.MTU, "MTU mismatch")
                assert.Equal(t, 6, len(ifc.HardwareAddr), "HardwareAddr length mismatch")
                assert.Equal(t,
                    net.FlagUp|net.FlagMulticast,
                    ifc.Flags,
                    "Flags mismatch")

                _, err := ifc.Addrs()
                assert.NotNil(t, err, "should fail")
            default:
                assert.Fail(t, "unknown tnet.Interface: %v", ifc.Name)
            }

            if addrs, err := ifc.Addrs(); err == nil {
                for _, addr := range addrs {
                    log.Debugf("[%d] %s:%s",
                        ifc.Index,
                        addr.Network(),
                        addr.String())
                }
            }
        }
    })

    t.Run("tnet.InterfaceByName", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        intfs, err := nw.Interfaces()
        assert.Equal(t, 2, len(intfs), "should be one tnet.Interface")
        assert.NoError(t, err, "should succeed")

        var ifc *transport.Interface

        ifc, err = nw.InterfaceByName(lo0String)
        assert.NoError(t, err, "should succeed")
        if ifc.Name == lo0String {
            assert.Equal(t, 1, ifc.Index, "Index mismatch")
            assert.Equal(t, 16384, ifc.MTU, "MTU mismatch")
            assert.Equal(t,
                net.HardwareAddr(nil),
                ifc.HardwareAddr,
                "HardwareAddr mismatch")
            assert.Equal(t,
                net.FlagUp|net.FlagLoopback|net.FlagMulticast,
                ifc.Flags,
                "Flags mismatch")

            addrs, err2 := ifc.Addrs()
            assert.NoError(t, err2, "should succeed")
            assert.Equal(t, 1, len(addrs), "should be one address")
        }

        ifc, err = nw.InterfaceByName("eth0")
        assert.NoError(t, err, "should succeed")
        assert.Equal(t, 2, ifc.Index, "Index mismatch")
        assert.Equal(t, 1500, ifc.MTU, "MTU mismatch")
        assert.Equal(t, 6, len(ifc.HardwareAddr), "HardwareAddr length mismatch")
        assert.Equal(t,
            net.FlagUp|net.FlagMulticast,
            ifc.Flags,
            "Flags mismatch")

        _, err = ifc.Addrs()
        assert.NotNil(t, err, "should fail")

        _, err = nw.InterfaceByName("foo0")
        assert.NotNil(t, err, "should fail")
    })

    t.Run("hasIPAddr", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        intfs, err := nw.Interfaces()
        assert.Equal(t, 2, len(intfs), "should be one tnet.Interface")
        assert.NoError(t, err, "should succeed")

        var ifc *transport.Interface

        ifc, err = nw.InterfaceByName("eth0")
        assert.NoError(t, err, "should succeed")
        ifc.AddAddress(&net.IPNet{
            IP:   net.ParseIP("10.1.2.3"),
            Mask: net.CIDRMask(24, 32),
        })

        _, err = ifc.Addrs()
        assert.NoError(t, err, "should succeed")

        assert.True(t, nw.hasIPAddr(net.ParseIP("127.0.0.1")),
            "the IP addr should exist")
        assert.True(t, nw.hasIPAddr(net.ParseIP("10.1.2.3")),
            "the IP addr should exist")
        assert.False(t, nw.hasIPAddr(net.ParseIP("192.168.1.1")),
            "the IP addr should NOT exist")
    })

    t.Run("getAllIPAddrs", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        intfs, err := nw.Interfaces()
        assert.Equal(t, 2, len(intfs), "should be one tnet.Interface")
        assert.NoError(t, err, "should succeed")

        var ifc *transport.Interface

        ifc, err = nw.InterfaceByName("eth0")
        assert.NoError(t, err, "should succeed")
        ifc.AddAddress(&net.IPNet{
            IP:   net.ParseIP("10.1.2.3"),
            Mask: net.CIDRMask(24, 32),
        })

        ips := nw.getAllIPAddrs(false)
        assert.Equal(t, 2, len(ips), "should match")

        for _, ip := range ips {
            log.Debugf("ip: %s", ip.String())
        }
    })

    t.Run("assignPort()", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        addr := demoIP
        start := 1000
        end := 1002
        space := end + 1 - start

        intfs, err := nw.Interfaces()
        assert.Equal(t, 2, len(intfs), "should be one tnet.Interface")
        assert.NoError(t, err, "should succeed")

        var ifc *transport.Interface

        ifc, err = nw.InterfaceByName("eth0")
        assert.NoError(t, err, "should succeed")
        ifc.AddAddress(&net.IPNet{
            IP:   net.ParseIP(addr),
            Mask: net.CIDRMask(24, 32),
        })

        // attempt to assign port with start > end should fail
        _, err = nw.assignPort(net.ParseIP(addr), 3000, 2999)
        assert.NotNil(t, err, "should fail")

        for i := 0; i < space; i++ {
            port, err2 := nw.assignPort(net.ParseIP(addr), start, end)
            assert.NoError(t, err2, "should succeed")
            log.Debugf("[%d] got port: %d", i, port)

            conn, err2 := newUDPConn(&net.UDPAddr{
                IP:   net.ParseIP(addr),
                Port: port,
            }, nil, &myConnObserver{})
            assert.NoError(t, err2, "should succeed")
            err2 = nw.udpConns.insert(conn)
            assert.NoError(t, err2, "should succeed")
        }

        assert.Equal(t, space, nw.udpConns.size(), "should match")

        // attempt to assign again should fail
        _, err = nw.assignPort(net.ParseIP(addr), start, end)
        assert.NotNil(t, err, "should fail")
    })

    t.Run("determineSourceIP()", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        intfs, err := nw.Interfaces()
        assert.Equal(t, 2, len(intfs), "should be one tnet.Interface")
        assert.NoError(t, err, "should succeed")

        var ifc *transport.Interface

        ifc, err = nw.InterfaceByName("eth0")
        assert.NoError(t, err, "should succeed")
        ifc.AddAddress(&net.IPNet{
            IP:   net.ParseIP(demoIP),
            Mask: net.CIDRMask(24, 32),
        })

        // Any IP turned into non-loopback IP
        anyIP := net.ParseIP("0.0.0.0")
        dstIP := net.ParseIP("27.1.7.135")
        srcIP := nw.determineSourceIP(anyIP, dstIP)
        log.Debugf("anyIP: %s => %s", anyIP.String(), srcIP.String())
        assert.NotNil(t, srcIP, "shouldn't be nil")
        assert.Equal(t, srcIP.String(), demoIP, "use non-loopback IP")

        // Any IP turned into loopback IP
        anyIP = net.ParseIP("0.0.0.0")
        dstIP = net.ParseIP("127.0.0.2")
        srcIP = nw.determineSourceIP(anyIP, dstIP)
        log.Debugf("anyIP: %s => %s", anyIP.String(), srcIP.String())
        assert.NotNil(t, srcIP, "shouldn't be nil")
        assert.Equal(t, srcIP.String(), "127.0.0.1", "use loopback IP")

        // Non any IP won't change
        anyIP = net.ParseIP(demoIP)
        dstIP = net.ParseIP("127.0.0.2")
        srcIP = nw.determineSourceIP(anyIP, dstIP)
        log.Debugf("anyIP: %s => %s", anyIP.String(), srcIP.String())
        assert.NotNil(t, srcIP, "shouldn't be nil")
        assert.True(t, srcIP.Equal(anyIP), "IP change")
    })

    t.Run("ResolveUDPAddr", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        udpAddr, err := nw.ResolveUDPAddr(udp, "localhost:1234")
        if !assert.NoError(t, err, "should succeed") {
            return
        }
        assert.Equal(t, "127.0.0.1", udpAddr.IP.String(), "should match")
        assert.Equal(t, 1234, udpAddr.Port, "should match")
    })

    t.Run("UDPLoopback", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        conn, err := nw.ListenPacket(udp, "127.0.0.1:0")
        assert.NoError(t, err, "should succeed")
        laddr := conn.LocalAddr()
        msg := "PING!"
        n, err := conn.WriteTo([]byte(msg), laddr)
        assert.NoError(t, err, "should succeed")
        assert.Equal(t, len(msg), n, "should match")

        buf := make([]byte, 1000)
        n, addr, err := conn.ReadFrom(buf)
        assert.NoError(t, err, "should succeed")
        assert.Equal(t, len(msg), n, "should match")
        assert.Equal(t, msg, string(buf[:n]), "should match")
        assert.Equal(t, laddr.(*net.UDPAddr).String(), addr.(*net.UDPAddr).String(), "should match") //nolint:forcetypeassert

        assert.Equal(t, 1, nw.udpConns.size(), "should match")
        assert.NoError(t, conn.Close(), "should succeed")
        assert.Equal(t, 0, nw.udpConns.size(), "should match")
    })

    t.Run("ListenPacket random port", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        conn, err := nw.ListenPacket(udp, "127.0.0.1:0")
        assert.NoError(t, err, "should succeed")

        laddr := conn.LocalAddr().String()
        log.Debugf("laddr: %s", laddr)

        assert.Equal(t, 1, nw.udpConns.size(), "should match")
        assert.NoError(t, conn.Close(), "should succeed")
        assert.Equal(t, 0, nw.udpConns.size(), "should match")
    })

    t.Run("ListenPacket specific port", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        conn, err := nw.ListenPacket(udp, "127.0.0.1:50916")
        assert.NoError(t, err, "should succeed")

        laddr := conn.LocalAddr().String()
        assert.Equal(t, "127.0.0.1:50916", laddr, "should match")

        assert.Equal(t, 1, nw.udpConns.size(), "should match")
        assert.NoError(t, conn.Close(), "should succeed")
        assert.Equal(t, 0, nw.udpConns.size(), "should match")
    })

    t.Run("ListenUDP random port", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        srcAddr := &net.UDPAddr{
            IP: net.ParseIP("127.0.0.1"),
        }
        conn, err := nw.ListenUDP(udp, srcAddr)
        assert.NoError(t, err, "should succeed")

        laddr := conn.LocalAddr().String()
        log.Debugf("laddr: %s", laddr)

        assert.Equal(t, 1, nw.udpConns.size(), "should match")
        assert.NoError(t, conn.Close(), "should succeed")
        assert.Equal(t, 0, nw.udpConns.size(), "should match")
    })

    t.Run("ListenUDP specific port", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        srcAddr := &net.UDPAddr{
            IP:   net.ParseIP("127.0.0.1"),
            Port: 60916,
        }
        conn, err := nw.ListenUDP(udp, srcAddr)
        assert.NoError(t, err, "should succeed")

        laddr := conn.LocalAddr().String()
        assert.Equal(t, "127.0.0.1:60916", laddr, "should match")

        assert.Equal(t, 1, nw.udpConns.size(), "should match")
        assert.NoError(t, conn.Close(), "should succeed")
        assert.Equal(t, 0, nw.udpConns.size(), "should match")
    })

    t.Run("Dial (UDP) lo0", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        conn, err := nw.Dial(udp, "127.0.0.1:1234")
        assert.NoError(t, err, "should succeed")

        laddr := conn.LocalAddr()
        log.Debugf("laddr: %s", laddr.String())

        raddr := conn.RemoteAddr()
        log.Debugf("raddr: %s", raddr.String())

        assert.Equal(t, "127.0.0.1", laddr.(*net.UDPAddr).IP.String(), "should match") //nolint:forcetypeassert
        assert.True(t, laddr.(*net.UDPAddr).Port != 0, "should match")                 //nolint:forcetypeassert
        assert.Equal(t, "127.0.0.1:1234", raddr.String(), "should match")
        assert.Equal(t, 1, nw.udpConns.size(), "should match")
        assert.NoError(t, conn.Close(), "should succeed")
        assert.Equal(t, 0, nw.udpConns.size(), "should match")
    })

    t.Run("Dial (UDP) eth0", func(t *testing.T) {
        wan, err := NewRouter(&RouterConfig{
            CIDR:          "1.2.3.0/24",
            LoggerFactory: loggerFactory,
        })
        assert.NoError(t, err, "should succeed")

        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        assert.NoError(t, wan.AddNet(nw), "should succeed")

        conn, err := nw.Dial(udp, "27.3.4.5:1234")
        assert.NoError(t, err, "should succeed")

        laddr := conn.LocalAddr()
        log.Debugf("laddr: %s", laddr.String())

        raddr := conn.RemoteAddr()
        log.Debugf("raddr: %s", raddr.String())

        assert.Equal(t, "1.2.3.1", laddr.(*net.UDPAddr).IP.String(), "should match") //nolint:forcetypeassert
        assert.True(t, laddr.(*net.UDPAddr).Port != 0, "should match")               //nolint:forcetypeassert
        assert.Equal(t, "27.3.4.5:1234", raddr.String(), "should match")
        assert.Equal(t, 1, nw.udpConns.size(), "should match")
        assert.NoError(t, conn.Close(), "should succeed")
        assert.Equal(t, 0, nw.udpConns.size(), "should match")
    })

    t.Run("DialUDP", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        locAddr := &net.UDPAddr{
            IP:   net.IPv4(127, 0, 0, 1),
            Port: 0,
        }

        remAddr := &net.UDPAddr{
            IP:   net.IPv4(127, 0, 0, 1),
            Port: 1234,
        }

        conn, err := nw.DialUDP(udp, locAddr, remAddr)
        assert.NoError(t, err, "should succeed")

        laddr := conn.LocalAddr()
        log.Debugf("laddr: %s", laddr.String())

        raddr := conn.RemoteAddr()
        log.Debugf("raddr: %s", raddr.String())

        assert.Equal(t, "127.0.0.1", laddr.(*net.UDPAddr).IP.String(), "should match") //nolint:forcetypeassert
        assert.True(t, laddr.(*net.UDPAddr).Port != 0, "should match")                 //nolint:forcetypeassert
        assert.Equal(t, "127.0.0.1:1234", raddr.String(), "should match")
        assert.Equal(t, 1, nw.udpConns.size(), "should match")
        assert.NoError(t, conn.Close(), "should succeed")
        assert.Equal(t, 0, nw.udpConns.size(), "should match")
    })

    t.Run("Resolver", func(t *testing.T) {
        wan, err := NewRouter(&RouterConfig{
            CIDR:          "1.2.3.0/24",
            LoggerFactory: loggerFactory,
        })
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        err = wan.AddHost("test.pion.ly", "30.31.32.33")
        assert.NoError(t, err, "should succeed")

        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        assert.NoError(t, wan.AddNet(nw), "should succeed")

        conn, err := nw.Dial(udp, "test.pion.ly:1234")
        assert.NoError(t, err, "should succeed")

        laddr := conn.LocalAddr()
        log.Debugf("laddr: %s", laddr.String())

        raddr := conn.RemoteAddr()
        log.Debugf("raddr: %s", raddr.String())

        assert.Equal(t, "1.2.3.1", laddr.(*net.UDPAddr).IP.String(), "should match") //nolint:forcetypeassert
        assert.True(t, laddr.(*net.UDPAddr).Port != 0, "should match")               //nolint:forcetypeassert
        assert.Equal(t, "30.31.32.33:1234", raddr.String(), "should match")
        assert.Equal(t, 1, nw.udpConns.size(), "should match")
        assert.NoError(t, conn.Close(), "should succeed")
        assert.Equal(t, 0, nw.udpConns.size(), "should match")
    })

    t.Run("Loopback", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        conn, err := nw.ListenPacket(udp, "127.0.0.1:50916")
        assert.NoError(t, err, "should succeed")

        laddr := conn.LocalAddr()
        assert.Equal(t, "127.0.0.1:50916", laddr.String(), "should match")

        c := newChunkUDP(&net.UDPAddr{
            IP:   net.ParseIP("127.0.0.1"),
            Port: 4000,
        }, &net.UDPAddr{
            IP:   net.ParseIP("127.0.0.1"),
            Port: 50916,
        })
        c.userData = []byte("Hello!")

        var hasReceived bool
        recvdCh := make(chan bool)
        doneCh := make(chan struct{})

        go func() {
            var err error
            var n int
            var addr net.Addr
            buf := make([]byte, 1500)
            for {
                n, addr, err = conn.ReadFrom(buf)
                if err != nil {
                    log.Debugf("ReadFrom returned: %v", err)
                    break
                }

                assert.Equal(t, 6, len(c.userData), "should match")
                assert.Equal(t, "127.0.0.1:4000", addr.String(), "should match")
                assert.Equal(t, "Hello!", string(buf[:n]), "should match")

                recvdCh <- true
            }

            close(doneCh)
        }()

        nw.onInboundChunk(c)

    loop:
        for {
            select {
            case <-recvdCh:
                hasReceived = true
                assert.NoError(t, conn.Close(), "should succeed")
            case <-doneCh:
                break loop
            }
        }

        assert.Equal(t, 0, nw.udpConns.size(), "should match")
        assert.True(t, hasReceived, "should have received data")
    })

    t.Run("End-to-End", func(t *testing.T) {
        doneCh := make(chan struct{})

        // WAN
        wan, err := NewRouter(&RouterConfig{
            CIDR:          "1.2.3.0/24",
            LoggerFactory: loggerFactory,
        })
        assert.NoError(t, err, "should succeed")

        net1, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        err = wan.AddNet(net1)
        assert.NoError(t, err, "should succeed")
        ip1, err := getIPAddr(net1)
        assert.NoError(t, err, "should succeed")

        net2, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        err = wan.AddNet(net2)
        assert.NoError(t, err, "should succeed")
        ip2, err := getIPAddr(net2)
        assert.NoError(t, err, "should succeed")

        conn1, err := net1.ListenPacket(
            udp,
            fmt.Sprintf("%s:%d", ip1, 1234),
        )
        assert.NoError(t, err, "should succeed")

        conn2, err := net2.ListenPacket(
            udp,
            fmt.Sprintf("%s:%d", ip2, 5678),
        )
        assert.NoError(t, err, "should succeed")

        // start the router
        err = wan.Start()
        assert.NoError(t, err, "should succeed")

        conn1RcvdCh := make(chan bool)

        // conn1
        go func() {
            buf := make([]byte, 1500)
            for {
                log.Debug("conn1: wait for a message..")
                n, _, err2 := conn1.ReadFrom(buf)
                if err2 != nil {
                    log.Debugf("ReadFrom returned: %v", err2)
                    break
                }

                log.Debugf("conn1 received %s", string(buf[:n]))
                conn1RcvdCh <- true
            }
            close(doneCh)
        }()

        // conn2
        go func() {
            buf := make([]byte, 1500)
            for {
                log.Debug("conn2: wait for a message..")
                n, addr, err2 := conn2.ReadFrom(buf)
                if err2 != nil {
                    log.Debugf("ReadFrom returned: %v", err2)
                    break
                }

                log.Debugf("conn2 received %s", string(buf[:n]))

                // echo back to conn1
                nSent, err2 := conn2.WriteTo([]byte("Good-bye!"), addr)
                assert.NoError(t, err2, "should succeed")
                assert.Equal(t, 9, nSent, "should match")
            }
        }()

        log.Debug("conn1: sending")
        nSent, err := conn1.WriteTo(
            []byte("Hello!"),
            conn2.LocalAddr(),
        )
        assert.NoError(t, err, "should succeed")
        assert.Equal(t, 6, nSent, "should match")

    loop:
        for {
            select {
            case <-conn1RcvdCh:
                assert.NoError(t, conn1.Close(), "should succeed")
                assert.NoError(t, conn2.Close(), "should succeed")
            case <-doneCh:
                break loop
            }
        }

        assert.NoError(t, wan.Stop(), "should succeed")
    })

    t.Run("Dialer", func(t *testing.T) {
        nw, err := NewNet(&NetConfig{})
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        dialer := nw.CreateDialer(&net.Dialer{
            LocalAddr: &net.UDPAddr{
                IP:   net.ParseIP("127.0.0.1"),
                Port: 0,
            },
        })

        conn, err := dialer.Dial(udp, "127.0.0.1:1234")
        assert.NoError(t, err, "should succeed")

        laddr := conn.LocalAddr()
        log.Debugf("laddr: %s", laddr.String())

        raddr := conn.RemoteAddr()
        log.Debugf("raddr: %s", raddr.String())

        assert.Equal(t, "127.0.0.1", laddr.(*net.UDPAddr).IP.String(), "should match") //nolint:forcetypeassert
        assert.True(t, laddr.(*net.UDPAddr).Port != 0, "should match")                 //nolint:forcetypeassert
        assert.Equal(t, "127.0.0.1:1234", raddr.String(), "should match")
        assert.Equal(t, 1, nw.udpConns.size(), "should match")
        assert.NoError(t, conn.Close(), "should succeed")
        assert.Equal(t, 0, nw.udpConns.size(), "should match")
    })

    t.Run("Two IPs on a NIC", func(t *testing.T) {
        doneCh := make(chan struct{})

        // WAN
        wan, err := NewRouter(&RouterConfig{
            CIDR:          "1.2.3.0/24",
            LoggerFactory: loggerFactory,
        })
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        net1, err := NewNet(&NetConfig{
            StaticIPs: []string{
                demoIP,
                "1.2.3.5",
            },
        })
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        err = wan.AddNet(net1)
        assert.NoError(t, err, "should succeed")

        // start the router
        err = wan.Start()
        assert.NoError(t, err, "should succeed")

        conn1, err := net1.ListenPacket(udp, "1.2.3.4:1234")
        assert.NoError(t, err, "should succeed")

        conn2, err := net1.ListenPacket(udp, "1.2.3.5:1234")
        assert.NoError(t, err, "should succeed")

        conn1RcvdCh := make(chan bool)

        // conn1
        go func() {
            buf := make([]byte, 1500)
            for {
                log.Debug("conn1: wait for a message..")
                n, _, err2 := conn1.ReadFrom(buf)
                if err2 != nil {
                    log.Debugf("ReadFrom returned: %v", err2)
                    break
                }

                log.Debugf("conn1 received %s", string(buf[:n]))
                conn1RcvdCh <- true
            }
            close(doneCh)
        }()

        // conn2
        go func() {
            buf := make([]byte, 1500)
            for {
                log.Debug("conn2: wait for a message..")
                n, addr, err2 := conn2.ReadFrom(buf)
                if err2 != nil {
                    log.Debugf("ReadFrom returned: %v", err2)
                    break
                }

                log.Debugf("conn2 received %s", string(buf[:n]))

                // echo back to conn1
                nSent, err2 := conn2.WriteTo([]byte("Good-bye!"), addr)
                assert.NoError(t, err2, "should succeed")
                assert.Equal(t, 9, nSent, "should match")
            }
        }()

        log.Debug("conn1: sending")
        nSent, err := conn1.WriteTo(
            []byte("Hello!"),
            conn2.LocalAddr(),
        )
        assert.NoError(t, err, "should succeed")
        assert.Equal(t, 6, nSent, "should match")

    loop:
        for {
            select {
            case <-conn1RcvdCh:
                assert.NoError(t, conn1.Close(), "should succeed")
                assert.NoError(t, conn2.Close(), "should succeed")
            case <-doneCh:
                break loop
            }
        }

        assert.NoError(t, wan.Stop(), "should succeed")
    })
}
