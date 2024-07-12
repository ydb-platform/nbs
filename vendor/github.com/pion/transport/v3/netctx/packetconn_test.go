// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package netctx

import (
    "bytes"
    "context"
    "errors"
    "io"
    "net"
    "testing"
    "time"
)

var _ net.PacketConn = wrapConn{}

type wrapConn struct {
    c net.Conn
}

func (w wrapConn) ReadFrom(p []byte) (int, net.Addr, error) {
    n, err := w.c.Read(p)
    return n, nil, err
}

func (w wrapConn) WriteTo(p []byte, _ net.Addr) (n int, err error) {
    return w.c.Write(p)
}

func (w wrapConn) Close() error {
    return w.c.Close()
}

func (w wrapConn) LocalAddr() net.Addr {
    return w.c.LocalAddr()
}

func (w wrapConn) RemoteAddr() net.Addr {
    return w.c.RemoteAddr()
}

func (w wrapConn) SetDeadline(t time.Time) error {
    return w.c.SetDeadline(t)
}

func (w wrapConn) SetReadDeadline(t time.Time) error {
    return w.c.SetReadDeadline(t)
}

func (w wrapConn) SetWriteDeadline(t time.Time) error {
    return w.c.SetWriteDeadline(t)
}

func pipe() (net.PacketConn, net.PacketConn) {
    a, b := net.Pipe()
    return wrapConn{a}, wrapConn{b}
}

func TestReadFrom(t *testing.T) {
    ca, cb := pipe()
    defer func() {
        _ = ca.Close()
    }()

    data := []byte{0x01, 0x02, 0xFF}
    chErr := make(chan error)

    go func() {
        _, err := cb.WriteTo(data, nil)
        chErr <- err
    }()

    c := NewPacketConn(ca)
    b := make([]byte, 100)
    n, _, err := c.ReadFromContext(context.Background(), b)
    if err != nil {
        t.Fatal(err)
    }
    if n != len(data) {
        t.Errorf("Wrong data length, expected %d, got %d", len(data), n)
    }
    if !bytes.Equal(data, b[:n]) {
        t.Errorf("Wrong data, expected %v, got %v", data, b)
    }

    err = <-chErr
    if err != nil {
        t.Fatal(err)
    }
}

func TestReadFromTimeout(t *testing.T) {
    ca, _ := pipe()
    defer func() {
        _ = ca.Close()
    }()

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
    defer cancel()

    c := NewPacketConn(ca)
    b := make([]byte, 100)
    n, _, err := c.ReadFromContext(ctx, b)
    if err == nil {
        t.Error("Read unexpectedly succeeded")
    }
    if n != 0 {
        t.Errorf("Wrong data length, expected %d, got %d", 0, n)
    }
}

func TestReadFromCancel(t *testing.T) {
    ca, _ := pipe()
    defer func() {
        _ = ca.Close()
    }()

    ctx, cancel := context.WithCancel(context.Background())
    go func() {
        time.Sleep(10 * time.Millisecond)
        cancel()
    }()

    c := NewPacketConn(ca)
    b := make([]byte, 100)
    n, _, err := c.ReadFromContext(ctx, b)
    if err == nil {
        t.Error("Read unexpectedly succeeded")
    }
    if n != 0 {
        t.Errorf("Wrong data length, expected %d, got %d", 0, n)
    }
}

func TestReadFromClosed(t *testing.T) {
    ca, _ := pipe()

    c := NewPacketConn(ca)
    _ = c.Close()

    b := make([]byte, 100)
    n, _, err := c.ReadFromContext(context.Background(), b)
    if !errors.Is(err, net.ErrClosed) {
        t.Errorf("Expected error '%v', got '%v'", net.ErrClosed, err)
    }
    if n != 0 {
        t.Errorf("Wrong data length, expected %d, got %d", 0, n)
    }
}

func TestWriteTo(t *testing.T) {
    ca, cb := pipe()
    defer func() {
        _ = ca.Close()
    }()

    chErr := make(chan error)
    chRead := make(chan []byte)

    go func() {
        b := make([]byte, 100)
        n, _, err := cb.ReadFrom(b)
        chErr <- err
        chRead <- b[:n]
    }()

    c := NewPacketConn(ca)
    data := []byte{0x01, 0x02, 0xFF}
    n, err := c.WriteToContext(context.Background(), data, nil)
    if err != nil {
        t.Fatal(err)
    }
    if n != len(data) {
        t.Errorf("Wrong data length, expected %d, got %d", len(data), n)
    }

    err = <-chErr
    b := <-chRead
    if !bytes.Equal(data, b) {
        t.Errorf("Wrong data, expected %v, got %v", data, b)
    }
    if err != nil {
        t.Fatal(err)
    }
}

func TestWriteToTimeout(t *testing.T) {
    ca, _ := pipe()
    defer func() {
        _ = ca.Close()
    }()

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
    defer cancel()

    c := NewPacketConn(ca)
    b := make([]byte, 100)
    n, err := c.WriteToContext(ctx, b, nil)
    if err == nil {
        t.Error("Write unexpectedly succeeded")
    }
    if n != 0 {
        t.Errorf("Wrong data length, expected %d, got %d", 0, n)
    }
}

func TestWriteToCancel(t *testing.T) {
    ca, _ := pipe()
    defer func() {
        _ = ca.Close()
    }()

    ctx, cancel := context.WithCancel(context.Background())
    go func() {
        time.Sleep(10 * time.Millisecond)
        cancel()
    }()

    c := NewPacketConn(ca)
    b := make([]byte, 100)
    n, err := c.WriteToContext(ctx, b, nil)
    if err == nil {
        t.Error("Write unexpectedly succeeded")
    }
    if n != 0 {
        t.Errorf("Wrong data length, expected %d, got %d", 0, n)
    }
}

func TestWriteToClosed(t *testing.T) {
    ca, _ := pipe()

    c := NewPacketConn(ca)
    _ = c.Close()

    b := make([]byte, 100)
    n, err := c.WriteToContext(context.Background(), b, nil)
    if !errors.Is(err, ErrClosing) {
        t.Errorf("Expected error '%v', got '%v'", ErrClosing, err)
    }
    if n != 0 {
        t.Errorf("Wrong data length, expected %d, got %d", 0, n)
    }
}

type packetConnAddrMock struct{}

func (*packetConnAddrMock) LocalAddr() net.Addr                    { return stringAddr{"local_net", "local_addr"} }
func (*packetConnAddrMock) ReadFrom([]byte) (int, net.Addr, error) { panic("unimplemented") }
func (*packetConnAddrMock) WriteTo([]byte, net.Addr) (int, error)  { panic("unimplemented") }
func (*packetConnAddrMock) Close() error                           { panic("unimplemented") }
func (*packetConnAddrMock) SetDeadline(_ time.Time) error          { panic("unimplemented") }
func (*packetConnAddrMock) SetReadDeadline(_ time.Time) error      { panic("unimplemented") }
func (*packetConnAddrMock) SetWriteDeadline(_ time.Time) error     { panic("unimplemented") }

func TestPacketConnLocalAddrAndRemoteAddr(t *testing.T) {
    c := NewPacketConn(&packetConnAddrMock{})
    al := c.LocalAddr()

    if al.String() != "local_addr" {
        t.Error("Wrong LocalAddr implementation")
    }
}

func BenchmarkPacketConnBase(b *testing.B) {
    ca, cb := pipe()
    defer func() {
        _ = ca.Close()
    }()

    data := make([]byte, 4096)
    for i := range data {
        data[i] = byte(i)
    }
    buf := make([]byte, len(data))

    b.SetBytes(int64(len(data)))
    b.ResetTimer()

    go func(n int) {
        for i := 0; i < n; i++ {
            _, _ = cb.WriteTo(data, nil)
        }
        _ = cb.Close()
    }(b.N)

    count := 0
    for {
        n, _, err := ca.ReadFrom(buf)
        if err != nil {
            if !errors.Is(err, io.EOF) {
                b.Fatal(err)
            }
            break
        }
        if n != len(data) {
            b.Errorf("Expected %v, got %v", len(data), n)
        }
        count++
    }
    if count != b.N {
        b.Errorf("Expected %v, got %v", b.N, count)
    }
}

func BenchmarkWriteTo(b *testing.B) {
    ca, cb := pipe()
    defer func() {
        _ = ca.Close()
    }()

    data := make([]byte, 4096)
    for i := range data {
        data[i] = byte(i)
    }
    buf := make([]byte, len(data))

    b.SetBytes(int64(len(data)))
    b.ResetTimer()

    go func(n int) {
        c := NewPacketConn(cb)
        for i := 0; i < n; i++ {
            _, _ = c.WriteToContext(context.Background(), data, nil)
        }
        _ = cb.Close()
    }(b.N)

    count := 0
    for {
        n, _, err := ca.ReadFrom(buf)
        if err != nil {
            if !errors.Is(err, io.EOF) {
                b.Fatal(err)
            }
            break
        }
        if n != len(data) {
            b.Errorf("Expected %v, got %v", len(data), n)
        }
        count++
    }
    if count != b.N {
        b.Errorf("Expected %v, got %v", b.N, count)
    }
}

func BenchmarkReadFrom(b *testing.B) {
    ca, cb := pipe()
    defer func() {
        _ = ca.Close()
    }()

    data := make([]byte, 4096)
    for i := range data {
        data[i] = byte(i)
    }
    buf := make([]byte, len(data))

    b.SetBytes(int64(len(data)))
    b.ResetTimer()

    go func(n int) {
        for i := 0; i < n; i++ {
            _, _ = cb.WriteTo(data, nil)
        }
        _ = cb.Close()
    }(b.N)

    c := NewPacketConn(ca)
    count := 0
    for {
        n, _, err := c.ReadFromContext(context.Background(), buf)
        if err != nil {
            if !errors.Is(err, io.EOF) {
                b.Fatal(err)
            }
            break
        }
        if n != len(data) {
            b.Errorf("Expected %v, got %v", len(data), n)
        }
        count++
    }
    if count != b.N {
        b.Errorf("Expected %v, got %v", b.N, count)
    }
}
