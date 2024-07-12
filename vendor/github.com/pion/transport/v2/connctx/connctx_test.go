// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package connctx

import (
    "bytes"
    "context"
    "errors"
    "io"
    "net"
    "testing"
    "time"
)

func TestRead(t *testing.T) {
    ca, cb := net.Pipe()
    defer func() {
        _ = ca.Close()
    }()

    data := []byte{0x01, 0x02, 0xFF}
    chErr := make(chan error)

    go func() {
        _, err := cb.Write(data)
        chErr <- err
    }()

    c := New(ca)
    b := make([]byte, 100)
    n, err := c.ReadContext(context.Background(), b)
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

func TestReadTimeout(t *testing.T) {
    ca, _ := net.Pipe()
    defer func() {
        _ = ca.Close()
    }()

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
    defer cancel()

    c := New(ca)
    b := make([]byte, 100)
    n, err := c.ReadContext(ctx, b)
    if err == nil {
        t.Error("Read unexpectedly succeeded")
    }
    if n != 0 {
        t.Errorf("Wrong data length, expected %d, got %d", 0, n)
    }
}

func TestReadCancel(t *testing.T) {
    ca, _ := net.Pipe()
    defer func() {
        _ = ca.Close()
    }()

    ctx, cancel := context.WithCancel(context.Background())
    go func() {
        time.Sleep(10 * time.Millisecond)
        cancel()
    }()

    c := New(ca)
    b := make([]byte, 100)
    n, err := c.ReadContext(ctx, b)
    if err == nil {
        t.Error("Read unexpectedly succeeded")
    }
    if n != 0 {
        t.Errorf("Wrong data length, expected %d, got %d", 0, n)
    }
}

func TestReadClosed(t *testing.T) {
    ca, _ := net.Pipe()

    c := New(ca)
    _ = c.Close()

    b := make([]byte, 100)
    n, err := c.ReadContext(context.Background(), b)
    if !errors.Is(err, io.EOF) {
        t.Errorf("Expected error '%v', got '%v'", io.EOF, err)
    }
    if n != 0 {
        t.Errorf("Wrong data length, expected %d, got %d", 0, n)
    }
}

func TestWrite(t *testing.T) {
    ca, cb := net.Pipe()
    defer func() {
        _ = ca.Close()
    }()

    chErr := make(chan error)
    chRead := make(chan []byte)

    go func() {
        b := make([]byte, 100)
        n, err := cb.Read(b)
        chErr <- err
        chRead <- b[:n]
    }()

    c := New(ca)
    data := []byte{0x01, 0x02, 0xFF}
    n, err := c.WriteContext(context.Background(), data)
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

func TestWriteTimeout(t *testing.T) {
    ca, _ := net.Pipe()
    defer func() {
        _ = ca.Close()
    }()

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
    defer cancel()

    c := New(ca)
    b := make([]byte, 100)
    n, err := c.WriteContext(ctx, b)
    if err == nil {
        t.Error("Write unexpectedly succeeded")
    }
    if n != 0 {
        t.Errorf("Wrong data length, expected %d, got %d", 0, n)
    }
}

func TestWriteCancel(t *testing.T) {
    ca, _ := net.Pipe()
    defer func() {
        _ = ca.Close()
    }()

    ctx, cancel := context.WithCancel(context.Background())
    go func() {
        time.Sleep(10 * time.Millisecond)
        cancel()
    }()

    c := New(ca)
    b := make([]byte, 100)
    n, err := c.WriteContext(ctx, b)
    if err == nil {
        t.Error("Write unexpectedly succeeded")
    }
    if n != 0 {
        t.Errorf("Wrong data length, expected %d, got %d", 0, n)
    }
}

func TestWriteClosed(t *testing.T) {
    ca, _ := net.Pipe()

    c := New(ca)
    _ = c.Close()

    b := make([]byte, 100)
    n, err := c.WriteContext(context.Background(), b)
    if !errors.Is(err, ErrClosing) {
        t.Errorf("Expected error '%v', got '%v'", ErrClosing, err)
    }
    if n != 0 {
        t.Errorf("Wrong data length, expected %d, got %d", 0, n)
    }
}

// Test for TestLocalAddrAndRemoteAddr
type stringAddr struct {
    network string
    addr    string
}

func (a stringAddr) Network() string { return a.network }
func (a stringAddr) String() string  { return a.addr }

type connAddrMock struct{}

func (*connAddrMock) RemoteAddr() net.Addr               { return stringAddr{"remote_net", "remote_addr"} }
func (*connAddrMock) LocalAddr() net.Addr                { return stringAddr{"local_net", "local_addr"} }
func (*connAddrMock) Read(_ []byte) (n int, err error)   { panic("unimplemented") }
func (*connAddrMock) Write(_ []byte) (n int, err error)  { panic("unimplemented") }
func (*connAddrMock) Close() error                       { panic("unimplemented") }
func (*connAddrMock) SetDeadline(_ time.Time) error      { panic("unimplemented") }
func (*connAddrMock) SetReadDeadline(_ time.Time) error  { panic("unimplemented") }
func (*connAddrMock) SetWriteDeadline(_ time.Time) error { panic("unimplemented") }

func TestLocalAddrAndRemoteAddr(t *testing.T) {
    c := New(&connAddrMock{})
    al := c.LocalAddr()
    ar := c.RemoteAddr()

    if al.String() != "local_addr" {
        t.Error("Wrong LocalAddr implementation")
    }
    if ar.String() != "remote_addr" {
        t.Error("Wrong RemoteAddr implementation")
    }
}

func BenchmarkBase(b *testing.B) {
    ca, cb := net.Pipe()
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
            _, _ = cb.Write(data)
        }
        _ = cb.Close()
    }(b.N)

    count := 0
    for {
        n, err := ca.Read(buf)
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

func BenchmarkWrite(b *testing.B) {
    ca, cb := net.Pipe()
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
        c := New(cb)
        for i := 0; i < n; i++ {
            _, _ = c.WriteContext(context.Background(), data)
        }
        _ = cb.Close()
    }(b.N)

    count := 0
    for {
        n, err := ca.Read(buf)
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

func BenchmarkRead(b *testing.B) {
    ca, cb := net.Pipe()
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
            _, _ = cb.Write(data)
        }
        _ = cb.Close()
    }(b.N)

    c := New(ca)
    count := 0
    for {
        n, err := c.ReadContext(context.Background(), buf)
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
