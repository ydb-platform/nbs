package test

import (
    "io"
    "net"
    "testing"
)

func TestStressIOPipe(t *testing.T) {
    r, w := io.Pipe()

    opt := Options{
        MsgSize:  2048,
        MsgCount: 100,
    }

    err := Stress(w, r, opt)
    if err != nil {
        t.Fatal(err)
    }
}

func TestStressDuplexNetPipe(t *testing.T) {
    ca, cb := net.Pipe()

    opt := Options{
        MsgSize:  2048,
        MsgCount: 100,
    }

    err := StressDuplex(ca, cb, opt)
    if err != nil {
        t.Fatal(err)
    }
}

func BenchmarkPipe(b *testing.B) {
    ca, cb := net.Pipe()

    b.ResetTimer()

    opt := Options{
        MsgSize:  2048,
        MsgCount: b.N,
    }

    check(Stress(ca, cb, opt))
}

func check(err error) {
    if err != nil {
        panic(err)
    }
}
