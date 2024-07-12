// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package test

import (
    "bytes"
    "context"
    "errors"
    "fmt"
    "io"
    "sync"

    "github.com/pion/transport/v2/netctx"
)

var errByteSequenceChanged = errors.New("byte sequence changed")

// Options represents the configuration of the stress test
type Options struct {
    MsgSize  int
    MsgCount int
}

// Stress enables stress testing of a io.ReadWriter.
// It checks that packets are received correctly and in order.
func Stress(ca io.Writer, cb io.Reader, opt Options) error {
    return StressContext(context.Background(), &wrappedWriter{ca}, &wrappedReader{cb}, opt)
}

// StressContext enables stress testing of a io.ReadWriter.
// It checks that packets are received correctly and in order.
func StressContext(ctx context.Context, ca netctx.Writer, cb netctx.Reader, opt Options) error {
    bufs := make(chan []byte, opt.MsgCount)
    errCh := make(chan error)
    // Write
    go func() {
        err := write(ctx, ca, bufs, opt)
        errCh <- err
        close(bufs)
    }()

    // Read
    go func() {
        result := make([]byte, opt.MsgSize)

        for original := range bufs {
            err := read(ctx, cb, original, result)
            if err != nil {
                errCh <- err
            }
        }

        close(errCh)
    }()

    return FlattenErrs(GatherErrs(errCh))
}

func read(ctx context.Context, r netctx.Reader, original, result []byte) error {
    n, err := r.ReadContext(ctx, result)
    if err != nil {
        return err
    }
    if !bytes.Equal(original, result[:n]) {
        return fmt.Errorf("%w %#v != %#v", errByteSequenceChanged, original, result)
    }

    return nil
}

// StressDuplex enables duplex stress testing of a io.ReadWriter.
// It checks that packets are received correctly and in order.
func StressDuplex(ca io.ReadWriter, cb io.ReadWriter, opt Options) error {
    return StressDuplexContext(context.Background(), &wrappedReadWriter{ca}, &wrappedReadWriter{cb}, opt)
}

// StressDuplexContext enables duplex stress testing of a io.ReadWriter.
// It checks that packets are received correctly and in order.
func StressDuplexContext(ctx context.Context, ca netctx.ReadWriter, cb netctx.ReadWriter, opt Options) error {
    errCh := make(chan error)

    var wg sync.WaitGroup
    wg.Add(2)

    go func() {
        defer wg.Done()
        errCh <- StressContext(ctx, ca, cb, opt)
    }()

    go func() {
        defer wg.Done()
        errCh <- StressContext(ctx, cb, ca, opt)
    }()

    go func() {
        wg.Wait()
        close(errCh)
    }()

    return FlattenErrs(GatherErrs(errCh))
}

func write(ctx context.Context, c netctx.Writer, bufs chan []byte, opt Options) error {
    randomizer := initRand()
    for i := 0; i < opt.MsgCount; i++ {
        buf, err := randomizer.randBuf(opt.MsgSize)
        if err != nil {
            return err
        }
        bufs <- buf
        if _, err = c.WriteContext(ctx, buf); err != nil {
            return err
        }
    }
    return nil
}
