// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package deadline

import (
    "bytes"
    "context"
    "errors"
    "testing"
    "time"
)

func TestDeadline(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    t.Run("Deadline", func(t *testing.T) {
        now := time.Now()

        ctx0, cancel0 := context.WithDeadline(ctx, now.Add(40*time.Millisecond))
        defer cancel0()
        ctx1, cancel1 := context.WithDeadline(ctx, now.Add(60*time.Millisecond))
        defer cancel1()
        d := New()
        d.Set(now.Add(50 * time.Millisecond))

        ch := make(chan byte)
        go sendOnDone(ctx, ctx0.Done(), ch, 0)
        go sendOnDone(ctx, ctx1.Done(), ch, 1)
        go sendOnDone(ctx, d.Done(), ch, 2)

        calls := collectCh(ch, 3, 100*time.Millisecond)
        expectedCalls := []byte{0, 2, 1}
        if !bytes.Equal(calls, expectedCalls) {
            t.Errorf("Wrong order of deadline signal, expected: %v, got: %v", expectedCalls, calls)
        }
    })

    t.Run("DeadlineExtend", func(t *testing.T) { //nolint:dupl
        now := time.Now()

        ctx0, cancel0 := context.WithDeadline(ctx, now.Add(40*time.Millisecond))
        defer cancel0()
        ctx1, cancel1 := context.WithDeadline(ctx, now.Add(60*time.Millisecond))
        defer cancel1()
        d := New()
        d.Set(now.Add(50 * time.Millisecond))
        d.Set(now.Add(70 * time.Millisecond))

        ch := make(chan byte)
        go sendOnDone(ctx, ctx0.Done(), ch, 0)
        go sendOnDone(ctx, ctx1.Done(), ch, 1)
        go sendOnDone(ctx, d.Done(), ch, 2)

        calls := collectCh(ch, 3, 100*time.Millisecond)
        expectedCalls := []byte{0, 1, 2}
        if !bytes.Equal(calls, expectedCalls) {
            t.Errorf("Wrong order of deadline signal, expected: %v, got: %v", expectedCalls, calls)
        }
    })

    t.Run("DeadlinePretend", func(t *testing.T) { //nolint:dupl
        now := time.Now()

        ctx0, cancel0 := context.WithDeadline(ctx, now.Add(40*time.Millisecond))
        defer cancel0()
        ctx1, cancel1 := context.WithDeadline(ctx, now.Add(60*time.Millisecond))
        defer cancel1()
        d := New()
        d.Set(now.Add(50 * time.Millisecond))
        d.Set(now.Add(30 * time.Millisecond))

        ch := make(chan byte)
        go sendOnDone(ctx, ctx0.Done(), ch, 0)
        go sendOnDone(ctx, ctx1.Done(), ch, 1)
        go sendOnDone(ctx, d.Done(), ch, 2)

        calls := collectCh(ch, 3, 100*time.Millisecond)
        expectedCalls := []byte{2, 0, 1}
        if !bytes.Equal(calls, expectedCalls) {
            t.Errorf("Wrong order of deadline signal, expected: %v, got: %v", expectedCalls, calls)
        }
    })

    t.Run("DeadlineCancel", func(t *testing.T) {
        now := time.Now()

        ctx0, cancel0 := context.WithDeadline(ctx, now.Add(40*time.Millisecond))
        defer cancel0()
        d := New()
        d.Set(now.Add(50 * time.Millisecond))
        d.Set(time.Time{})

        ch := make(chan byte)
        go sendOnDone(ctx, ctx0.Done(), ch, 0)
        go sendOnDone(ctx, d.Done(), ch, 1)

        calls := collectCh(ch, 2, 60*time.Millisecond)
        expectedCalls := []byte{0}
        if !bytes.Equal(calls, expectedCalls) {
            t.Errorf("Wrong order of deadline signal, expected: %v, got: %v", expectedCalls, calls)
        }
    })
}

func sendOnDone(ctx context.Context, done <-chan struct{}, dest chan byte, val byte) {
    select {
    case <-done:
    case <-ctx.Done():
        return
    }
    dest <- val
}

func collectCh(ch <-chan byte, n int, timeout time.Duration) []byte {
    a := time.After(timeout)
    var calls []byte
    for len(calls) < n {
        select {
        case call := <-ch:
            calls = append(calls, call)
        case <-a:
            return calls
        }
    }
    return calls
}

func TestContext(t *testing.T) {
    t.Run("Cancel", func(t *testing.T) {
        d := New()
        select {
        case <-d.Done():
            t.Fatal("Deadline unexpectedly done")
        case <-time.After(50 * time.Millisecond):
        }
        if err := d.Err(); err != nil {
            t.Errorf("Wrong Err(), expected: nil, got: %v", err)
        }
        d.Set(time.Unix(0, 1)) // exceeded
        select {
        case <-d.Done():
        case <-time.After(50 * time.Millisecond):
            t.Fatal("Timeout")
        }
        if err := d.Err(); !errors.Is(err, context.DeadlineExceeded) {
            t.Errorf("Wrong Err(), expected: %v, got: %v", context.DeadlineExceeded, err)
        }
    })
    t.Run("Deadline", func(t *testing.T) {
        d := New()
        t0, expired0 := d.Deadline()
        if !t0.IsZero() {
            t.Errorf("Initial Deadline is expected to be 0, got %v", t0)
        }
        if expired0 {
            t.Error("Deadline is not expected to be expired at initial state")
        }

        dl := time.Unix(12345, 0)
        d.Set(dl) // exceeded

        t1, expired1 := d.Deadline()
        if !t1.Equal(dl) {
            t.Errorf("Initial Deadline is expected to be %v, got %v", dl, t1)
        }
        if !expired1 {
            t.Error("Deadline is expected to be expired")
        }
    })
}
