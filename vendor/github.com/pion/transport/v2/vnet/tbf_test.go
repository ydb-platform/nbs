// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

//go:build !wasm
// +build !wasm

package vnet

import (
    "context"
    "sync"
    "testing"
    "time"

    "github.com/pion/logging"
    "github.com/stretchr/testify/assert"
)

func TestTokenBucketFilter(t *testing.T) {
    t.Run("bitrateBelowCapacity", func(t *testing.T) {
        mnic := newMockNIC(t)

        tbf, err := NewTokenBucketFilter(mnic, TBFRate(10*MBit), TBFMaxBurst(10*MBit))
        assert.NoError(t, err, "should succeed")

        received := 0
        mnic.mockOnInboundChunk = func(Chunk) {
            received++
        }

        time.Sleep(1 * time.Second)

        sent := 100
        for i := 0; i < sent; i++ {
            tbf.onInboundChunk(&chunkUDP{
                userData: make([]byte, 1200),
            })
        }

        assert.NoError(t, tbf.Close())

        assert.Equal(t, sent, received)
    })

    subTest := func(t *testing.T, capacity int, maxBurst int, duration time.Duration) {
        log := logging.NewDefaultLoggerFactory().NewLogger("test")

        mnic := newMockNIC(t)

        tbf, err := NewTokenBucketFilter(mnic, TBFRate(capacity), TBFMaxBurst(maxBurst))
        assert.NoError(t, err, "should succeed")

        chunkChan := make(chan Chunk)
        mnic.mockOnInboundChunk = func(c Chunk) {
            chunkChan <- c
        }

        var wg sync.WaitGroup
        wg.Add(1)

        ctx, cancel := context.WithCancel(context.Background())

        go func() {
            defer wg.Done()

            totalBytesReceived := 0
            totalPacketsReceived := 0
            bytesReceived := 0
            packetsReceived := 0
            start := time.Now()
            last := time.Now()

            ticker := time.NewTicker(time.Second)
            defer ticker.Stop()

            for {
                select {
                case <-ctx.Done():
                    bits := float64(totalBytesReceived) * 8.0
                    rate := bits / time.Since(start).Seconds()
                    mBitPerSecond := rate / float64(MBit)
                    // Allow 5% more than capacity due to max bursts
                    assert.Less(t, rate, 1.05*float64(capacity))
                    assert.Greater(t, rate, 0.9*float64(capacity))

                    log.Infof("duration=%v, bytesReceived=%v, packetsReceived=%v throughput=%.2f Mb/s", time.Since(start), bytesReceived, packetsReceived, mBitPerSecond)
                    return

                case now := <-ticker.C:
                    delta := now.Sub(last)
                    last = now
                    bits := float64(bytesReceived) * 8.0
                    rate := bits / delta.Seconds()
                    mBitPerSecond := rate / float64(MBit)
                    log.Infof("duration=%v, bytesReceived=%v, packetsReceived=%v throughput=%.2f Mb/s", delta, bytesReceived, packetsReceived, mBitPerSecond)
                    // Allow 10% more than capacity due to max bursts
                    assert.Less(t, rate, 1.10*float64(capacity))
                    assert.Greater(t, rate, 0.9*float64(capacity))
                    bytesReceived = 0
                    packetsReceived = 0

                case c := <-chunkChan:
                    bytesReceived += len(c.UserData())
                    packetsReceived++
                    totalBytesReceived += len(c.UserData())
                    totalPacketsReceived++
                }
            }
        }()

        go func() {
            defer cancel()
            bytesSent := 0
            packetsSent := 0
            var start time.Time
            for start = time.Now(); time.Since(start) < duration; {
                c := &chunkUDP{
                    userData: make([]byte, 1200),
                }
                tbf.onInboundChunk(c)
                bytesSent += len(c.UserData())
                packetsSent++
                time.Sleep(1 * time.Millisecond)
            }
            bits := float64(bytesSent) * 8.0
            rate := bits / time.Since(start).Seconds()
            mBitPerSecond := rate / float64(MBit)
            log.Infof("duration=%v, bytesSent=%v, packetsSent=%v throughput=%.2f Mb/s", time.Since(start), bytesSent, packetsSent, mBitPerSecond)

            assert.NoError(t, tbf.Close())
        }()

        wg.Wait()
    }

    t.Run("500Kbit-s", func(t *testing.T) {
        subTest(t, 500*KBit, 10*KBit, 10*time.Second)
    })

    t.Run("1Mbit-s", func(t *testing.T) {
        subTest(t, 1*MBit, 20*KBit, 10*time.Second)
    })

    t.Run("2Mbit-s", func(t *testing.T) {
        subTest(t, 2*MBit, 40*KBit, 10*time.Second)
    })

    t.Run("8Mbit-s", func(t *testing.T) {
        subTest(t, 8*MBit, 160*KBit, 10*time.Second)
    })
}
