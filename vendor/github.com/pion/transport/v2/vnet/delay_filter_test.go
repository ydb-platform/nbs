// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package vnet

import (
    "context"
    "testing"
    "time"

    "github.com/stretchr/testify/assert"
)

func TestDelayFilter(t *testing.T) {
    t.Run("schedulesOnePacketAtATime", func(t *testing.T) {
        nic := newMockNIC(t)
        df, err := NewDelayFilter(nic, 10*time.Millisecond)
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        ctx, cancel := context.WithCancel(context.Background())
        defer cancel()
        go df.Run(ctx)

        type TimestampedChunk struct {
            ts time.Time
            c  Chunk
        }
        receiveCh := make(chan TimestampedChunk)
        nic.mockOnInboundChunk = func(c Chunk) {
            receivedAt := time.Now()
            receiveCh <- TimestampedChunk{
                ts: receivedAt,
                c:  c,
            }
        }

        lastNr := -1
        for i := 0; i < 100; i++ {
            sent := time.Now()
            df.onInboundChunk(&chunkUDP{
                chunkIP:  chunkIP{timestamp: sent},
                userData: []byte{byte(i)},
            })

            select {
            case c := <-receiveCh:
                nr := int(c.c.UserData()[0])

                assert.Greater(t, nr, lastNr)
                lastNr = nr

                assert.Greater(t, c.ts.Sub(sent), 10*time.Millisecond)
            case <-time.After(time.Second):
                assert.Fail(t, "expected to receive next chunk")
            }
        }
    })

    t.Run("schedulesSubsequentManyPackets", func(t *testing.T) {
        nic := newMockNIC(t)
        df, err := NewDelayFilter(nic, 10*time.Millisecond)
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        ctx, cancel := context.WithCancel(context.Background())
        defer cancel()
        go df.Run(ctx)

        type TimestampedChunk struct {
            ts time.Time
            c  Chunk
        }
        receiveCh := make(chan TimestampedChunk)
        nic.mockOnInboundChunk = func(c Chunk) {
            receivedAt := time.Now()
            receiveCh <- TimestampedChunk{
                ts: receivedAt,
                c:  c,
            }
        }

        // schedule 100 chunks
        sent := time.Now()
        for i := 0; i < 100; i++ {
            df.onInboundChunk(&chunkUDP{
                chunkIP:  chunkIP{timestamp: sent},
                userData: []byte{byte(i)},
            })
        }

        // receive 100 chunks with delay>10ms
        for i := 0; i < 100; i++ {
            select {
            case c := <-receiveCh:
                nr := int(c.c.UserData()[0])
                assert.Equal(t, i, nr)
                assert.Greater(t, c.ts.Sub(sent), 10*time.Millisecond)
            case <-time.After(time.Second):
                assert.Fail(t, "expected to receive next chunk")
            }
        }
    })
}
