// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package vnet

import (
    "net"
    "testing"

    "github.com/pion/transport/v2"
    "github.com/stretchr/testify/assert"
)

type mockNIC struct {
    mockGetInterface   func(ifName string) (*transport.Interface, error)
    mockOnInboundChunk func(c Chunk)
    mockGetStaticIPs   func() []net.IP
    mockSetRouter      func(r *Router) error
}

func (n *mockNIC) getInterface(ifName string) (*transport.Interface, error) {
    return n.mockGetInterface(ifName)
}

func (n *mockNIC) onInboundChunk(c Chunk) {
    n.mockOnInboundChunk(c)
}

func (n *mockNIC) getStaticIPs() []net.IP {
    return n.mockGetStaticIPs()
}

func (n *mockNIC) setRouter(r *Router) error {
    return n.mockSetRouter(r)
}

func newMockNIC(t *testing.T) *mockNIC {
    return &mockNIC{
        mockGetInterface: func(string) (*transport.Interface, error) {
            assert.Fail(t, "unexpected call to mockGetInterface")
            return nil, nil
        },
        mockOnInboundChunk: func(Chunk) {
            assert.Fail(t, "unexpected call to mockOnInboundChunk")
        },
        mockGetStaticIPs: func() []net.IP {
            assert.Fail(t, "unexpected call to mockGetStaticIPs")
            return nil
        },
        mockSetRouter: func(*Router) error {
            assert.Fail(t, "unexpected call to mockSetRouter")
            return nil
        },
    }
}

func TestLossFilter(t *testing.T) {
    t.Run("FullLoss", func(t *testing.T) {
        mnic := newMockNIC(t)

        f, err := NewLossFilter(mnic, 100)
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        f.onInboundChunk(&chunkUDP{})
    })

    t.Run("NoLoss", func(t *testing.T) {
        mnic := newMockNIC(t)

        f, err := NewLossFilter(mnic, 0)
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        packets := 100
        received := 0
        mnic.mockOnInboundChunk = func(Chunk) {
            received++
        }

        for i := 0; i < packets; i++ {
            f.onInboundChunk(&chunkUDP{})
        }

        assert.Equal(t, packets, received)
    })

    t.Run("SomeLoss", func(t *testing.T) {
        mnic := newMockNIC(t)

        f, err := NewLossFilter(mnic, 50)
        if !assert.NoError(t, err, "should succeed") {
            return
        }

        packets := 1000
        received := 0
        mnic.mockOnInboundChunk = func(Chunk) {
            received++
        }

        for i := 0; i < packets; i++ {
            f.onInboundChunk(&chunkUDP{})
        }

        // One of the following could technically fail, but very unlikely
        assert.Less(t, 0, received)
        assert.Greater(t, packets, received)
    })
}
