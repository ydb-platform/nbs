package auth

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	grpc_peer "google.golang.org/grpc/peer"
)

type stringAddr string

func (a stringAddr) Network() string {
	return "tcp"
}

func (a stringAddr) String() string {
	return string(a)
}

func contextWithAddr(ctx context.Context, addr net.Addr) context.Context {
	return grpc_peer.NewContext(ctx, &grpc_peer.Peer{Addr: addr})
}

func contextWithPeer(ctx context.Context, userIP string) context.Context {
	return contextWithAddr(
		ctx,
		&net.TCPAddr{
			IP:   net.ParseIP(userIP),
			Port: 1234,
		},
	)
}

func TestGetUserIP(t *testing.T) {
	tests := []struct {
		name     string
		ctx      context.Context
		expected string
	}{
		{
			name:     "no user IP",
			ctx:      context.Background(),
			expected: "",
		},
		{
			name:     "peer IPv4",
			ctx:      contextWithPeer(context.Background(), "192.168.0.1"),
			expected: "192.168.0.1",
		},
		{
			name: "peer IPv6",
			ctx: contextWithPeer(
				context.Background(),
				"2a02:6b8:bf00:3038:5058:7ff:fe8d:e4ca",
			),
			expected: "2a02:6b8:bf00:3038:5058:7ff:fe8d:e4ca",
		},
		{
			name: "multiple addresses",
			ctx: contextWithAddr(
				context.Background(),
				stringAddr("192.168.0.1:1234,192.168.0.2:1234"),
			),
			expected: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, GetUserIP(test.ctx))
		})
	}
}
