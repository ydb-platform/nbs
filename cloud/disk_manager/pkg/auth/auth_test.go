package auth

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	grpc_peer "google.golang.org/grpc/peer"
)

func contextWithPeer(ctx context.Context, userIP string) context.Context {
	return grpc_peer.NewContext(ctx, &grpc_peer.Peer{
		Addr: &net.TCPAddr{
			IP:   net.ParseIP(userIP),
			Port: 1234,
		},
	})
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
			ctx:      contextWithPeer(context.Background(), "192.0.2.4"),
			expected: "192.0.2.4:1234",
		},
		{
			name:     "peer IPv6",
			ctx:      contextWithPeer(context.Background(), "2001:db8::1"),
			expected: "[2001:db8::1]:1234",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, GetUserIP(test.ctx))
		})
	}
}
