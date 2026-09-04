package auth

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	grpc_metadata "google.golang.org/grpc/metadata"
	grpc_peer "google.golang.org/grpc/peer"
)

func contextWithMetadata(ctx context.Context, values ...string) context.Context {
	return grpc_metadata.NewIncomingContext(ctx, grpc_metadata.Pairs(values...))
}

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
			name: "x-user-ip has highest priority",
			ctx: contextWithMetadata(
				contextWithPeer(context.Background(), "192.0.2.4"),
				HeaderUserIP, "192.0.2.1",
				HeaderRealIP, "192.0.2.2",
				HeaderForwardedFor, "192.0.2.3, 192.0.2.5",
			),
			expected: "192.0.2.1",
		},
		{
			name: "x-real-ip overrides x-forwarded-for and peer",
			ctx: contextWithMetadata(
				contextWithPeer(context.Background(), "192.0.2.4"),
				HeaderRealIP, "192.0.2.2",
				HeaderForwardedFor, "192.0.2.3, 192.0.2.5",
			),
			expected: "192.0.2.2",
		},
		{
			name: "first x-forwarded-for address overrides peer",
			ctx: contextWithMetadata(
				contextWithPeer(context.Background(), "192.0.2.4"),
				HeaderForwardedFor, " 192.0.2.3, 192.0.2.5",
			),
			expected: "192.0.2.3",
		},
		{
			name:     "peer IPv4 fallback",
			ctx:      contextWithPeer(context.Background(), "192.0.2.4"),
			expected: "192.0.2.4",
		},
		{
			name:     "peer IPv6 fallback",
			ctx:      contextWithPeer(context.Background(), "2001:db8::1"),
			expected: "2001:db8::1",
		},
		{
			name: "empty headers fall back to peer",
			ctx: contextWithMetadata(
				contextWithPeer(context.Background(), "192.0.2.4"),
				HeaderUserIP, "",
				HeaderRealIP, "   ",
				HeaderForwardedFor, "",
			),
			expected: "192.0.2.4",
		},
		{
			name: "invalid header falls back to peer",
			ctx: contextWithMetadata(
				contextWithPeer(context.Background(), "192.0.2.4"),
				HeaderUserIP, "not-an-ip",
			),
			expected: "192.0.2.4",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, GetUserIP(test.ctx))
		})
	}
}
