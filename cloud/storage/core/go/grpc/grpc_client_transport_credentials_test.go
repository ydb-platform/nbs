package grpc

import (
	"context"
	"crypto/tls"
	"testing"
)

////////////////////////////////////////////////////////////////////////////////

type countingTLSConfigProvider struct {
	callCount int
}

func (p *countingTLSConfigProvider) GetTLSConfig() *tls.Config {
	p.callCount++
	return nil
}

func TestShouldGetTLSConfigForEachClientHandshake(t *testing.T) {
	provider := &countingTLSConfigProvider{}
	credentials := NewGRPCClientTransportCredentials(provider)

	_, _, err := credentials.ClientHandshake(context.Background(), "", nil)
	if err == nil {
		t.Fatal("expected the first handshake to fail")
	}

	_, _, err = credentials.ClientHandshake(context.Background(), "", nil)
	if err == nil {
		t.Fatal("expected the second handshake to fail")
	}

	if provider.callCount != 2 {
		t.Fatalf("expected two TLS config requests, got %v", provider.callCount)
	}
}
