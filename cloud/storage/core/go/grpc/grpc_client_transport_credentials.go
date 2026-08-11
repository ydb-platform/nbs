package grpc

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"

	"google.golang.org/grpc/credentials"
)

////////////////////////////////////////////////////////////////////////////////

type TLSConfigProvider interface {
	GetTLSConfig() *tls.Config
}

type grpcClientTransportCredentials struct {
	provider TLSConfigProvider

	mutex              sync.RWMutex
	serverNameOverride string
}

func NewGRPCClientTransportCredentials(
	provider TLSConfigProvider,
) credentials.TransportCredentials {
	return &grpcClientTransportCredentials{provider: provider}
}

func (c *grpcClientTransportCredentials) ClientHandshake(
	ctx context.Context,
	authority string,
	rawConn net.Conn,
) (net.Conn, credentials.AuthInfo, error) {
	if c.provider == nil {
		return nil, nil, errors.New("TLS config provider is nil")
	}

	config := c.provider.GetTLSConfig()
	if config == nil {
		return nil, nil, errors.New("TLS config provider returned nil config")
	}
	config = config.Clone()

	c.mutex.RLock()
	serverNameOverride := c.serverNameOverride
	c.mutex.RUnlock()
	if serverNameOverride != "" {
		config.ServerName = serverNameOverride
	}

	return credentials.NewTLS(config).ClientHandshake(ctx, authority, rawConn)
}

func (c *grpcClientTransportCredentials) ServerHandshake(
	net.Conn,
) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("server handshake is not supported")
}

func (c *grpcClientTransportCredentials) Info() credentials.ProtocolInfo {
	c.mutex.RLock()
	serverNameOverride := c.serverNameOverride
	c.mutex.RUnlock()

	return credentials.NewTLS(&tls.Config{
		ServerName: serverNameOverride,
	}).Info()
}

func (c *grpcClientTransportCredentials) Clone() credentials.TransportCredentials {
	c.mutex.RLock()
	serverNameOverride := c.serverNameOverride
	c.mutex.RUnlock()

	return &grpcClientTransportCredentials{
		provider:           c.provider,
		serverNameOverride: serverNameOverride,
	}
}

func (c *grpcClientTransportCredentials) OverrideServerName(
	serverNameOverride string,
) error {
	c.mutex.Lock()
	c.serverNameOverride = serverNameOverride
	c.mutex.Unlock()
	return nil
}
