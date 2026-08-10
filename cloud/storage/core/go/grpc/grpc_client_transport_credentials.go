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
	GetTLSConfig(ctx context.Context) (*tls.Config, error)
}

type grpcClientTransportCredentials struct {
	provider TLSConfigProvider

	lock               sync.RWMutex
	serverNameOverride string
}

func NewGRPCClientTransportCredentials(
	provider TLSConfigProvider,
) credentials.TransportCredentials {
	return &grpcClientTransportCredentials{
		provider: provider,
	}
}

func (c *grpcClientTransportCredentials) ClientHandshake(
	ctx context.Context,
	authority string,
	rawConn net.Conn,
) (net.Conn, credentials.AuthInfo, error) {
	if c.provider == nil {
		return nil, nil, errors.New("nil TLS config provider")
	}

	cfg, err := c.provider.GetTLSConfig(ctx)
	if err != nil {
		return nil, nil, err
	}
	if cfg == nil {
		return nil, nil, errors.New("empty TLS config from provider")
	}

	cfg = cfg.Clone()

	c.lock.RLock()
	serverNameOverride := c.serverNameOverride
	c.lock.RUnlock()

	if serverNameOverride != "" {
		cfg.ServerName = serverNameOverride
	}

	return credentials.NewTLS(cfg).ClientHandshake(
		ctx,
		authority,
		rawConn,
	)
}

func (c *grpcClientTransportCredentials) ServerHandshake(
	rawConn net.Conn,
) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("server handshake is not supported")
}

func (c *grpcClientTransportCredentials) Info() credentials.ProtocolInfo {
	c.lock.RLock()
	serverNameOverride := c.serverNameOverride
	c.lock.RUnlock()

	cfg := &tls.Config{
		ServerName: serverNameOverride,
	}

	return credentials.NewTLS(cfg).Info()
}

func (c *grpcClientTransportCredentials) Clone() credentials.TransportCredentials {
	c.lock.RLock()
	serverNameOverride := c.serverNameOverride
	c.lock.RUnlock()

	return &grpcClientTransportCredentials{
		provider:           c.provider,
		serverNameOverride: serverNameOverride,
	}
}

func (c *grpcClientTransportCredentials) OverrideServerName(
	serverNameOverride string,
) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.serverNameOverride = serverNameOverride
	return nil
}
