package grpc

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"reflect"
	"sync"

	creds "google.golang.org/grpc/credentials"
)

////////////////////////////////////////////////////////////////////////////////

var errTLSConfigProviderIsNil = errors.New("TLS config provider is nil")

var errTLSConfigIsNil = errors.New(
	"TLS config provider returned nil config",
)

////////////////////////////////////////////////////////////////////////////////

type TLSConfigProvider interface {
	// GetTLSConfig returns the current TLS config, or nil if it is unavailable.
	// The returned config must not be modified.
	GetTLSConfig() *tls.Config
}

type grpcClientTransportCredentials struct {
	provider TLSConfigProvider

	mutex              sync.RWMutex
	serverNameOverride string
}

func NewGRPCClientTransportCredentials(
	provider TLSConfigProvider,
) (creds.TransportCredentials, error) {

	if isNilTLSConfigProvider(provider) {
		return nil, errTLSConfigProviderIsNil
	}

	return &grpcClientTransportCredentials{provider: provider}, nil
}

func (c *grpcClientTransportCredentials) ClientHandshake(
	ctx context.Context,
	authority string,
	rawConn net.Conn,
) (net.Conn, creds.AuthInfo, error) {

	config := c.provider.GetTLSConfig()
	if config == nil {
		return nil, nil, errTLSConfigIsNil
	}

	config = config.Clone()

	c.mutex.RLock()
	serverNameOverride := c.serverNameOverride
	c.mutex.RUnlock()
	if serverNameOverride != "" {
		config.ServerName = serverNameOverride
	}

	return creds.NewTLS(config).ClientHandshake(ctx, authority, rawConn)
}

func isNilTLSConfigProvider(provider TLSConfigProvider) bool {
	if provider == nil {
		return true
	}

	value := reflect.ValueOf(provider)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (c *grpcClientTransportCredentials) ServerHandshake(
	net.Conn,
) (net.Conn, creds.AuthInfo, error) {

	return nil, nil, errors.New("server handshake is not supported")
}

func (c *grpcClientTransportCredentials) Info() creds.ProtocolInfo {
	c.mutex.RLock()
	serverNameOverride := c.serverNameOverride
	c.mutex.RUnlock()

	return creds.ProtocolInfo{
		SecurityProtocol: "tls",
		SecurityVersion:  "1.2",
		ServerName:       serverNameOverride,
	}
}

func (c *grpcClientTransportCredentials) Clone() creds.TransportCredentials {

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
