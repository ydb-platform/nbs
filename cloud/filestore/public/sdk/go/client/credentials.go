package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"

	storage_grpc "github.com/ydb-platform/nbs/cloud/storage/core/go/grpc"
)

////////////////////////////////////////////////////////////////////////////////

const authHeader = "authorization"
const authMethod = "Bearer"

type ClientCredentials struct {
	RootCertsFile      string
	CertFile           string
	CertPrivateKeyFile string
	// TlsProvider, when set, takes precedence over RootCertsFile,
	// CertFile, and CertPrivateKeyFile.
	TlsProvider TlsConfigProvider
	AuthToken   string
	IAMClient   TokenProvider
}

type TlsConfigProvider = storage_grpc.TlsConfigProvider

type TokenProvider interface {
	Token(ctx context.Context) (string, error)
}

type grpcTokenProvider struct {
	provider TokenProvider
}

func (p *grpcTokenProvider) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	token, err := p.provider.Token(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]string{authHeader: authMethod + " " + token}, nil
}

func (p *grpcTokenProvider) RequireTransportSecurity() bool {
	return false
}

func (creds *ClientCredentials) GetSslChannelCredentials() ([]grpc.DialOption, error) {
	var transportCredentials credentials.TransportCredentials
	// A typed-nil TlsProvider is a configuration error. Let the constructor
	// reject it instead of silently falling back to the certificate files.
	if creds.TlsProvider != nil {
		var err error
		transportCredentials, err = storage_grpc.NewGrpcClientTransportCredentials(
			creds.TlsProvider,
		)
		if err != nil {
			return nil, err
		}
	} else {
		cfg := &tls.Config{}
		if creds.CertFile != "" {
			cert, err := tls.LoadX509KeyPair(creds.CertFile, creds.CertPrivateKeyFile)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to load client certificate/key: %s",
					err.Error(),
				)
			}

			cfg.Certificates = []tls.Certificate{cert}
		}

		if creds.RootCertsFile != "" {
			pem, err := os.ReadFile(creds.RootCertsFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read root cert file: %s", err.Error())
			}

			pool := x509.NewCertPool()
			ok := pool.AppendCertsFromPEM(pem)
			if !ok {
				return nil, errors.New("failed to parse PEM")
			}

			cfg.RootCAs = pool
		}

		transportCredentials = credentials.NewTLS(cfg)
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCredentials),
	}

	if creds.AuthToken != "" {
		token := oauth2.Token{AccessToken: creds.AuthToken}
		opts = append(opts, grpc.WithPerRPCCredentials(oauth.NewOauthAccess(&token)))
	}

	if creds.IAMClient != nil {
		opts = append(opts, grpc.WithPerRPCCredentials(&grpcTokenProvider{provider: creds.IAMClient}))
	}

	return opts, nil
}
