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
)

////////////////////////////////////////////////////////////////////////////////

const authHeader = "authorization"
const authMethod = "Bearer"

type ClientCredentials struct {
	RootCertsFile      string
	CertFile           string
	CertPrivateKeyFile string
	AuthToken          string
	IAMClient          TokenProvider
}

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
	cfg := tls.Config{}

	if creds.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(creds.CertFile, creds.CertPrivateKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate/key: %s", err.Error())
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

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&cfg)),
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
