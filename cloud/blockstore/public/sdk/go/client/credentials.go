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
	grpc_codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	grpc_metadata "google.golang.org/grpc/metadata"
	grpc_status "google.golang.org/grpc/status"
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

func (creds *ClientCredentials) GetSslChannelCredentials() ([]grpc.DialOption, error) {
	cfg := tls.Config{}

	if creds.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(creds.CertFile, creds.CertPrivateKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate/key: %w", err)
		}

		cfg.Certificates = []tls.Certificate{cert}
	}

	if creds.RootCertsFile != "" {
		pem, err := os.ReadFile(creds.RootCertsFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read root cert file: %w", err)
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

	tokenProvider := creds.IAMClient

	if tokenProvider != nil {
		interceptor := func(
			ctx context.Context,
			method string, req,
			reply interface{},
			cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker,
			opts ...grpc.CallOption,
		) error {

			token, err := tokenProvider.Token(ctx)
			if err != nil {
				// NBS-2396
				return grpc_status.Error(grpc_codes.Unauthenticated, err.Error())
			}

			ctx = grpc_metadata.AppendToOutgoingContext(ctx, authHeader, authMethod+" "+token)
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		opts = append(opts, grpc.WithChainUnaryInterceptor(interceptor))
	}

	return opts, nil
}
