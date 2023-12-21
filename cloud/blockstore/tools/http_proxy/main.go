package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	blockstore_config "a.yandex-team.ru/cloud/blockstore/config"
	blockstore_grpc "a.yandex-team.ru/cloud/blockstore/public/api/grpc"
)

////////////////////////////////////////////////////////////////////////////////

func parseConfig(
	configFileName string,
	config *blockstore_config.THttpProxyConfig,
) error {

	configBytes, err := ioutil.ReadFile(configFileName)
	if err != nil {
		return fmt.Errorf(
			"failed to read config file %v: %w",
			configFileName,
			err,
		)
	}

	err = proto.UnmarshalText(string(configBytes), config)
	if err != nil {
		return fmt.Errorf(
			"failed to parse config file %v as protobuf: %w",
			configFileName,
			err,
		)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func createTLSConfig(
	config *blockstore_config.THttpProxyConfig,
) (*tls.Config, error) {

	roots := x509.NewCertPool()
	if config.RootCertsFile != nil {
		rootCertsFileBytes, err := ioutil.ReadFile(*config.RootCertsFile)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to read root certs file %v: %w",
				config.RootCertsFile,
				err,
			)
		}

		ok := roots.AppendCertsFromPEM(rootCertsFileBytes)
		if !ok {
			return nil, fmt.Errorf("failed to parse root cert")
		}
	}

	certificates := make([]tls.Certificate, 0, len(config.Certs))
	for _, certFile := range config.Certs {
		cert, err := tls.LoadX509KeyPair(
			*certFile.CertFile,
			*certFile.CertPrivateKeyFile,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to load cert file %v: %w",
				*certFile.CertFile,
				err,
			)
		}

		certificates = append(certificates, cert)
	}

	cfg := &tls.Config{
		Certificates: certificates,
		RootCAs:      roots,
		MinVersion:   tls.VersionTLS12,
	}
	// TODO: https://golang.org/doc/go1.14#crypto/tls
	//nolint:SA1019
	cfg.BuildNameToCertificate()

	return cfg, nil
}

////////////////////////////////////////////////////////////////////////////////

func createTransportCredentials(
	config *blockstore_config.THttpProxyConfig,
) (credentials.TransportCredentials, error) {

	if *config.NbsServerInsecure {
		return nil, nil
	}

	if config.NbsServerCertFile != nil {
		return credentials.NewClientTLSFromFile(*config.NbsServerCertFile, "")
	}

	return credentials.NewClientTLSFromCert(nil, ""), nil
}

////////////////////////////////////////////////////////////////////////////////

func createEndpoint(
	nbsServerHost string,
	nbsServerPort uint32,
) (string, error) {

	if nbsServerHost == "localhost-expanded" {
		hostname, err := os.Hostname()
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%v:%v", hostname, nbsServerPort), nil
	}

	return fmt.Sprintf("%v:%v", nbsServerHost, nbsServerPort), nil
}

////////////////////////////////////////////////////////////////////////////////

func isNbsHeaderKey(key string) (string, bool) {
	key = strings.ToLower(key)
	if strings.HasPrefix(key, "x-nbs-") {
		return key, true
	}
	return key, false
}

////////////////////////////////////////////////////////////////////////////////

func createProxyMux(
	ctx context.Context,
	config *blockstore_config.THttpProxyConfig,
) (*runtime.ServeMux, error) {

	creds, err := createTransportCredentials(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport credentials: %w", err)
	}

	endpoint, err := createEndpoint(*config.NbsServerHost, *config.NbsServerPort)
	if err != nil {
		return nil, fmt.Errorf("failed to create endpoint: %w", err)
	}
	if creds == nil {
		log.Printf("Forwarding to (insecure) %v", endpoint)
	} else {
		log.Printf("Forwarding to (secure) %v", endpoint)
	}

	mux := runtime.NewServeMux(
		runtime.WithIncomingHeaderMatcher(func(key string) (string, bool) {
			if key, ok := isNbsHeaderKey(key); ok {
				return key, true
			}
			return runtime.DefaultHeaderMatcher(key)
		}),
	)
	var transportDialOption grpc.DialOption
	if creds == nil {
		transportDialOption = grpc.WithInsecure()
	} else {
		transportDialOption = grpc.WithTransportCredentials(creds)
	}
	err = blockstore_grpc.RegisterTBlockStoreServiceHandlerFromEndpoint(
		ctx,
		mux,
		endpoint,
		[]grpc.DialOption{
			transportDialOption,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register TBlockStoreService handler: %w", err)
	}

	return mux, nil
}

type httpProxyServer struct {
	addr string
	wait func()
	kill func()
}

func runSecureServer(
	mux *runtime.ServeMux,
	config *blockstore_config.THttpProxyConfig,
) (*httpProxyServer, error) {

	addr := fmt.Sprintf(":%v", *config.SecurePort)

	tlsConfig, err := createTLSConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create TLS config: %w", err)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on address %v: %w", addr, err)
	}
	addr = lis.Addr().String()
	log.Printf("Listening on (secure) %v", addr)

	srv := &http.Server{
		Handler:   mux,
		TLSConfig: tlsConfig,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Use certificates from srv.
		err := srv.ServeTLS(lis, "", "")
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Error serving HTTP: %v", err)
		}
	}()

	return &httpProxyServer{
		addr: addr,
		wait: wg.Wait,
		kill: func() {
			err := srv.Shutdown(context.Background())
			if err != nil {
				log.Fatalf("Error shutting down HTTP: %v", err)
			}
		},
	}, nil
}

func runInsecureServer(
	mux *runtime.ServeMux,
	config *blockstore_config.THttpProxyConfig,
) (*httpProxyServer, error) {

	addr := fmt.Sprintf(":%v", *config.Port)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on address %v: %w", addr, err)
	}
	addr = lis.Addr().String()
	log.Printf("Listening on (insecure) %v", addr)

	srv := &http.Server{
		Handler: mux,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := srv.Serve(lis)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Error serving HTTP: %v", err)
		}
	}()

	return &httpProxyServer{
		addr: addr,
		wait: wg.Wait,
		kill: func() {
			err := srv.Shutdown(context.Background())
			if err != nil {
				log.Fatalf("Error shutting down HTTP: %v", err)
			}
		},
	}, nil
}

func createAndRunServers(
	ctx context.Context,
	config *blockstore_config.THttpProxyConfig,
) (func(), error) {

	mux, err := createProxyMux(ctx, config)
	if err != nil {
		return nil, err
	}

	var insecureServer *httpProxyServer
	if config.Port != nil {
		insecureServer, err = runInsecureServer(mux, config)
		if err != nil {
			return nil, err
		}
	}

	var secureServer *httpProxyServer
	if config.SecurePort != nil {
		secureServer, err = runSecureServer(mux, config)
		if err != nil {
			return nil, err
		}
	}

	return func() {
		if insecureServer != nil {
			insecureServer.wait()
		}
		if secureServer != nil {
			secureServer.wait()
		}
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	log.Println("blockstore-http-proxy launched")

	var configFileName string
	config := &blockstore_config.THttpProxyConfig{}

	var rootCmd = &cobra.Command{
		Use:   "blockstore-http-proxy",
		Short: "HTTP proxy for NBS server",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return parseConfig(configFileName, config)
		},
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancelCtx := context.WithCancel(context.Background())
			defer cancelCtx()

			wait, err := createAndRunServers(ctx, config)
			if err != nil {
				log.Fatalf("Error: %v", err)
			}
			wait()
		},
	}
	rootCmd.Flags().StringVar(
		&configFileName,
		"config",
		"/Berkanavt/nbs-server/cfg/nbs-http-proxy.txt",
		"Path to the config file",
	)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
