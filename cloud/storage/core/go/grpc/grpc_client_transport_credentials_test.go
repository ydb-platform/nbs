package grpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"
)

////////////////////////////////////////////////////////////////////////////////

type mutableTlsConfigProvider struct {
	mutex     sync.Mutex
	tlsConfig *tls.Config
	callCount int
}

func (p *mutableTlsConfigProvider) GetTlsConfig() *tls.Config {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.callCount++
	return p.tlsConfig
}

func (p *mutableTlsConfigProvider) setTlsConfig(config *tls.Config) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.tlsConfig = config
}

func (p *mutableTlsConfigProvider) getCallCount() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.callCount
}

func TestGrpcClientTransportCredentialsUsesLatestTlsConfigForEveryHandshake(
	t *testing.T,
) {

	firstCertificate, firstConfig := newServerCertificate(t, 1)
	secondCertificate, secondConfig := newServerCertificate(t, 2)

	provider := &mutableTlsConfigProvider{tlsConfig: firstConfig}
	transportCredentials := newGrpcClientTransportCredentials(t, provider)

	performTlsHandshake(
		t,
		transportCredentials,
		firstCertificate,
		"localhost:443",
	)
	provider.setTlsConfig(secondConfig)
	performTlsHandshake(
		t,
		transportCredentials,
		secondCertificate,
		"localhost:443",
	)

	if provider.getCallCount() != 2 {
		t.Fatalf(
			"expected the provider to be called twice, got %v",
			provider.getCallCount(),
		)
	}
}

func TestGrpcClientTransportCredentialsUsesServerNameOverride(t *testing.T) {
	serverCertificate, config := newServerCertificate(t, 1)
	provider := &mutableTlsConfigProvider{tlsConfig: config}
	transportCredentials := newGrpcClientTransportCredentials(
		t,
		provider,
	)

	if err := transportCredentials.OverrideServerName("localhost"); err != nil {
		t.Fatalf("failed to override server name: %v", err)
	}

	performTlsHandshake(
		t,
		transportCredentials,
		serverCertificate,
		"unexpected.example:443",
	)

	if config.ServerName != "" {
		t.Fatalf(
			"expected provider config to remain unchanged, got %q",
			config.ServerName,
		)
	}
}

func TestGrpcClientTransportCredentialsClonePreservesServerNameOverride(
	t *testing.T,
) {

	serverCertificate, config := newServerCertificate(t, 1)
	transportCredentials := newGrpcClientTransportCredentials(
		t,
		&mutableTlsConfigProvider{tlsConfig: config},
	)

	if err := transportCredentials.OverrideServerName("localhost"); err != nil {
		t.Fatalf("failed to override server name: %v", err)
	}

	performTlsHandshake(
		t,
		transportCredentials.Clone(),
		serverCertificate,
		"unexpected.example:443",
	)
}

func TestGrpcClientTransportCredentialsRejectsNilTlsConfig(t *testing.T) {
	transportCredentials := newGrpcClientTransportCredentials(
		t,
		&mutableTlsConfigProvider{},
	)

	_, _, err := transportCredentials.ClientHandshake(
		context.Background(),
		"localhost:443",
		nil,
	)
	if !errors.Is(err, errTlsConfigIsNil) {
		t.Fatalf("expected nil TLS config error, got %v", err)
	}
}

func TestNewGrpcClientTransportCredentialsRejectsNilProvider(t *testing.T) {
	var typedNilProvider *mutableTlsConfigProvider
	for _, provider := range []TlsConfigProvider{nil, typedNilProvider} {
		transportCredentials, err := NewGrpcClientTransportCredentials(provider)
		if !errors.Is(err, errTlsConfigProviderIsNil) {
			t.Fatalf("expected nil TLS config provider error, got %v", err)
		}

		if transportCredentials != nil {
			t.Fatal("expected nil transport credentials")
		}
	}
}

func newGrpcClientTransportCredentials(
	t *testing.T,
	provider TlsConfigProvider,
) credentials.TransportCredentials {

	t.Helper()

	transportCredentials, err := NewGrpcClientTransportCredentials(provider)
	if err != nil {
		t.Fatalf("failed to create transport credentials: %v", err)
	}

	return transportCredentials
}

func performTlsHandshake(
	t *testing.T,
	transportCredentials credentials.TransportCredentials,
	serverCertificate tls.Certificate,
	authority string,
) {

	t.Helper()

	clientConn, serverConn := net.Pipe()
	serverResult := make(chan error, 1)
	go func() {
		conn := tls.Server(serverConn, &tls.Config{
			Certificates: []tls.Certificate{serverCertificate},
			NextProtos:   []string{"h2"},
		})

		serverResult <- conn.Handshake()
		_ = conn.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := transportCredentials.ClientHandshake(
		ctx,
		authority,
		clientConn,
	)
	if err != nil {
		t.Fatalf("client TLS handshake failed: %v", err)
	}

	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		t.Fatalf("expected a TLS connection, got %T", conn)
	}

	protocol := tlsConn.ConnectionState().NegotiatedProtocol
	if protocol != "h2" {
		t.Fatalf("expected h2 to be negotiated, got %q", protocol)
	}

	_ = conn.Close()

	if err := <-serverResult; err != nil {
		t.Fatalf("server TLS handshake failed: %v", err)
	}
}

func newServerCertificate(
	t *testing.T,
	serialNumber int64,
) (tls.Certificate, *tls.Config) {

	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serialNumber),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		DNSNames:              []string{"localhost"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certificateDER, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		&privateKey.PublicKey,
		privateKey,
	)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certificate, err := x509.ParseCertificate(certificateDER)
	if err != nil {
		t.Fatalf("failed to parse certificate: %v", err)
	}

	serverCertificate := tls.Certificate{
		Certificate: [][]byte{certificateDER},
		PrivateKey:  privateKey,
		Leaf:        certificate,
	}

	rootCAs := x509.NewCertPool()
	rootCAs.AddCert(certificate)
	return serverCertificate, &tls.Config{RootCAs: rootCAs}
}
