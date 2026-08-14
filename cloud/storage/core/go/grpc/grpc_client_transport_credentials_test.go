package grpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"
)

////////////////////////////////////////////////////////////////////////////////

type mutableTLSConfigProvider struct {
	mutex     sync.Mutex
	tlsConfig *tls.Config
	callCount int
}

func (p *mutableTLSConfigProvider) GetTLSConfig() *tls.Config {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.callCount++
	return p.tlsConfig
}

func (p *mutableTLSConfigProvider) setTLSConfig(config *tls.Config) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.tlsConfig = config
}

func (p *mutableTLSConfigProvider) getCallCount() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.callCount
}

func TestGRPCClientTransportCredentialsUsesLatestTLSConfigForEveryHandshake(
	t *testing.T,
) {
	firstCertificate, firstConfig := newServerCertificate(t, 1)
	secondCertificate, secondConfig := newServerCertificate(t, 2)

	provider := &mutableTLSConfigProvider{tlsConfig: firstConfig}
	transportCredentials := NewGRPCClientTransportCredentials(provider)

	performTLSHandshake(t, transportCredentials, firstCertificate)
	provider.setTLSConfig(secondConfig)
	performTLSHandshake(t, transportCredentials, secondCertificate)

	if provider.getCallCount() != 2 {
		t.Fatalf(
			"expected the provider to be called twice, got %v",
			provider.getCallCount(),
		)
	}
}

func performTLSHandshake(
	t *testing.T,
	transportCredentials credentials.TransportCredentials,
	serverCertificate tls.Certificate,
) {
	t.Helper()

	clientConn, serverConn := net.Pipe()
	serverResult := make(chan error, 1)
	go func() {
		conn := tls.Server(serverConn, &tls.Config{
			Certificates: []tls.Certificate{serverCertificate},
		})
		serverResult <- conn.Handshake()
		_ = conn.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := transportCredentials.ClientHandshake(
		ctx,
		"localhost:443",
		clientConn,
	)
	if err != nil {
		t.Fatalf("client TLS handshake failed: %v", err)
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
