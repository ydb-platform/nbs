package common

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metrics_mocks "github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
	"github.com/ydb-platform/nbs/contrib/go/cityhash"
)

////////////////////////////////////////////////////////////////////////////////

func TestGrpcClientTlsProviderLoadsConfigAndReportsFingerprint(t *testing.T) {
	certPEM, _ := generateCertificate(t, "root", time.Now().Add(24*time.Hour))
	certPath := filepath.Join(t.TempDir(), "root.pem")
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))

	registry := metrics_mocks.NewRegistryMock()
	fingerprint := cityhash.Hash64(certPEM) & ((1 << 53) - 1)
	registry.GetGauge(
		"Fingerprint",
		map[string]string{
			"subsystem": "certificates",
			"path":      certPath,
		},
	).On("Set", float64(fingerprint)).Once()

	provider, err := NewGrpcClientTlsProvider(
		GrpcClientTlsProviderConfig{RootCertsFile: certPath},
		registry,
	)
	require.NoError(t, err)
	tlsConfig := provider.GetTlsConfig()
	require.NotNil(t, tlsConfig.RootCAs)
	require.Equal(t, uint16(tls.VersionTLS12), tlsConfig.MinVersion)
	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcClientTlsProviderIsOptional(t *testing.T) {
	configs := []GrpcClientTlsProviderConfig{
		{},
		{
			Insecure:      true,
			RootCertsFile: "unused.pem",
		},
	}

	for _, config := range configs {
		provider, err := NewGrpcClientTlsProvider(
			config,
			metrics_mocks.NewRegistryMock(),
		)

		require.NoError(t, err)
		require.Nil(t, provider)
	}
}

func TestGrpcServerTlsProviderReportsEarliestExpiration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Now().Truncate(time.Second)
	leafPEM, leafKeyPEM := generateCertificate(
		t,
		"leaf",
		now.Add(48*time.Hour),
	)
	intermediatePEM, _ := generateCertificate(
		t,
		"intermediate",
		now.Add(24*time.Hour),
	)

	dir := t.TempDir()
	certPath := filepath.Join(dir, "server.pem")
	keyPath := filepath.Join(dir, "server.key")
	require.NoError(
		t,
		os.WriteFile(certPath, append(leafPEM, intermediatePEM...), 0o600),
	)
	require.NoError(t, os.WriteFile(keyPath, leafKeyPEM, 0o600))

	registry := metrics_mocks.NewRegistryMock()
	registry.GetGauge(
		"expireTs",
		map[string]string{"path": certPath},
	).On("Set", float64(now.Add(24*time.Hour).Unix())).Once()
	registry.GetGauge(
		"certificateValidity",
		map[string]string{"path": certPath},
	).On("Set", float64(0)).Once()

	provider, err := NewGrpcServerTlsProvider(
		ctx,
		[]GrpcServerCertificateConfig{{
			CertFile:       certPath,
			PrivateKeyFile: keyPath,
		}},
		registry,
	)
	require.NoError(t, err)
	require.Len(t, provider.certificates, 1)
	require.Len(t, provider.certificates[0].Certificate, 2)
	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcServerTlsProviderSelectsCertificate(t *testing.T) {
	firstCertificate := loadServerCertificate(t, "first.example")
	secondCertificate := loadServerCertificate(t, "second.example")
	provider := &GrpcServerTlsProvider{
		certificates: []tls.Certificate{firstCertificate, secondCertificate},
	}

	selected, err := provider.getCertificate(&tls.ClientHelloInfo{
		ServerName:        "second.example",
		SupportedVersions: []uint16{tls.VersionTLS13},
	})

	require.NoError(t, err)
	require.Equal(t, secondCertificate.Leaf.Raw, selected.Leaf.Raw)

	selected, err = provider.getCertificate(&tls.ClientHelloInfo{
		ServerName:        "unknown.example",
		SupportedVersions: []uint16{tls.VersionTLS13},
	})

	require.NoError(t, err)
	require.Equal(t, firstCertificate.Leaf.Raw, selected.Leaf.Raw)
}

func TestGrpcTlsProvidersRejectInvalidInitialCertificates(t *testing.T) {
	certPath := filepath.Join(t.TempDir(), "invalid.pem")
	require.NoError(t, os.WriteFile(certPath, []byte("invalid"), 0o600))

	_, err := NewGrpcClientTlsProvider(
		GrpcClientTlsProviderConfig{RootCertsFile: certPath},
		metrics_mocks.NewRegistryMock(),
	)
	require.Error(t, err)

	_, err = NewGrpcServerTlsProvider(
		context.Background(),
		[]GrpcServerCertificateConfig{{
			CertFile:       certPath,
			PrivateKeyFile: certPath,
		}},
		metrics_mocks.NewRegistryMock(),
	)
	require.Error(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func generateCertificate(
	t *testing.T,
	commonName string,
	notAfter time.Time,
) ([]byte, []byte) {

	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: commonName},
		DNSNames:              []string{commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		&privateKey.PublicKey,
		privateKey,
	)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(privateKey)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func loadServerCertificate(t *testing.T, serverName string) tls.Certificate {
	t.Helper()

	certPEM, keyPEM := generateCertificate(
		t,
		serverName,
		time.Now().Add(24*time.Hour),
	)
	dir := t.TempDir()
	certPath := filepath.Join(dir, "server.pem")
	keyPath := filepath.Join(dir, "server.key")
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))

	certificate, _, err := readServerCertificate(GrpcServerCertificateConfig{
		CertFile:       certPath,
		PrivateKeyFile: keyPath,
	})

	require.NoError(t, err)
	return certificate
}
