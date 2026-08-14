package common

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
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

func TestGRPCClientTLSProviderLoadsConfigAndReportsFingerprint(t *testing.T) {
	certPEM, _ := generateCertificate(t, "root", time.Now().Add(24*time.Hour))
	certPath := filepath.Join(t.TempDir(), "root.pem")
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))

	registry := metrics_mocks.NewRegistryMock()
	fingerprint := cityhash.Hash64(certPEM) & ((1 << 53) - 1)
	registry.GetGauge(
		"Fingerprint",
		map[string]string{
			"subsystem": "certificates",
			"cert":      filepath.Base(certPath),
		},
	).On("Set", float64(fingerprint)).Once()

	provider, err := NewGRPCClientTLSProvider(
		GRPCClientTLSProviderConfig{RootCertsFile: certPath},
		registry,
	)
	require.NoError(t, err)
	require.NotNil(t, provider.GetTLSConfig().RootCAs)
	require.True(t, registry.AssertAllExpectations(t))
}

func TestGRPCServerTLSProviderReportsEarliestExpiration(t *testing.T) {
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
		"ExpireTs",
		map[string]string{"path": certPath},
	).On("Set", float64(now.Add(24*time.Hour).Unix())).Once()
	registry.GetGauge(
		"certificateValidity",
		map[string]string{"path": certPath},
	).On("Set", float64(0)).Once()

	provider, err := NewGRPCServerTLSProvider(
		ctx,
		[]GRPCServerCertificateConfig{{
			CertFile:       certPath,
			PrivateKeyFile: keyPath,
		}},
		registry,
	)
	require.NoError(t, err)
	require.Len(t, provider.certificates, 1)
	require.Len(t, provider.certificates[0].Certificate, 2)
	require.NotNil(t, provider.NewTransportCredentials())
	require.True(t, registry.AssertAllExpectations(t))
}

func TestGRPCTLSProvidersRejectInvalidInitialCertificates(t *testing.T) {
	certPath := filepath.Join(t.TempDir(), "invalid.pem")
	require.NoError(t, os.WriteFile(certPath, []byte("invalid"), 0o600))

	_, err := NewGRPCClientTLSProvider(
		GRPCClientTLSProviderConfig{RootCertsFile: certPath},
		metrics_mocks.NewRegistryMock(),
	)
	require.Error(t, err)

	_, err = NewGRPCServerTLSProvider(
		context.Background(),
		[]GRPCServerCertificateConfig{{
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
