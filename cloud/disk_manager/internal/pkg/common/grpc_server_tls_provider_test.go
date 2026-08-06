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
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

func TestGRPCServerTLSProviderRefreshAndKeepLastGood(t *testing.T) {
	ctx, cancel := context.WithCancel(
		logging.SetLogger(
			context.Background(),
			logging.NewStderrLogger(logging.DebugLevel),
		),
	)
	defer cancel()

	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "server.crt")
	keyPath := filepath.Join(tempDir, "server.key")

	cert1PEM, key1PEM, serial1 := mustGenerateServerCertificatePair(
		t,
		"disk-manager-server-cert-1",
	)
	require.NoError(t, os.WriteFile(certPath, cert1PEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, key1PEM, 0o600))

	provider, err := NewGRPCServerTLSProvider(
		ctx,
		[]*server_config.Cert{
			{
				CertFile:       &certPath,
				PrivateKeyFile: &keyPath,
			},
		},
		10*time.Millisecond,
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	require.Eventually(
		t,
		func() bool {
			certificate, err := provider.getCertificate(nil)
			if err != nil {
				return false
			}

			serial, err := parseCertificateSerial(certificate)
			return err == nil && serial.Cmp(serial1) == 0
		},
		time.Second,
		10*time.Millisecond,
	)

	cert2PEM, key2PEM, serial2 := mustGenerateServerCertificatePair(
		t,
		"disk-manager-server-cert-2",
	)
	require.NoError(t, os.WriteFile(certPath, cert2PEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, key2PEM, 0o600))

	require.Eventually(
		t,
		func() bool {
			certificate, err := provider.getCertificate(nil)
			if err != nil {
				return false
			}

			serial, err := parseCertificateSerial(certificate)
			return err == nil && serial.Cmp(serial2) == 0
		},
		time.Second,
		10*time.Millisecond,
	)

	require.NoError(
		t,
		os.WriteFile(certPath, []byte("broken certificate"), 0o600),
	)

	time.Sleep(100 * time.Millisecond)

	certificate, err := provider.getCertificate(nil)
	require.NoError(t, err)
	serial, err := parseCertificateSerial(certificate)
	require.NoError(t, err)
	require.Equal(t, 0, serial.Cmp(serial2))
}

////////////////////////////////////////////////////////////////////////////////

func parseCertificateSerial(
	certificate *tls.Certificate,
) (*big.Int, error) {
	if certificate == nil || len(certificate.Certificate) == 0 {
		return nil, x509.ErrUnsupportedAlgorithm
	}

	parsed, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return nil, err
	}

	return parsed.SerialNumber, nil
}

func mustGenerateServerCertificatePair(
	t *testing.T,
	commonName string,
) ([]byte, []byte, *big.Int) {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serialNumber := big.NewInt(time.Now().UnixNano())
	notBefore := time.Now().Add(-time.Hour)
	notAfter := time.Now().Add(24 * time.Hour)

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
	}

	der, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		&privateKey.PublicKey,
		privateKey,
	)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(
		&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: der,
		},
	)

	keyDER, err := x509.MarshalECPrivateKey(privateKey)
	require.NoError(t, err)

	keyPEM := pem.EncodeToMemory(
		&pem.Block{
			Type:  "EC PRIVATE KEY",
			Bytes: keyDER,
		},
	)

	return certPEM, keyPEM, serialNumber
}
