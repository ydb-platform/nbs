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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	metrics_mocks "github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
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

	require.NoError(t, os.WriteFile(keyPath, key1PEM, 0o600))
	time.Sleep(100 * time.Millisecond)
	requireProviderCertificateSerial(t, provider, serial2)
	require.NoError(t, os.WriteFile(keyPath, key2PEM, 0o600))

	candidateCertPEM, candidateKeyPEM, _ := mustGenerateServerCertificatePair(
		t,
		"disk-manager-candidate-cert",
	)
	expiredIntermediatePEM, _, _ := mustGenerateServerCertificatePairWithValidity(
		t,
		"disk-manager-expired-intermediate",
		time.Now().Add(-48*time.Hour),
		time.Now().Add(-24*time.Hour),
	)
	require.NoError(
		t,
		os.WriteFile(
			certPath,
			concatenateCertificatePEM(
				candidateCertPEM,
				expiredIntermediatePEM,
			),
			0o600,
		),
	)
	require.NoError(t, os.WriteFile(keyPath, candidateKeyPEM, 0o600))
	time.Sleep(100 * time.Millisecond)
	requireProviderCertificateSerial(t, provider, serial2)
	require.True(t, providerCertificateChainLengthEquals(provider, 0, 1))

	futureIntermediatePEM, _, _ := mustGenerateServerCertificatePairWithValidity(
		t,
		"disk-manager-future-intermediate",
		time.Now().Add(24*time.Hour),
		time.Now().Add(48*time.Hour),
	)
	require.NoError(
		t,
		os.WriteFile(
			certPath,
			concatenateCertificatePEM(
				candidateCertPEM,
				futureIntermediatePEM,
			),
			0o600,
		),
	)
	time.Sleep(100 * time.Millisecond)
	requireProviderCertificateSerial(t, provider, serial2)
	require.True(t, providerCertificateChainLengthEquals(provider, 0, 1))

	require.NoError(
		t,
		os.WriteFile(certPath, []byte("broken certificate"), 0o600),
	)

	time.Sleep(100 * time.Millisecond)

	requireProviderCertificateSerial(t, provider, serial2)
}

////////////////////////////////////////////////////////////////////////////////

func TestGRPCServerTLSProviderRejectsMismatchedInitialKey(t *testing.T) {
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "server.crt")
	keyPath := filepath.Join(tempDir, "server.key")

	certPEM, _, _ := mustGenerateServerCertificatePair(t, "server-cert")
	_, unrelatedKeyPEM, _ := mustGenerateServerCertificatePair(
		t,
		"unrelated-cert",
	)
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, unrelatedKeyPEM, 0o600))

	_, err := NewGRPCServerTLSProvider(
		context.Background(),
		[]*server_config.Cert{
			{
				CertFile:       &certPath,
				PrivateKeyFile: &keyPath,
			},
		},
		0,
		metrics.NewEmptyRegistry(),
	)
	require.Error(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func TestGRPCServerTLSProviderAcceptsExpiredInitialCertificate(t *testing.T) {
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "server.crt")
	keyPath := filepath.Join(tempDir, "server.key")

	certPEM, keyPEM, _ := mustGenerateServerCertificatePairWithValidity(
		t,
		"expired-server-cert",
		time.Now().Add(-48*time.Hour),
		time.Now().Add(-24*time.Hour),
	)
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))

	_, err := NewGRPCServerTLSProvider(
		context.Background(),
		[]*server_config.Cert{
			{
				CertFile:       &certPath,
				PrivateKeyFile: &keyPath,
			},
		},
		0,
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func TestGRPCServerTLSProviderRefreshesValidCertificatesIndependently(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(
		logging.SetLogger(
			context.Background(),
			logging.NewStderrLogger(logging.DebugLevel),
		),
	)
	defer cancel()

	tempDir := t.TempDir()
	cert1Path := filepath.Join(tempDir, "server1.crt")
	key1Path := filepath.Join(tempDir, "server1.key")
	cert2Path := filepath.Join(tempDir, "server2.crt")
	key2Path := filepath.Join(tempDir, "server2.key")

	cert1PEM, key1PEM, _ := mustGenerateServerCertificatePair(t, "server-1")
	cert2PEM, key2PEM, serial2 := mustGenerateServerCertificatePair(t, "server-2")
	require.NoError(t, os.WriteFile(cert1Path, cert1PEM, 0o600))
	require.NoError(t, os.WriteFile(key1Path, key1PEM, 0o600))
	require.NoError(t, os.WriteFile(cert2Path, cert2PEM, 0o600))
	require.NoError(t, os.WriteFile(key2Path, key2PEM, 0o600))

	provider, err := NewGRPCServerTLSProvider(
		ctx,
		[]*server_config.Cert{
			{
				CertFile:       &cert1Path,
				PrivateKeyFile: &key1Path,
			},
			{
				CertFile:       &cert2Path,
				PrivateKeyFile: &key2Path,
			},
		},
		10*time.Millisecond,
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	rotatedCertPEM, rotatedKeyPEM, rotatedSerial := mustGenerateServerCertificatePair(
		t,
		"server-1-rotated",
	)
	require.NoError(t, os.WriteFile(cert1Path, rotatedCertPEM, 0o600))
	require.NoError(t, os.WriteFile(key1Path, rotatedKeyPEM, 0o600))
	require.NoError(
		t,
		os.WriteFile(cert2Path, []byte("broken certificate"), 0o600),
	)

	require.Eventually(
		t,
		func() bool {
			return providerCertificateSerialEquals(provider, 0, rotatedSerial)
		},
		time.Second,
		10*time.Millisecond,
	)
	require.True(t, providerCertificateSerialEquals(provider, 1, serial2))
}

////////////////////////////////////////////////////////////////////////////////

func TestGRPCServerTLSProviderReportsCertificateExpirationsIndependently(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(
		logging.SetLogger(
			context.Background(),
			logging.NewStderrLogger(logging.DebugLevel),
		),
	)
	defer cancel()

	tempDir := t.TempDir()
	cert1Path := filepath.Join(tempDir, "server1.crt")
	key1Path := filepath.Join(tempDir, "server1.key")
	cert2Path := filepath.Join(tempDir, "server2.crt")
	key2Path := filepath.Join(tempDir, "server2.key")

	now := time.Now().Truncate(time.Second)
	initialExpiration1 := now.Add(24 * time.Hour)
	initialLeafExpiration1 := now.Add(36 * time.Hour)
	initialExpiration2 := now.Add(48 * time.Hour)
	rotatedExpiration1 := now.Add(72 * time.Hour)
	rotatedLeafExpiration1 := now.Add(96 * time.Hour)
	cert1PEM, key1PEM, _ := mustGenerateServerCertificatePairWithValidity(
		t,
		"server-1",
		now.Add(-time.Hour),
		initialLeafExpiration1,
	)
	intermediate1PEM, _, _ := mustGenerateServerCertificatePairWithValidity(
		t,
		"server-1-intermediate",
		now.Add(-time.Hour),
		initialExpiration1,
	)
	cert2PEM, key2PEM, serial2 := mustGenerateServerCertificatePairWithValidity(
		t,
		"server-2",
		now.Add(-time.Hour),
		initialExpiration2,
	)
	require.NoError(
		t,
		os.WriteFile(
			cert1Path,
			concatenateCertificatePEM(cert1PEM, intermediate1PEM),
			0o600,
		),
	)
	require.NoError(t, os.WriteFile(key1Path, key1PEM, 0o600))
	require.NoError(t, os.WriteFile(cert2Path, cert2PEM, 0o600))
	require.NoError(t, os.WriteFile(key2Path, key2PEM, 0o600))

	registry := metrics_mocks.NewRegistryMock()
	gauge1 := registry.GetGauge(
		"ExpireTs",
		map[string]string{"path": cert1Path},
	)
	gauge2 := registry.GetGauge(
		"ExpireTs",
		map[string]string{"path": cert2Path},
	)
	gauge1.Test(t)
	gauge2.Test(t)

	initialReported1 := make(chan struct{})
	initialReported2 := make(chan struct{})
	rotatedReported1 := make(chan struct{})
	var initialOnce1 sync.Once
	var initialOnce2 sync.Once
	var rotatedOnce1 sync.Once
	gauge1.On("Set", float64(initialExpiration1.Unix())).Run(
		func(mock.Arguments) {
			initialOnce1.Do(func() { close(initialReported1) })
		},
	).Maybe()
	gauge1.On("Set", float64(rotatedExpiration1.Unix())).Run(
		func(mock.Arguments) {
			rotatedOnce1.Do(func() { close(rotatedReported1) })
		},
	).Maybe()
	gauge2.On("Set", float64(initialExpiration2.Unix())).Run(
		func(mock.Arguments) {
			initialOnce2.Do(func() { close(initialReported2) })
		},
	).Maybe()

	provider, err := NewGRPCServerTLSProvider(
		ctx,
		[]*server_config.Cert{
			{
				CertFile:       &cert1Path,
				PrivateKeyFile: &key1Path,
			},
			{
				CertFile:       &cert2Path,
				PrivateKeyFile: &key2Path,
			},
		},
		10*time.Millisecond,
		registry,
	)
	require.NoError(t, err)
	requireSignal(t, initialReported1)
	requireSignal(t, initialReported2)

	rotatedCert1PEM, rotatedKey1PEM, _ :=
		mustGenerateServerCertificatePairWithValidity(
			t,
			"server-1-rotated",
			now.Add(-time.Hour),
			rotatedLeafExpiration1,
		)
	rotatedIntermediate1PEM, _, _ :=
		mustGenerateServerCertificatePairWithValidity(
			t,
			"server-1-rotated-intermediate",
			now.Add(-time.Hour),
			rotatedExpiration1,
		)
	require.NoError(
		t,
		os.WriteFile(
			cert1Path,
			concatenateCertificatePEM(
				rotatedCert1PEM,
				rotatedIntermediate1PEM,
			),
			0o600,
		),
	)
	require.NoError(t, os.WriteFile(key1Path, rotatedKey1PEM, 0o600))
	require.NoError(
		t,
		os.WriteFile(cert2Path, []byte("broken certificate"), 0o600),
	)
	requireSignal(t, rotatedReported1)

	time.Sleep(100 * time.Millisecond)
	cancel()
	require.True(t, providerCertificateSerialEquals(provider, 1, serial2))
	require.True(t, registry.AssertAllExpectations(t))
}

////////////////////////////////////////////////////////////////////////////////

func providerCertificateSerialEquals(
	provider *GRPCServerTLSProvider,
	index int,
	expected *big.Int,
) bool {
	provider.lock.RLock()
	defer provider.lock.RUnlock()

	if index >= len(provider.certificates) {
		return false
	}
	serial, err := parseCertificateSerial(&provider.certificates[index])
	return err == nil && serial.Cmp(expected) == 0
}

func providerCertificateChainLengthEquals(
	provider *GRPCServerTLSProvider,
	index int,
	expected int,
) bool {
	provider.lock.RLock()
	defer provider.lock.RUnlock()

	return index < len(provider.certificates) &&
		len(provider.certificates[index].Certificate) == expected
}

////////////////////////////////////////////////////////////////////////////////

func requireProviderCertificateSerial(
	t *testing.T,
	provider *GRPCServerTLSProvider,
	expected *big.Int,
) {
	t.Helper()

	certificate, err := provider.getCertificate(nil)
	require.NoError(t, err)
	serial, err := parseCertificateSerial(certificate)
	require.NoError(t, err)
	require.Equal(t, 0, serial.Cmp(expected))
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
	return mustGenerateServerCertificatePairWithValidity(
		t,
		commonName,
		time.Now().Add(-time.Hour),
		time.Now().Add(24*time.Hour),
	)
}

func mustGenerateServerCertificatePairWithValidity(
	t *testing.T,
	commonName string,
	notBefore time.Time,
	notAfter time.Time,
) ([]byte, []byte, *big.Int) {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serialNumber := big.NewInt(time.Now().UnixNano())
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

func concatenateCertificatePEM(certificates ...[]byte) []byte {
	var result []byte
	for _, certificate := range certificates {
		result = append(result, certificate...)
	}
	return result
}
