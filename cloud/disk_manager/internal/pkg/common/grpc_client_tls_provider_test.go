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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	metrics_mocks "github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
	"github.com/ydb-platform/nbs/contrib/go/cityhash"
)

////////////////////////////////////////////////////////////////////////////////

func TestGRPCClientTLSProviderRefreshAndKeepLastGood(t *testing.T) {
	ctx, cancel := context.WithCancel(
		logging.SetLogger(
			context.Background(),
			logging.NewStderrLogger(logging.DebugLevel),
		),
	)
	defer cancel()

	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "root_ca.pem")

	cert1PEM, cert1Subject := mustGenerateRootCertificate(
		t,
		"disk-manager-test-cert-1",
	)
	require.NoError(t, os.WriteFile(certPath, cert1PEM, 0o600))

	provider, err := NewGRPCClientTLSProvider(
		ctx,
		GRPCClientTLSProviderConfig{
			RootCertsFile:     certPath,
			RefreshPeriod:     10 * time.Millisecond,
			UseSystemCertPool: true,
		},
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	require.Eventually(
		t,
		func() bool {
			cfg, err := provider.GetTLSConfig(ctx)
			return err == nil &&
				containsSubject(cfg.RootCAs, cert1Subject)
		},
		time.Second,
		10*time.Millisecond,
	)

	cert2PEM, cert2Subject := mustGenerateRootCertificate(
		t,
		"disk-manager-test-cert-2",
	)
	require.NoError(t, os.WriteFile(certPath, cert2PEM, 0o600))

	require.Eventually(
		t,
		func() bool {
			cfg, err := provider.GetTLSConfig(ctx)
			return err == nil &&
				containsSubject(cfg.RootCAs, cert2Subject)
		},
		time.Second,
		10*time.Millisecond,
	)

	expiredCertPEM, expiredCertSubject := mustGenerateRootCertificateWithValidity(
		t,
		"disk-manager-expired-cert",
		time.Now().Add(-48*time.Hour),
		time.Now().Add(-24*time.Hour),
	)
	require.NoError(t, os.WriteFile(certPath, expiredCertPEM, 0o600))
	require.Eventually(
		t,
		func() bool {
			cfg, err := provider.GetTLSConfig(ctx)
			return err == nil &&
				containsSubject(cfg.RootCAs, expiredCertSubject)
		},
		time.Second,
		10*time.Millisecond,
	)

	require.NoError(
		t,
		os.WriteFile(certPath, []byte("broken certificate"), 0o600),
	)

	time.Sleep(100 * time.Millisecond)

	cfg, err := provider.GetTLSConfig(ctx)
	require.NoError(t, err)
	require.True(t, containsSubject(cfg.RootCAs, expiredCertSubject))
}

////////////////////////////////////////////////////////////////////////////////

func TestGRPCClientTLSProviderRejectsInvalidInitialRootCertificate(
	t *testing.T,
) {
	certPath := filepath.Join(t.TempDir(), "root_ca.pem")
	require.NoError(
		t,
		os.WriteFile(certPath, []byte("broken certificate"), 0o600),
	)

	_, err := NewGRPCClientTLSProvider(
		context.Background(),
		GRPCClientTLSProviderConfig{
			RootCertsFile: certPath,
		},
		metrics.NewEmptyRegistry(),
	)
	require.Error(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func TestGRPCClientTLSProviderAcceptsExpiredInitialRootCertificate(
	t *testing.T,
) {
	certPath := filepath.Join(t.TempDir(), "root_ca.pem")
	certPEM, _ := mustGenerateRootCertificateWithValidity(
		t,
		"expired-root",
		time.Now().Add(-48*time.Hour),
		time.Now().Add(-24*time.Hour),
	)
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))

	_, err := NewGRPCClientTLSProvider(
		context.Background(),
		GRPCClientTLSProviderConfig{
			RootCertsFile: certPath,
		},
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func TestGRPCClientTLSProviderChecksIdentityValidityOnlyOnRefresh(
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
	certPath := filepath.Join(tempDir, "client.crt")
	keyPath := filepath.Join(tempDir, "client.key")

	expiredCertPEM, expiredKeyPEM, expiredSerial :=
		mustGenerateServerCertificatePairWithValidity(
			t,
			"expired-client-cert",
			time.Now().Add(-48*time.Hour),
			time.Now().Add(-24*time.Hour),
		)
	require.NoError(t, os.WriteFile(certPath, expiredCertPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, expiredKeyPEM, 0o600))

	provider, err := NewGRPCClientTLSProvider(
		ctx,
		GRPCClientTLSProviderConfig{
			CertFile:           certPath,
			CertPrivateKeyFile: keyPath,
			RefreshPeriod:      10 * time.Millisecond,
		},
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)
	require.True(
		t,
		clientProviderCertificateSerialEquals(ctx, provider, expiredSerial),
	)

	validCertPEM, validKeyPEM, validSerial := mustGenerateServerCertificatePair(
		t,
		"valid-client-cert",
	)
	require.NoError(t, os.WriteFile(certPath, validCertPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, validKeyPEM, 0o600))
	require.Eventually(
		t,
		func() bool {
			return clientProviderCertificateSerialEquals(ctx, provider, validSerial)
		},
		time.Second,
		10*time.Millisecond,
	)

	candidateCertPEM, candidateKeyPEM, _ := mustGenerateServerCertificatePair(
		t,
		"candidate-client-cert",
	)
	expiredIntermediatePEM, _, _ := mustGenerateServerCertificatePairWithValidity(
		t,
		"expired-intermediate",
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
	require.True(
		t,
		clientProviderCertificateSerialEquals(ctx, provider, validSerial),
	)
	require.True(
		t,
		clientProviderCertificateChainLengthEquals(ctx, provider, 1),
	)

	futureIntermediatePEM, _, _ := mustGenerateServerCertificatePairWithValidity(
		t,
		"future-intermediate",
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
	require.True(
		t,
		clientProviderCertificateSerialEquals(ctx, provider, validSerial),
	)
	require.True(
		t,
		clientProviderCertificateChainLengthEquals(ctx, provider, 1),
	)
}

func clientProviderCertificateSerialEquals(
	ctx context.Context,
	provider *GRPCClientTLSProvider,
	expected *big.Int,
) bool {
	cfg, err := provider.GetTLSConfig(ctx)
	if err != nil || len(cfg.Certificates) == 0 {
		return false
	}

	serial, err := parseCertificateSerial(&cfg.Certificates[0])
	return err == nil && serial.Cmp(expected) == 0
}

func clientProviderCertificateChainLengthEquals(
	ctx context.Context,
	provider *GRPCClientTLSProvider,
	expected int,
) bool {
	cfg, err := provider.GetTLSConfig(ctx)
	return err == nil &&
		len(cfg.Certificates) == 1 &&
		len(cfg.Certificates[0].Certificate) == expected
}

////////////////////////////////////////////////////////////////////////////////

func TestGRPCClientTLSProviderReportsRootCertFingerprintWithoutRefresh(
	t *testing.T,
) {
	ctx := logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)

	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "root_ca.pem")
	certPEM, _ := mustGenerateRootCertificate(
		t,
		"disk-manager-test-cert",
	)
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))

	fingerprint := cityhash.Hash64(certPEM) & ((1 << 53) - 1)
	registry := metrics_mocks.NewRegistryMock()
	registry.GetGauge(
		"Fingerprint",
		map[string]string{
			"subsystem": "certificates",
			"cert":      filepath.Base(certPath),
		},
	).On("Set", float64(fingerprint)).Once()

	_, err := NewGRPCClientTLSProvider(
		ctx,
		GRPCClientTLSProviderConfig{
			RootCertsFile: certPath,
		},
		registry,
	)
	require.NoError(t, err)
	require.True(t, registry.AssertAllExpectations(t))
}

////////////////////////////////////////////////////////////////////////////////

func TestGRPCClientTLSProviderRefreshesRootCertFingerprint(t *testing.T) {
	ctx, cancel := context.WithCancel(
		logging.SetLogger(
			context.Background(),
			logging.NewStderrLogger(logging.DebugLevel),
		),
	)
	defer cancel()

	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "root_ca.pem")
	cert1PEM, _ := mustGenerateRootCertificate(t, "root-cert-1")
	cert2PEM, _ := mustGenerateRootCertificate(t, "root-cert-2")
	require.NoError(t, os.WriteFile(certPath, cert1PEM, 0o600))

	fingerprint1 := cityhash.Hash64(cert1PEM) & ((1 << 53) - 1)
	fingerprint2 := cityhash.Hash64(cert2PEM) & ((1 << 53) - 1)
	brokenCert := []byte("broken certificate")
	brokenFingerprint := cityhash.Hash64(brokenCert) & ((1 << 53) - 1)

	registry := metrics_mocks.NewRegistryMock()
	gauge := registry.GetGauge(
		"Fingerprint",
		map[string]string{
			"subsystem": "certificates",
			"cert":      filepath.Base(certPath),
		},
	)
	gauge.Test(t)

	initialReported := make(chan struct{})
	rotatedReported := make(chan struct{})
	var initialOnce sync.Once
	var rotatedOnce sync.Once
	gauge.On("Set", float64(fingerprint1)).Run(func(mock.Arguments) {
		initialOnce.Do(func() { close(initialReported) })
	}).Maybe()
	gauge.On("Set", float64(fingerprint2)).Run(func(mock.Arguments) {
		rotatedOnce.Do(func() { close(rotatedReported) })
	}).Maybe()
	// Accept the call so that its absence can be asserted explicitly below.
	gauge.On("Set", float64(brokenFingerprint)).Maybe()

	_, err := NewGRPCClientTLSProvider(
		ctx,
		GRPCClientTLSProviderConfig{
			RootCertsFile: certPath,
			RefreshPeriod: 10 * time.Millisecond,
		},
		registry,
	)
	require.NoError(t, err)

	requireSignal(t, initialReported)
	require.NoError(t, os.WriteFile(certPath, cert2PEM, 0o600))
	requireSignal(t, rotatedReported)

	require.NoError(t, os.WriteFile(certPath, brokenCert, 0o600))
	time.Sleep(100 * time.Millisecond)
	cancel()

	gauge.AssertNotCalled(t, "Set", float64(brokenFingerprint))
	require.True(t, registry.AssertAllExpectations(t))
}

////////////////////////////////////////////////////////////////////////////////

func requireSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for metric update")
	}
}

////////////////////////////////////////////////////////////////////////////////

func containsSubject(pool *x509.CertPool, subject []byte) bool {
	if pool == nil {
		return false
	}

	for _, candidate := range pool.Subjects() {
		if string(candidate) == string(subject) {
			return true
		}
	}
	return false
}

func mustGenerateRootCertificate(
	t *testing.T,
	commonName string,
) ([]byte, []byte) {
	return mustGenerateRootCertificateWithValidity(
		t,
		commonName,
		time.Now().Add(-time.Hour),
		time.Now().Add(24*time.Hour),
	)
}

func mustGenerateRootCertificateWithValidity(
	t *testing.T,
	commonName string,
	notBefore time.Time,
	notAfter time.Time,
) ([]byte, []byte) {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(notBefore.UnixNano()),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		&privateKey.PublicKey,
		privateKey,
	)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pemBytes := pem.EncodeToMemory(
		&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: der,
		},
	)

	return pemBytes, cert.RawSubject
}
