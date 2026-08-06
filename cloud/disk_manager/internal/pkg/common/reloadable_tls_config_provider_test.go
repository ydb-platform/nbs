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
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

func TestReloadableTLSConfigProviderRefreshAndKeepLastGood(t *testing.T) {
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

	provider, err := NewReloadableTLSConfigProvider(
		ctx,
		ReloadableTLSConfigProviderConfig{
			RootCertsFile:     certPath,
			RefreshPeriod:     10 * time.Millisecond,
			UseSystemCertPool: true,
		},
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

	require.NoError(
		t,
		os.WriteFile(certPath, []byte("broken certificate"), 0o600),
	)

	time.Sleep(100 * time.Millisecond)

	cfg, err := provider.GetTLSConfig(ctx)
	require.NoError(t, err)
	require.True(t, containsSubject(cfg.RootCAs, cert2Subject))
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
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	notBefore := time.Now().Add(-time.Hour)
	notAfter := time.Now().Add(24 * time.Hour)

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
