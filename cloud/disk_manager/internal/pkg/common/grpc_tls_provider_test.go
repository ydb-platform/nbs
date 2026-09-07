package common

import (
	"bytes"
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
		"fingerprint",
		map[string]string{
			"subsystem": "certificates",
			"path":      certPath,
		},
	).On("Set", float64(fingerprint)).Once()

	provider, err := NewGrpcClientTlsProvider(
		context.Background(),
		false,
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
	configs := []struct {
		insecure bool
		config   GrpcClientTlsProviderConfig
	}{
		{},
		{
			insecure: true,
			config: GrpcClientTlsProviderConfig{
				RootCertsFile: "unused.pem",
			},
		},
	}

	for _, config := range configs {
		provider, err := NewGrpcClientTlsProvider(
			context.Background(),
			config.insecure,
			config.config,
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
		0, // refreshPeriod
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
		context.Background(),
		false,
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
		0, // refreshPeriod
		metrics_mocks.NewRegistryMock(),
	)
	require.Error(t, err)
}

func TestGrpcTlsProvidersPreserveFileErrors(t *testing.T) {
	missingPath := filepath.Join(t.TempDir(), "missing.pem")

	_, err := NewGrpcClientTlsProvider(
		context.Background(),
		false,
		GrpcClientTlsProviderConfig{RootCertsFile: missingPath},
		metrics_mocks.NewRegistryMock(),
	)

	require.ErrorIs(t, err, os.ErrNotExist)

	_, err = NewGrpcServerTlsProvider(
		context.Background(),
		[]GrpcServerCertificateConfig{{
			CertFile:       missingPath,
			PrivateKeyFile: missingPath,
		}},
		0, // refreshPeriod
		metrics_mocks.NewRegistryMock(),
	)

	require.ErrorIs(t, err, os.ErrNotExist)
}

////////////////////////////////////////////////////////////////////////////////

func generateCertificate(
	t *testing.T,
	commonName string,
	notAfter time.Time,
) ([]byte, []byte) {

	t.Helper()
	return generateCertificateWithValidity(
		t,
		commonName,
		time.Now().Add(-time.Hour),
		notAfter,
	)
}

func generateCertificateWithValidity(
	t *testing.T,
	commonName string,
	notBefore time.Time,
	notAfter time.Time,
) ([]byte, []byte) {

	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: commonName},
		DNSNames:              []string{commonName},
		NotBefore:             notBefore,
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

////////////////////////////////////////////////////////////////////////////////

func TestGrpcClientTlsProviderRefreshLoadsNewRootCertificate(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	firstPEM, _ := generateCertificate(t, "first", time.Now().Add(24*time.Hour))
	secondPEM, _ := generateCertificate(t, "second", time.Now().Add(24*time.Hour))
	certPath := filepath.Join(t.TempDir(), "root.pem")
	require.NoError(t, os.WriteFile(certPath, firstPEM, 0o600))

	registry := metrics_mocks.NewRegistryMock()
	gauge := registry.GetGauge(
		"fingerprint",
		map[string]string{
			"subsystem": "certificates",
			"path":      certPath,
		},
	)
	gauge.On("Set", float64(fingerprintOf(firstPEM))).Once()
	gauge.On("Set", float64(fingerprintOf(secondPEM))).Once()

	provider, err := NewGrpcClientTlsProvider(
		ctx,
		false,
		GrpcClientTlsProviderConfig{RootCertsFile: certPath},
		registry,
	)
	require.NoError(t, err)
	require.True(
		t,
		provider.GetTlsConfig().RootCAs.Equal(newCertPool(t, firstPEM)),
	)

	require.NoError(t, os.WriteFile(certPath, secondPEM, 0o600))
	provider.(*grpcClientTlsProvider).refresh(ctx)

	require.True(
		t,
		provider.GetTlsConfig().RootCAs.Equal(newCertPool(t, secondPEM)),
	)
	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcClientTlsProviderRefreshKeepsLastGoodRootCertificate(
	t *testing.T,
) {

	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	certPEM, _ := generateCertificate(t, "root", time.Now().Add(24*time.Hour))
	certPath := filepath.Join(t.TempDir(), "root.pem")
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))

	registry := metrics_mocks.NewRegistryMock()
	registry.GetGauge(
		"fingerprint",
		map[string]string{
			"subsystem": "certificates",
			"path":      certPath,
		},
	).On("Set", float64(fingerprintOf(certPEM))).Once()

	provider, err := NewGrpcClientTlsProvider(
		ctx,
		false,
		GrpcClientTlsProviderConfig{RootCertsFile: certPath},
		registry,
	)
	require.NoError(t, err)
	expectedConfig := provider.GetTlsConfig()

	// Unchanged file.
	provider.(*grpcClientTlsProvider).refresh(ctx)
	require.Same(t, expectedConfig, provider.GetTlsConfig())

	require.NoError(t, os.WriteFile(certPath, []byte("invalid"), 0o600))
	provider.(*grpcClientTlsProvider).refresh(ctx)
	require.Same(t, expectedConfig, provider.GetTlsConfig())

	require.NoError(t, os.Remove(certPath))
	provider.(*grpcClientTlsProvider).refresh(ctx)
	require.Same(t, expectedConfig, provider.GetTlsConfig())

	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcClientTlsProviderRefreshesPeriodically(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	firstPEM, _ := generateCertificate(t, "first", time.Now().Add(24*time.Hour))
	secondPEM, _ := generateCertificate(t, "second", time.Now().Add(24*time.Hour))
	certPath := filepath.Join(t.TempDir(), "root.pem")
	require.NoError(t, os.WriteFile(certPath, firstPEM, 0o600))

	registry := metrics_mocks.NewRegistryMock()
	gauge := registry.GetGauge(
		"fingerprint",
		map[string]string{
			"subsystem": "certificates",
			"path":      certPath,
		},
	)
	gauge.On("Set", float64(fingerprintOf(firstPEM)))
	gauge.On("Set", float64(fingerprintOf(secondPEM)))

	provider, err := NewGrpcClientTlsProvider(
		ctx,
		false,
		GrpcClientTlsProviderConfig{
			RootCertsFile: certPath,
			RefreshPeriod: 10 * time.Millisecond,
		},
		registry,
	)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(certPath, secondPEM, 0o600))
	expectedPool := newCertPool(t, secondPEM)
	require.Eventually(
		t,
		func() bool {
			return provider.GetTlsConfig().RootCAs.Equal(expectedPool)
		},
		5*time.Second,
		10*time.Millisecond,
	)
}

////////////////////////////////////////////////////////////////////////////////

func fingerprintOf(pem []byte) uint64 {
	return cityhash.Hash64(pem) & ((1 << 53) - 1)
}

func newCertPool(t *testing.T, pem []byte) *x509.CertPool {
	t.Helper()

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pem))
	return pool
}

////////////////////////////////////////////////////////////////////////////////

type serverCertificateFiles struct {
	certPath string
	keyPath  string
}

func writeServerCertificate(
	t *testing.T,
	files serverCertificateFiles,
	certPEM []byte,
	keyPEM []byte,
) {

	t.Helper()
	require.NoError(t, os.WriteFile(files.certPath, certPEM, 0o600))
	require.NoError(t, os.WriteFile(files.keyPath, keyPEM, 0o600))
}

func newServerCertificateFiles(t *testing.T) serverCertificateFiles {
	dir := t.TempDir()
	return serverCertificateFiles{
		certPath: filepath.Join(dir, "server.pem"),
		keyPath:  filepath.Join(dir, "server.key"),
	}
}

func leafOf(t *testing.T, certPEM []byte) []byte {
	t.Helper()

	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block)
	return block.Bytes
}

func selectedLeaf(
	t *testing.T,
	provider *GrpcServerTlsProvider,
	serverName string,
) []byte {

	t.Helper()

	selected, err := provider.getCertificate(&tls.ClientHelloInfo{
		ServerName:        serverName,
		SupportedVersions: []uint16{tls.VersionTLS13},
	})
	require.NoError(t, err)
	return selected.Leaf.Raw
}

func expectServerCertificateMetrics(
	registry *metrics_mocks.RegistryMock,
	certPath string,
	expireTs time.Time,
	validity float64,
) {

	registry.GetGauge(
		"expireTs",
		map[string]string{"path": certPath},
	).On("Set", float64(expireTs.Unix())).Once()
	registry.GetGauge(
		"certificateValidity",
		map[string]string{"path": certPath},
	).On("Set", validity).Once()
}

func TestGrpcServerTlsProviderRefreshLoadsNewCertificate(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	now := time.Now().Truncate(time.Second)
	files := newServerCertificateFiles(t)
	firstPEM, firstKeyPEM := generateCertificate(
		t,
		"server.example",
		now.Add(30*24*time.Hour),
	)
	secondPEM, secondKeyPEM := generateCertificate(
		t,
		"server.example",
		now.Add(60*24*time.Hour),
	)
	writeServerCertificate(t, files, firstPEM, firstKeyPEM)

	registry := metrics_mocks.NewRegistryMock()
	expectServerCertificateMetrics(
		registry,
		files.certPath,
		now.Add(30*24*time.Hour),
		1,
	)
	expectServerCertificateMetrics(
		registry,
		files.certPath,
		now.Add(60*24*time.Hour),
		1,
	)

	provider, err := NewGrpcServerTlsProvider(
		ctx,
		[]GrpcServerCertificateConfig{{
			CertFile:       files.certPath,
			PrivateKeyFile: files.keyPath,
		}},
		0, // refreshPeriod
		registry,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		leafOf(t, firstPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	writeServerCertificate(t, files, secondPEM, secondKeyPEM)
	provider.refresh(ctx, now)

	require.Equal(
		t,
		leafOf(t, secondPEM),
		selectedLeaf(t, provider, "server.example"),
	)
	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcServerTlsProviderRefreshKeepsLastGoodCertificate(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	now := time.Now().Truncate(time.Second)
	files := newServerCertificateFiles(t)
	certPEM, keyPEM := generateCertificate(
		t,
		"server.example",
		now.Add(30*24*time.Hour),
	)
	otherPEM, _ := generateCertificate(
		t,
		"server.example",
		now.Add(30*24*time.Hour),
	)
	writeServerCertificate(t, files, certPEM, keyPEM)

	registry := metrics_mocks.NewRegistryMock()
	expectServerCertificateMetrics(
		registry,
		files.certPath,
		now.Add(30*24*time.Hour),
		1,
	)

	provider, err := NewGrpcServerTlsProvider(
		ctx,
		[]GrpcServerCertificateConfig{{
			CertFile:       files.certPath,
			PrivateKeyFile: files.keyPath,
		}},
		0, // refreshPeriod
		registry,
	)
	require.NoError(t, err)

	// Unchanged files.
	provider.refresh(ctx, now)
	require.Equal(
		t,
		leafOf(t, certPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	// Invalid certificate.
	writeServerCertificate(t, files, []byte("invalid"), keyPEM)
	provider.refresh(ctx, now)
	require.Equal(
		t,
		leafOf(t, certPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	// Certificate does not match the private key.
	writeServerCertificate(t, files, otherPEM, keyPEM)
	provider.refresh(ctx, now)
	require.Equal(
		t,
		leafOf(t, certPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	// Missing files.
	require.NoError(t, os.Remove(files.certPath))
	require.NoError(t, os.Remove(files.keyPath))
	provider.refresh(ctx, now)
	require.Equal(
		t,
		leafOf(t, certPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcServerTlsProviderRefreshRejectsCertificateOutsideValidityPeriod(
	t *testing.T,
) {

	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	now := time.Now().Truncate(time.Second)
	files := newServerCertificateFiles(t)
	certPEM, keyPEM := generateCertificate(
		t,
		"server.example",
		now.Add(30*24*time.Hour),
	)
	expiredPEM, expiredKeyPEM := generateCertificateWithValidity(
		t,
		"server.example",
		now.Add(-2*time.Hour),
		now.Add(-time.Hour),
	)
	futurePEM, futureKeyPEM := generateCertificateWithValidity(
		t,
		"server.example",
		now.Add(time.Hour),
		now.Add(30*24*time.Hour),
	)
	writeServerCertificate(t, files, certPEM, keyPEM)

	registry := metrics_mocks.NewRegistryMock()
	expectServerCertificateMetrics(
		registry,
		files.certPath,
		now.Add(30*24*time.Hour),
		1,
	)

	provider, err := NewGrpcServerTlsProvider(
		ctx,
		[]GrpcServerCertificateConfig{{
			CertFile:       files.certPath,
			PrivateKeyFile: files.keyPath,
		}},
		0, // refreshPeriod
		registry,
	)
	require.NoError(t, err)

	writeServerCertificate(t, files, expiredPEM, expiredKeyPEM)
	provider.refresh(ctx, now)
	require.Equal(
		t,
		leafOf(t, certPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	writeServerCertificate(t, files, futurePEM, futureKeyPEM)
	provider.refresh(ctx, now)
	require.Equal(
		t,
		leafOf(t, certPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	// Expired intermediate certificate invalidates the whole chain.
	writeServerCertificate(t, files, append(certPEM, expiredPEM...), keyPEM)
	provider.refresh(ctx, now)
	require.Equal(
		t,
		leafOf(t, certPEM),
		selectedLeaf(t, provider, "server.example"),
	)
	require.Len(t, provider.certificates[0].Certificate, 1)

	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcServerTlsProviderInitialLoadAcceptsExpiredCertificate(
	t *testing.T,
) {

	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	now := time.Now().Truncate(time.Second)
	files := newServerCertificateFiles(t)
	expiredPEM, expiredKeyPEM := generateCertificateWithValidity(
		t,
		"server.example",
		now.Add(-2*time.Hour),
		now.Add(-time.Hour),
	)
	writeServerCertificate(t, files, expiredPEM, expiredKeyPEM)

	registry := metrics_mocks.NewRegistryMock()
	expectServerCertificateMetrics(
		registry,
		files.certPath,
		now.Add(-time.Hour),
		0,
	)

	provider, err := NewGrpcServerTlsProvider(
		ctx,
		[]GrpcServerCertificateConfig{{
			CertFile:       files.certPath,
			PrivateKeyFile: files.keyPath,
		}},
		0, // refreshPeriod
		registry,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		leafOf(t, expiredPEM),
		selectedLeaf(t, provider, "server.example"),
	)
	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcServerTlsProviderRefreshesCertificatesIndependently(
	t *testing.T,
) {

	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	now := time.Now().Truncate(time.Second)
	firstFiles := newServerCertificateFiles(t)
	secondFiles := newServerCertificateFiles(t)
	firstPEM, firstKeyPEM := generateCertificate(
		t,
		"first.example",
		now.Add(30*24*time.Hour),
	)
	newFirstPEM, newFirstKeyPEM := generateCertificate(
		t,
		"first.example",
		now.Add(60*24*time.Hour),
	)
	secondPEM, secondKeyPEM := generateCertificate(
		t,
		"second.example",
		now.Add(30*24*time.Hour),
	)
	writeServerCertificate(t, firstFiles, firstPEM, firstKeyPEM)
	writeServerCertificate(t, secondFiles, secondPEM, secondKeyPEM)

	registry := metrics_mocks.NewRegistryMock()
	expectServerCertificateMetrics(
		registry,
		firstFiles.certPath,
		now.Add(30*24*time.Hour),
		1,
	)
	expectServerCertificateMetrics(
		registry,
		firstFiles.certPath,
		now.Add(60*24*time.Hour),
		1,
	)
	expectServerCertificateMetrics(
		registry,
		secondFiles.certPath,
		now.Add(30*24*time.Hour),
		1,
	)

	provider, err := NewGrpcServerTlsProvider(
		ctx,
		[]GrpcServerCertificateConfig{
			{
				CertFile:       firstFiles.certPath,
				PrivateKeyFile: firstFiles.keyPath,
			},
			{
				CertFile:       secondFiles.certPath,
				PrivateKeyFile: secondFiles.keyPath,
			},
		},
		0, // refreshPeriod
		registry,
	)
	require.NoError(t, err)

	writeServerCertificate(t, firstFiles, newFirstPEM, newFirstKeyPEM)
	writeServerCertificate(t, secondFiles, []byte("invalid"), secondKeyPEM)
	provider.refresh(ctx, now)

	require.Equal(
		t,
		leafOf(t, newFirstPEM),
		selectedLeaf(t, provider, "first.example"),
	)
	require.Equal(
		t,
		leafOf(t, secondPEM),
		selectedLeaf(t, provider, "second.example"),
	)
	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcServerTlsProviderRefreshesPeriodically(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	now := time.Now().Truncate(time.Second)
	files := newServerCertificateFiles(t)
	firstPEM, firstKeyPEM := generateCertificate(
		t,
		"server.example",
		now.Add(30*24*time.Hour),
	)
	secondPEM, secondKeyPEM := generateCertificate(
		t,
		"server.example",
		now.Add(60*24*time.Hour),
	)
	writeServerCertificate(t, files, firstPEM, firstKeyPEM)

	registry := metrics_mocks.NewRegistryMock()
	expireTsGauge := registry.GetGauge(
		"expireTs",
		map[string]string{"path": files.certPath},
	)
	expireTsGauge.On("Set", float64(now.Add(30*24*time.Hour).Unix()))
	expireTsGauge.On("Set", float64(now.Add(60*24*time.Hour).Unix()))
	registry.GetGauge(
		"certificateValidity",
		map[string]string{"path": files.certPath},
	).On("Set", float64(1))

	provider, err := NewGrpcServerTlsProvider(
		ctx,
		[]GrpcServerCertificateConfig{{
			CertFile:       files.certPath,
			PrivateKeyFile: files.keyPath,
		}},
		10*time.Millisecond,
		registry,
	)
	require.NoError(t, err)

	writeServerCertificate(t, files, secondPEM, secondKeyPEM)
	expectedLeaf := leafOf(t, secondPEM)
	require.Eventually(
		t,
		func() bool {
			selected, err := provider.getCertificate(nil)
			return err == nil && bytes.Equal(selected.Leaf.Raw, expectedLeaf)
		},
		5*time.Second,
		10*time.Millisecond,
	)
}

////////////////////////////////////////////////////////////////////////////////

// Simulates a file that is being rewritten non-atomically: the last PEM block
// has no end marker.
func truncatePEM(certPEM []byte) []byte {
	return certPEM[:len(certPEM)/2]
}

func TestGrpcClientTlsProviderRefreshRejectsTruncatedRootCertificates(
	t *testing.T,
) {

	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	firstPEM, _ := generateCertificate(t, "first", time.Now().Add(24*time.Hour))
	secondPEM, _ := generateCertificate(t, "second", time.Now().Add(24*time.Hour))
	bundlePEM := append(append([]byte(nil), firstPEM...), secondPEM...)
	certPath := filepath.Join(t.TempDir(), "root.pem")
	require.NoError(t, os.WriteFile(certPath, bundlePEM, 0o600))

	registry := metrics_mocks.NewRegistryMock()
	registry.GetGauge(
		"fingerprint",
		map[string]string{
			"subsystem": "certificates",
			"path":      certPath,
		},
	).On("Set", float64(fingerprintOf(bundlePEM))).Once()

	provider, err := NewGrpcClientTlsProvider(
		ctx,
		false,
		GrpcClientTlsProviderConfig{RootCertsFile: certPath},
		registry,
	)
	require.NoError(t, err)
	expectedConfig := provider.GetTlsConfig()

	truncatedPEM := append(
		append([]byte(nil), firstPEM...),
		truncatePEM(secondPEM)...,
	)
	require.NoError(t, os.WriteFile(certPath, truncatedPEM, 0o600))
	provider.(*grpcClientTlsProvider).refresh(ctx)

	require.Same(t, expectedConfig, provider.GetTlsConfig())
	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcServerTlsProviderRefreshRejectsTruncatedCertificateChain(
	t *testing.T,
) {

	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	now := time.Now().Truncate(time.Second)
	files := newServerCertificateFiles(t)
	leafPEM, leafKeyPEM := generateCertificate(
		t,
		"server.example",
		now.Add(60*24*time.Hour),
	)
	intermediatePEM, _ := generateCertificate(
		t,
		"intermediate",
		now.Add(30*24*time.Hour),
	)
	chainPEM := append(append([]byte(nil), leafPEM...), intermediatePEM...)
	writeServerCertificate(t, files, chainPEM, leafKeyPEM)

	registry := metrics_mocks.NewRegistryMock()
	expectServerCertificateMetrics(
		registry,
		files.certPath,
		now.Add(30*24*time.Hour),
		1,
	)

	provider, err := NewGrpcServerTlsProvider(
		ctx,
		[]GrpcServerCertificateConfig{{
			CertFile:       files.certPath,
			PrivateKeyFile: files.keyPath,
		}},
		0, // refreshPeriod
		registry,
	)
	require.NoError(t, err)

	truncatedPEM := append(
		append([]byte(nil), leafPEM...),
		truncatePEM(intermediatePEM)...,
	)
	writeServerCertificate(t, files, truncatedPEM, leafKeyPEM)
	provider.refresh(ctx, now)

	require.Equal(
		t,
		leafOf(t, leafPEM),
		selectedLeaf(t, provider, "server.example"),
	)
	require.Len(t, provider.certificates[0].Certificate, 2)
	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcServerTlsProviderRefreshDetectsIntermediateChange(
	t *testing.T,
) {

	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	now := time.Now().Truncate(time.Second)
	files := newServerCertificateFiles(t)
	leafPEM, leafKeyPEM := generateCertificate(
		t,
		"server.example",
		now.Add(60*24*time.Hour),
	)
	firstIntermediatePEM, _ := generateCertificate(
		t,
		"intermediate",
		now.Add(30*24*time.Hour),
	)
	secondIntermediatePEM, _ := generateCertificate(
		t,
		"intermediate",
		now.Add(20*24*time.Hour),
	)
	writeServerCertificate(
		t,
		files,
		append(append([]byte(nil), leafPEM...), firstIntermediatePEM...),
		leafKeyPEM,
	)

	registry := metrics_mocks.NewRegistryMock()
	expectServerCertificateMetrics(
		registry,
		files.certPath,
		now.Add(30*24*time.Hour),
		1,
	)
	expectServerCertificateMetrics(
		registry,
		files.certPath,
		now.Add(20*24*time.Hour),
		1,
	)

	provider, err := NewGrpcServerTlsProvider(
		ctx,
		[]GrpcServerCertificateConfig{{
			CertFile:       files.certPath,
			PrivateKeyFile: files.keyPath,
		}},
		0, // refreshPeriod
		registry,
	)
	require.NoError(t, err)

	writeServerCertificate(
		t,
		files,
		append(append([]byte(nil), leafPEM...), secondIntermediatePEM...),
		leafKeyPEM,
	)
	provider.refresh(ctx, now)

	require.Equal(
		t,
		leafOf(t, leafPEM),
		selectedLeaf(t, provider, "server.example"),
	)
	require.Equal(
		t,
		leafOf(t, secondIntermediatePEM),
		provider.certificates[0].Certificate[1],
	)
	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcTlsProvidersRejectNegativeRefreshPeriod(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	certPEM, keyPEM := generateCertificate(t, "root", time.Now().Add(24*time.Hour))
	files := newServerCertificateFiles(t)
	writeServerCertificate(t, files, certPEM, keyPEM)

	_, err := NewGrpcClientTlsProvider(
		ctx,
		false,
		GrpcClientTlsProviderConfig{
			RootCertsFile: files.certPath,
			RefreshPeriod: -time.Second,
		},
		metrics_mocks.NewRegistryMock(),
	)
	require.Error(t, err)

	_, err = NewGrpcServerTlsProvider(
		ctx,
		[]GrpcServerCertificateConfig{{
			CertFile:       files.certPath,
			PrivateKeyFile: files.keyPath,
		}},
		-time.Second,
		metrics_mocks.NewRegistryMock(),
	)
	require.Error(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func TestParsePEMCertificates(t *testing.T) {
	firstPEM, keyPEM := generateCertificate(
		t,
		"first",
		time.Now().Add(24*time.Hour),
	)
	secondPEM, _ := generateCertificate(
		t,
		"second",
		time.Now().Add(24*time.Hour),
	)
	join := func(parts ...[]byte) []byte {
		var result []byte
		for _, part := range parts {
			result = append(result, part...)
		}
		return result
	}

	certificates, err := parsePEMCertificates(join(firstPEM, secondPEM))
	require.NoError(t, err)
	require.Len(t, certificates, 2)
	require.Equal(t, "first", certificates[0].Subject.CommonName)
	require.Equal(t, "second", certificates[1].Subject.CommonName)

	// Comments and blocks of other types are skipped.
	certificates, err = parsePEMCertificates(
		join([]byte("# comment\n"), firstPEM, keyPEM, []byte("trailing\n")),
	)
	require.NoError(t, err)
	require.Len(t, certificates, 1)

	invalid := [][]byte{
		nil,
		[]byte("invalid"),
		keyPEM,
		truncatePEM(firstPEM),
		join(firstPEM, truncatePEM(secondPEM)),
		join(truncatePEM(firstPEM), secondPEM),
		// Malformed blocks followed by a valid one are skipped by pem.Decode.
		join(truncatePEM(firstPEM), []byte("\n"), secondPEM),
		join([]byte("-----BEGIN CERTIFICATE-----\ngarbage\n"), secondPEM),
		join(firstPEM, []byte("-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----\n")),
	}
	for i, data := range invalid {
		_, err = parsePEMCertificates(data)
		require.Error(t, err, "case #%v", i)
	}
}
