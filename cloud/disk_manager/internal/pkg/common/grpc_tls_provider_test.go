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
	storage_grpc "github.com/ydb-platform/nbs/cloud/storage/core/go/grpc"
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
	intermediatePEM, intermediateKeyPEM := generateCertificate(
		t,
		"intermediate",
		now.Add(24*time.Hour),
	)
	leafPEM, leafKeyPEM := generateSignedCertificate(
		t,
		"leaf",
		now.Add(-time.Hour),
		now.Add(48*time.Hour),
		intermediatePEM,
		intermediateKeyPEM,
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

	key, keyPEM := generatePrivateKey(t)
	return createCertificate(
		t,
		commonName,
		notBefore,
		notAfter,
		key,
		nil, // issuer
		nil, // issuerKey
	), keyPEM
}

// Self-signed certificate for an existing key, e.g. a re-issued CA.
func generateCertificateForKey(
	t *testing.T,
	commonName string,
	notBefore time.Time,
	notAfter time.Time,
	keyPEM []byte,
) []byte {

	t.Helper()

	key := parsePrivateKeyPEM(t, keyPEM)
	return createCertificate(
		t,
		commonName,
		notBefore,
		notAfter,
		key,
		nil, // issuer
		nil, // issuerKey
	)
}

// Certificate with a fresh key signed by the issuer.
func generateSignedCertificate(
	t *testing.T,
	commonName string,
	notBefore time.Time,
	notAfter time.Time,
	issuerPEM []byte,
	issuerKeyPEM []byte,
) ([]byte, []byte) {

	t.Helper()

	key, keyPEM := generatePrivateKey(t)
	return createCertificate(
		t,
		commonName,
		notBefore,
		notAfter,
		key,
		parseCertificatePEM(t, issuerPEM),
		parsePrivateKeyPEM(t, issuerKeyPEM),
	), keyPEM
}

func generatePrivateKey(t *testing.T) (*ecdsa.PrivateKey, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	return key, pem.EncodeToMemory(
		&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER},
	)
}

func parsePrivateKeyPEM(t *testing.T, keyPEM []byte) *ecdsa.PrivateKey {
	t.Helper()

	block, _ := pem.Decode(keyPEM)
	require.NotNil(t, block)
	key, err := x509.ParseECPrivateKey(block.Bytes)
	require.NoError(t, err)
	return key
}

func parseCertificatePEM(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()

	certificate, err := x509.ParseCertificate(leafOf(t, certPEM))
	require.NoError(t, err)
	return certificate
}

// Self-signed if issuer is nil.
func createCertificate(
	t *testing.T,
	commonName string,
	notBefore time.Time,
	notAfter time.Time,
	key *ecdsa.PrivateKey,
	issuer *x509.Certificate,
	issuerKey *ecdsa.PrivateKey,
) []byte {

	t.Helper()

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

	if issuer == nil {
		issuer = template
		issuerKey = key
	}

	der, err := x509.CreateCertificate(
		rand.Reader,
		template,
		issuer,
		&key.PublicKey,
		issuerKey,
	)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func readServerCertificate(
	cert GrpcServerCertificateConfig,
) (tls.Certificate, []*x509.Certificate, error) {

	pem, err := readServerCertificatePEM(cert)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	return parseServerCertificate(cert, pem)
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
	refreshClientUntilStable(ctx, provider)

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
	refreshClientUntilStable(ctx, provider)
	require.Same(t, expectedConfig, provider.GetTlsConfig())

	require.NoError(t, os.WriteFile(certPath, []byte("invalid"), 0o600))
	refreshClientUntilStable(ctx, provider)
	require.Same(t, expectedConfig, provider.GetTlsConfig())

	require.NoError(t, os.Remove(certPath))
	refreshClientUntilStable(ctx, provider)
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
	refreshServerUntilStable(ctx, provider, now)

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
	refreshServerUntilStable(ctx, provider, now)
	require.Equal(
		t,
		leafOf(t, certPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	writeServerCertificate(t, files, []byte("invalid"), keyPEM)
	refreshServerUntilStable(ctx, provider, now)
	require.Equal(
		t,
		leafOf(t, certPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	// Certificate does not match the private key.
	writeServerCertificate(t, files, otherPEM, keyPEM)
	refreshServerUntilStable(ctx, provider, now)
	require.Equal(
		t,
		leafOf(t, certPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	require.NoError(t, os.Remove(files.certPath))
	require.NoError(t, os.Remove(files.keyPath))
	refreshServerUntilStable(ctx, provider, now)
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
	refreshServerUntilStable(ctx, provider, now)
	require.Equal(
		t,
		leafOf(t, certPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	writeServerCertificate(t, files, futurePEM, futureKeyPEM)
	refreshServerUntilStable(ctx, provider, now)
	require.Equal(
		t,
		leafOf(t, certPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	// Expired intermediate certificate invalidates the whole chain.
	signedPEM, signedKeyPEM := generateSignedCertificate(
		t,
		"server.example",
		now.Add(-time.Hour),
		now.Add(30*24*time.Hour),
		expiredPEM,
		expiredKeyPEM,
	)
	writeServerCertificate(
		t,
		files,
		append(append([]byte(nil), signedPEM...), expiredPEM...),
		signedKeyPEM,
	)
	refreshServerUntilStable(ctx, provider, now)
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
	refreshServerUntilStable(ctx, provider, now)

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

func TestGrpcServerTlsProviderRefreshDetectsIntermediateChange(
	t *testing.T,
) {

	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	now := time.Now().Truncate(time.Second)
	files := newServerCertificateFiles(t)
	// The intermediate certificate is re-issued for the same key, so the leaf
	// certificate is valid with both.
	_, intermediateKeyPEM := generatePrivateKey(t)
	firstIntermediatePEM := generateCertificateForKey(
		t,
		"intermediate",
		now.Add(-time.Hour),
		now.Add(30*24*time.Hour),
		intermediateKeyPEM,
	)
	secondIntermediatePEM := generateCertificateForKey(
		t,
		"intermediate",
		now.Add(-time.Hour),
		now.Add(20*24*time.Hour),
		intermediateKeyPEM,
	)
	leafPEM, leafKeyPEM := generateSignedCertificate(
		t,
		"server.example",
		now.Add(-time.Hour),
		now.Add(60*24*time.Hour),
		firstIntermediatePEM,
		intermediateKeyPEM,
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
	refreshServerUntilStable(ctx, provider, now)

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

func TestReadServerCertificateReturnsPlainErrors(t *testing.T) {
	certPEM, _ := generateCertificate(t, "server", time.Now().Add(24*time.Hour))
	_, otherKeyPEM := generateCertificate(t, "other", time.Now().Add(24*time.Hour))
	files := newServerCertificateFiles(t)
	writeServerCertificate(t, files, certPEM, otherKeyPEM)

	_, _, err := readServerCertificate(GrpcServerCertificateConfig{
		CertFile:       files.certPath,
		PrivateKeyFile: files.keyPath,
	})
	require.Error(t, err)

	// Errors are logged on every refresh tick, so they must not carry a stack
	// trace.
	require.NotContains(t, err.Error(), "\n")
}

////////////////////////////////////////////////////////////////////////////////

// New content is applied only after it has been read unchanged twice in a row.
func refreshClientUntilStable(
	ctx context.Context,
	provider storage_grpc.TlsConfigProvider,
) {

	for i := 0; i < 2; i++ {
		provider.(*grpcClientTlsProvider).refresh(ctx)
	}
}

func refreshServerUntilStable(
	ctx context.Context,
	provider *GrpcServerTlsProvider,
	now time.Time,
) {

	for i := 0; i < 2; i++ {
		provider.refresh(ctx, now)
	}
}

func TestGrpcClientTlsProviderRefreshWaitsForStableRootCertificates(
	t *testing.T,
) {

	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	firstPEM, _ := generateCertificate(t, "first", time.Now().Add(24*time.Hour))
	secondPEM, _ := generateCertificate(t, "second", time.Now().Add(24*time.Hour))
	thirdPEM, _ := generateCertificate(t, "third", time.Now().Add(24*time.Hour))
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
	gauge.On("Set", float64(fingerprintOf(thirdPEM))).Once()

	provider, err := NewGrpcClientTlsProvider(
		ctx,
		false,
		GrpcClientTlsProviderConfig{RootCertsFile: certPath},
		registry,
	)
	require.NoError(t, err)
	expectedConfig := provider.GetTlsConfig()

	// Content seen once is not applied yet.
	require.NoError(t, os.WriteFile(certPath, secondPEM, 0o600))
	provider.(*grpcClientTlsProvider).refresh(ctx)
	require.Same(t, expectedConfig, provider.GetTlsConfig())

	// Content changed again, so it is still not stable.
	require.NoError(t, os.WriteFile(certPath, thirdPEM, 0o600))
	provider.(*grpcClientTlsProvider).refresh(ctx)
	require.Same(t, expectedConfig, provider.GetTlsConfig())

	provider.(*grpcClientTlsProvider).refresh(ctx)
	require.True(
		t,
		provider.GetTlsConfig().RootCAs.Equal(newCertPool(t, thirdPEM)),
	)
	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcServerTlsProviderRefreshWaitsForStableFiles(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	now := time.Now().Truncate(time.Second)
	files := newServerCertificateFiles(t)
	firstIntermediatePEM, firstIntermediateKeyPEM := generateCertificate(
		t,
		"intermediate",
		now.Add(30*24*time.Hour),
	)
	firstLeafPEM, firstKeyPEM := generateSignedCertificate(
		t,
		"server.example",
		now.Add(-time.Hour),
		now.Add(60*24*time.Hour),
		firstIntermediatePEM,
		firstIntermediateKeyPEM,
	)
	secondIntermediatePEM, secondIntermediateKeyPEM := generateCertificate(
		t,
		"intermediate",
		now.Add(45*24*time.Hour),
	)
	secondLeafPEM, secondKeyPEM := generateSignedCertificate(
		t,
		"server.example",
		now.Add(-time.Hour),
		now.Add(90*24*time.Hour),
		secondIntermediatePEM,
		secondIntermediateKeyPEM,
	)
	writeServerCertificate(
		t,
		files,
		append(append([]byte(nil), firstLeafPEM...), firstIntermediatePEM...),
		firstKeyPEM,
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
		now.Add(45*24*time.Hour),
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

	// A non-atomic writer has written the new key and the new leaf, but not
	// the intermediate certificate yet. The chain is valid on its own.
	writeServerCertificate(t, files, secondLeafPEM, secondKeyPEM)
	provider.refresh(ctx, now)
	require.Equal(
		t,
		leafOf(t, firstLeafPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	// The writer has finished, but the content differs from the previous
	// read, so it is still not applied.
	writeServerCertificate(
		t,
		files,
		append(append([]byte(nil), secondLeafPEM...), secondIntermediatePEM...),
		secondKeyPEM,
	)
	provider.refresh(ctx, now)
	require.Equal(
		t,
		leafOf(t, firstLeafPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	provider.refresh(ctx, now)
	require.Equal(
		t,
		leafOf(t, secondLeafPEM),
		selectedLeaf(t, provider, "server.example"),
	)
	require.Len(t, provider.certificates[0].Certificate, 2)
	require.True(t, registry.AssertAllExpectations(t))
}

func TestRefreshInterval(t *testing.T) {
	// Files are checked once per period, new content is re-checked after half
	// a period.
	require.Equal(t, 10*time.Second, refreshInterval(10*time.Second, false))
	require.Equal(t, 5*time.Second, refreshInterval(10*time.Second, true))
	require.Equal(t, time.Nanosecond, refreshInterval(time.Nanosecond, true))
}

func TestGrpcServerTlsProviderRefreshRejectsBrokenChain(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	now := time.Now().Truncate(time.Second)
	files := newServerCertificateFiles(t)
	intermediatePEM, intermediateKeyPEM := generateCertificate(
		t,
		"intermediate",
		now.Add(30*24*time.Hour),
	)
	leafPEM, leafKeyPEM := generateSignedCertificate(
		t,
		"server.example",
		now.Add(-time.Hour),
		now.Add(60*24*time.Hour),
		intermediatePEM,
		intermediateKeyPEM,
	)
	unrelatedPEM, _ := generateCertificate(
		t,
		"intermediate",
		now.Add(30*24*time.Hour),
	)
	newLeafPEM, newLeafKeyPEM := generateSignedCertificate(
		t,
		"server.example",
		now.Add(-time.Hour),
		now.Add(90*24*time.Hour),
		intermediatePEM,
		intermediateKeyPEM,
	)
	writeServerCertificate(
		t,
		files,
		append(append([]byte(nil), leafPEM...), intermediatePEM...),
		leafKeyPEM,
	)

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

	// Leaf is not signed by the intermediate certificate.
	writeServerCertificate(
		t,
		files,
		append(append([]byte(nil), newLeafPEM...), unrelatedPEM...),
		newLeafKeyPEM,
	)
	refreshServerUntilStable(ctx, provider, now)
	require.Equal(
		t,
		leafOf(t, leafPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	// Intermediate certificate is not signed by the next one.
	writeServerCertificate(
		t,
		files,
		append(
			append(append([]byte(nil), newLeafPEM...), intermediatePEM...),
			unrelatedPEM...,
		),
		newLeafKeyPEM,
	)
	refreshServerUntilStable(ctx, provider, now)
	require.Equal(
		t,
		leafOf(t, leafPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	// Intermediate certificate has been re-issued for the same key with a
	// different subject, so the signature is valid but the issuer name of the
	// leaf certificate does not match and clients cannot build the chain.
	renamedIntermediatePEM := generateCertificateForKey(
		t,
		"intermediate-renamed",
		now.Add(-time.Hour),
		now.Add(30*24*time.Hour),
		intermediateKeyPEM,
	)
	writeServerCertificate(
		t,
		files,
		append(append([]byte(nil), newLeafPEM...), renamedIntermediatePEM...),
		newLeafKeyPEM,
	)
	refreshServerUntilStable(ctx, provider, now)
	require.Equal(
		t,
		leafOf(t, leafPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcClientTlsProviderReadErrorResetsPendingRootCertificates(
	t *testing.T,
) {

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
	expectedConfig := provider.GetTlsConfig()

	require.NoError(t, os.WriteFile(certPath, secondPEM, 0o600))
	provider.(*grpcClientTlsProvider).refresh(ctx)
	require.Same(t, expectedConfig, provider.GetTlsConfig())

	// A read error, e.g. in the middle of a non-atomic rotation, restarts the
	// stable-read.
	require.NoError(t, os.Remove(certPath))
	provider.(*grpcClientTlsProvider).refresh(ctx)
	require.Same(t, expectedConfig, provider.GetTlsConfig())

	require.NoError(t, os.WriteFile(certPath, secondPEM, 0o600))
	provider.(*grpcClientTlsProvider).refresh(ctx)
	require.Same(t, expectedConfig, provider.GetTlsConfig())

	provider.(*grpcClientTlsProvider).refresh(ctx)
	require.True(
		t,
		provider.GetTlsConfig().RootCAs.Equal(newCertPool(t, secondPEM)),
	)
	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcServerTlsProviderReadErrorResetsPendingFiles(t *testing.T) {
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

	writeServerCertificate(t, files, secondPEM, secondKeyPEM)
	provider.refresh(ctx, now)
	require.Equal(
		t,
		leafOf(t, firstPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	// A read error, e.g. in the middle of a non-atomic rotation, restarts the
	// stable-read.
	require.NoError(t, os.Remove(files.keyPath))
	provider.refresh(ctx, now)
	require.Equal(
		t,
		leafOf(t, firstPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	writeServerCertificate(t, files, secondPEM, secondKeyPEM)
	provider.refresh(ctx, now)
	require.Equal(
		t,
		leafOf(t, firstPEM),
		selectedLeaf(t, provider, "server.example"),
	)

	provider.refresh(ctx, now)
	require.Equal(
		t,
		leafOf(t, secondPEM),
		selectedLeaf(t, provider, "server.example"),
	)
	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcClientTlsProviderRefreshReportsPendingContent(t *testing.T) {
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
	client := provider.(*grpcClientTlsProvider)

	// Unchanged file.
	require.False(t, client.refresh(ctx))

	require.NoError(t, os.WriteFile(certPath, secondPEM, 0o600))
	require.True(t, client.refresh(ctx))
	require.False(t, client.refresh(ctx))

	// Stable but invalid content is not pending anymore.
	require.NoError(t, os.WriteFile(certPath, []byte("invalid"), 0o600))
	require.True(t, client.refresh(ctx))
	require.False(t, client.refresh(ctx))

	require.NoError(t, os.Remove(certPath))
	require.False(t, client.refresh(ctx))

	require.True(t, registry.AssertAllExpectations(t))
}

func TestGrpcServerTlsProviderRefreshReportsPendingFiles(t *testing.T) {
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

	// Unchanged files.
	require.False(t, provider.refresh(ctx, now))

	// Any pending certificate makes the whole refresh pending.
	writeServerCertificate(t, firstFiles, newFirstPEM, newFirstKeyPEM)
	require.True(t, provider.refresh(ctx, now))
	require.False(t, provider.refresh(ctx, now))
	require.Equal(
		t,
		leafOf(t, newFirstPEM),
		selectedLeaf(t, provider, "first.example"),
	)

	// Stable but invalid content is not pending anymore.
	writeServerCertificate(t, secondFiles, []byte("invalid"), secondKeyPEM)
	require.True(t, provider.refresh(ctx, now))
	require.False(t, provider.refresh(ctx, now))

	require.NoError(t, os.Remove(secondFiles.certPath))
	require.False(t, provider.refresh(ctx, now))

	require.True(t, registry.AssertAllExpectations(t))
}
