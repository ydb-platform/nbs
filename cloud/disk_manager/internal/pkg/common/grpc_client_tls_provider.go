package common

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	storage_grpc "github.com/ydb-platform/nbs/cloud/storage/core/go/grpc"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	"github.com/ydb-platform/nbs/contrib/go/cityhash"
)

////////////////////////////////////////////////////////////////////////////////

type GrpcClientTlsProviderConfig struct {
	RootCertsFile string
	// Zero disables automatic root certificate refresh.
	RefreshPeriod time.Duration
}

type grpcClientTlsProvider struct {
	rootCertsFile    string
	fingerprintGauge metrics.Gauge

	// Used by the refreshing goroutine only.
	rootCerts  string
	stableRead stableRead[string]

	mutex     sync.RWMutex
	tlsConfig *tls.Config
}

// A provider is not created for insecure clients or when system roots are used.
func NewGrpcClientTlsProvider(
	ctx context.Context,
	insecure bool,
	config GrpcClientTlsProviderConfig,
	registry metrics.Registry,
) (storage_grpc.TlsConfigProvider, error) {

	if insecure || config.RootCertsFile == "" {
		return nil, nil
	}

	if config.RefreshPeriod < 0 {
		return nil, fmt.Errorf(
			"refresh period must not be negative: %v",
			config.RefreshPeriod,
		)
	}

	rootCerts, err := os.ReadFile(config.RootCertsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read root cert file: %w", err)
	}

	tlsConfig, err := newClientTlsConfig(rootCerts)
	if err != nil {
		return nil, err
	}

	provider := &grpcClientTlsProvider{
		rootCertsFile: config.RootCertsFile,
		fingerprintGauge: registry.WithTags(
			map[string]string{
				"subsystem": "certificates",
				"path":      config.RootCertsFile,
			},
		).Gauge("fingerprint"),
		rootCerts: string(rootCerts),
		tlsConfig: tlsConfig,
	}
	provider.fingerprintGauge.Set(float64(rootCertsFingerprint(rootCerts)))

	if config.RefreshPeriod > 0 {
		go provider.refreshLoop(ctx, config.RefreshPeriod)
	}

	return provider, nil
}

func (p *grpcClientTlsProvider) GetTlsConfig() *tls.Config {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	return p.tlsConfig
}

func (p *grpcClientTlsProvider) refreshLoop(
	ctx context.Context,
	period time.Duration,
) {

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.refresh(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// New content is applied once stableRead confirms it. Content that fails to
// load or parse is logged and the previous one is kept.
func (p *grpcClientTlsProvider) refresh(ctx context.Context) {
	rootCerts, err := os.ReadFile(p.rootCertsFile)
	if err != nil {
		p.stableRead.reset()
		p.warnRefreshFailure(ctx, err)
		return
	}

	switch p.stableRead.observe(p.rootCerts, string(rootCerts)) {
	case stableReadUnchanged:
		return
	case stableReadWait:
		logging.Info(
			ctx,
			"New root certificates in %v, waiting for a stable read",
			p.rootCertsFile,
		)
		return
	}

	tlsConfig, err := newClientTlsConfig(rootCerts)
	if err != nil {
		p.warnRefreshFailure(ctx, err)
		return
	}

	p.rootCerts = string(rootCerts)
	p.mutex.Lock()
	p.tlsConfig = tlsConfig
	p.mutex.Unlock()

	fingerprint := rootCertsFingerprint(rootCerts)
	logging.Info(
		ctx,
		"Refreshed root certificates from %v, fingerprint %v",
		p.rootCertsFile,
		fingerprint,
	)

	p.fingerprintGauge.Set(float64(fingerprint))
}

func (p *grpcClientTlsProvider) warnRefreshFailure(
	ctx context.Context,
	err error,
) {

	logging.Warn(
		ctx,
		"Failed to refresh root certificates from %v: %v",
		p.rootCertsFile,
		err,
	)
}

////////////////////////////////////////////////////////////////////////////////

func newClientTlsConfig(rootCerts []byte) (*tls.Config, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(rootCerts) {
		return nil, errors.New("failed to parse root certificate PEM")
	}

	return &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	}, nil
}

func rootCertsFingerprint(rootCerts []byte) uint64 {
	// Metrics gauges use float64. Keep 53 bits so the fingerprint can be
	// represented without precision loss.
	return cityhash.Hash64(rootCerts) & ((1 << 53) - 1)
}
