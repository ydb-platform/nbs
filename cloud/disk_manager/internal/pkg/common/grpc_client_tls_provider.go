package common

import (
	"bytes"
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

	mutex     sync.RWMutex
	rootCerts []byte
	tlsConfig *tls.Config
	// Content that differs from rootCerts and has been read once, see
	// stableReadDecision.
	pendingRootCerts    []byte
	hasPendingRootCerts bool
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

	provider := &grpcClientTlsProvider{
		rootCertsFile: config.RootCertsFile,
		fingerprintGauge: registry.WithTags(
			map[string]string{
				"subsystem": "certificates",
				"path":      config.RootCertsFile,
			},
		).Gauge("fingerprint"),
	}

	rootCerts, err := os.ReadFile(config.RootCertsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read root cert file: %w", err)
	}

	tlsConfig, err := newClientTlsConfig(rootCerts)
	if err != nil {
		return nil, err
	}

	provider.rootCerts = rootCerts
	provider.tlsConfig = tlsConfig
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

	timer := time.NewTimer(period)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			pending := p.refresh(ctx)
			timer.Reset(refreshInterval(period, pending))
		case <-ctx.Done():
			return
		}
	}
}

// Reloads root certificates from disk. New content is applied only after it
// has been read unchanged twice in a row, a read error restarts the count. The
// last successfully loaded config is kept if the file cannot be read or
// parsed; new content that fails to parse is reported on every tick until the
// file changes. Returns true if new content is waiting for a stable read.
func (p *grpcClientTlsProvider) refresh(ctx context.Context) bool {
	rootCerts, err := os.ReadFile(p.rootCertsFile)
	if err != nil {
		p.clearPending()
		p.warnRefreshFailure(ctx, err)
		return false
	}

	switch p.decide(rootCerts) {
	case stableReadUnchanged:
		return false
	case stableReadWait:
		logging.Info(
			ctx,
			"New root certificates in %v, waiting for a stable read",
			p.rootCertsFile,
		)
		return true
	}

	tlsConfig, err := newClientTlsConfig(rootCerts)
	if err != nil {
		p.warnRefreshFailure(ctx, err)
		return false
	}

	p.mutex.Lock()
	p.rootCerts = rootCerts
	p.tlsConfig = tlsConfig
	p.pendingRootCerts = nil
	p.hasPendingRootCerts = false
	p.mutex.Unlock()

	fingerprint := rootCertsFingerprint(rootCerts)
	logging.Info(
		ctx,
		"Refreshed root certificates from %v, fingerprint %v",
		p.rootCertsFile,
		fingerprint,
	)

	p.fingerprintGauge.Set(float64(fingerprint))
	return false
}

func (p *grpcClientTlsProvider) decide(rootCerts []byte) stableReadDecision {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if bytes.Equal(p.rootCerts, rootCerts) {
		p.pendingRootCerts = nil
		p.hasPendingRootCerts = false
		return stableReadUnchanged
	}

	stable := p.hasPendingRootCerts &&
		bytes.Equal(p.pendingRootCerts, rootCerts)
	p.pendingRootCerts = rootCerts
	p.hasPendingRootCerts = true
	if !stable {
		return stableReadWait
	}

	return stableReadApply
}

func (p *grpcClientTlsProvider) clearPending() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.pendingRootCerts = nil
	p.hasPendingRootCerts = false
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
