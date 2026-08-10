package common

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/contrib/go/cityhash"
)

////////////////////////////////////////////////////////////////////////////////

type GRPCClientTLSProviderConfig struct {
	UseSystemCertPool  bool
	RootCertsFile      string
	CertFile           string
	CertPrivateKeyFile string
	RefreshPeriod      time.Duration
}

type GRPCClientTLSProvider struct {
	config   GRPCClientTLSProviderConfig
	registry metrics.Registry

	lock      sync.RWMutex
	tlsConfig *tls.Config
}

func NewGRPCClientTLSProvider(
	ctx context.Context,
	config GRPCClientTLSProviderConfig,
	registry metrics.Registry,
) (*GRPCClientTLSProvider, error) {
	provider := &GRPCClientTLSProvider{
		config:   config,
		registry: registry,
	}

	cfg, rootCertFingerprint, err := provider.readTLSConfig()
	if err != nil {
		return nil, err
	}
	provider.tlsConfig = cfg
	provider.publishRootCertFingerprint(rootCertFingerprint)

	if config.RefreshPeriod > 0 {
		go provider.runRefreshLoop(ctx)
	}

	return provider, nil
}

func (p *GRPCClientTLSProvider) GetTLSConfig(
	_ context.Context,
) (*tls.Config, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if p.tlsConfig == nil {
		return nil, errors.New("TLS config is not initialized")
	}

	return p.tlsConfig.Clone(), nil
}

func (p *GRPCClientTLSProvider) runRefreshLoop(
	ctx context.Context,
) {
	ticker := time.NewTicker(p.config.RefreshPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cfg, rootCertFingerprint, err := p.readTLSConfig()
			if err != nil {
				logging.Warn(
					ctx,
					"Failed to refresh TLS config: %v",
					err,
				)
				continue
			}

			p.lock.Lock()
			p.tlsConfig = cfg
			p.lock.Unlock()
			p.publishRootCertFingerprint(rootCertFingerprint)
		case <-ctx.Done():
			return
		}
	}
}

func (p *GRPCClientTLSProvider) publishRootCertFingerprint(
	fingerprint *uint64,
) {
	if fingerprint == nil {
		return
	}

	p.registry.WithTags(
		map[string]string{
			"subsystem": "certificates",
			"cert":      filepath.Base(p.config.RootCertsFile),
		},
	).Gauge("Fingerprint").Set(float64(*fingerprint))
}

func (p *GRPCClientTLSProvider) readTLSConfig() (
	*tls.Config,
	*uint64,
	error,
) {
	cfg := &tls.Config{}

	if p.config.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(
			p.config.CertFile,
			p.config.CertPrivateKeyFile,
		)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"failed to load client certificate/key: %w",
				err,
			)
		}

		cfg.Certificates = []tls.Certificate{cert}
	}

	if p.config.UseSystemCertPool {
		cp, err := x509.SystemCertPool()
		if err != nil {
			return nil, nil, err
		}
		cfg.RootCAs = cp
	}

	var rootCertFingerprint *uint64
	if p.config.RootCertsFile != "" {
		pem, err := os.ReadFile(p.config.RootCertsFile)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"failed to read root cert file: %w",
				err,
			)
		}

		pool := cfg.RootCAs
		if pool == nil {
			pool = x509.NewCertPool()
		}

		ok := pool.AppendCertsFromPEM(pem)
		if !ok {
			return nil, nil, errors.New("failed to parse PEM")
		}

		cfg.RootCAs = pool
		fingerprint := cityhash.Hash64(pem) & ((1 << 53) - 1)
		rootCertFingerprint = &fingerprint
	}

	return cfg, rootCertFingerprint, nil
}
