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

	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type ReloadableTLSConfigProviderConfig struct {
	UseSystemCertPool  bool
	RootCertsFile      string
	CertFile           string
	CertPrivateKeyFile string
	RefreshPeriod      time.Duration
}

type ReloadableTLSConfigProvider struct {
	config ReloadableTLSConfigProviderConfig

	lock      sync.RWMutex
	tlsConfig *tls.Config
}

func NewReloadableTLSConfigProvider(
	ctx context.Context,
	config ReloadableTLSConfigProviderConfig,
) (*ReloadableTLSConfigProvider, error) {
	provider := &ReloadableTLSConfigProvider{
		config: config,
	}

	cfg, err := provider.readTLSConfig()
	if err != nil {
		return nil, err
	}
	provider.tlsConfig = cfg

	if config.RefreshPeriod > 0 {
		go provider.runRefreshLoop(ctx)
	}

	return provider, nil
}

func (p *ReloadableTLSConfigProvider) GetTLSConfig(
	_ context.Context,
) (*tls.Config, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if p.tlsConfig == nil {
		return nil, errors.New("TLS config is not initialized")
	}

	return p.tlsConfig.Clone(), nil
}

func (p *ReloadableTLSConfigProvider) runRefreshLoop(
	ctx context.Context,
) {
	ticker := time.NewTicker(p.config.RefreshPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cfg, err := p.readTLSConfig()
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
		case <-ctx.Done():
			return
		}
	}
}

func (p *ReloadableTLSConfigProvider) readTLSConfig() (*tls.Config, error) {
	cfg := &tls.Config{}

	if p.config.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(
			p.config.CertFile,
			p.config.CertPrivateKeyFile,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to load client certificate/key: %w",
				err,
			)
		}

		cfg.Certificates = []tls.Certificate{cert}
	}

	if p.config.UseSystemCertPool {
		cp, err := x509.SystemCertPool()
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = cp
	}

	if p.config.RootCertsFile != "" {
		pem, err := os.ReadFile(p.config.RootCertsFile)
		if err != nil {
			return nil, fmt.Errorf(
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
			return nil, errors.New("failed to parse PEM")
		}

		cfg.RootCAs = pool
	}

	return cfg, nil
}
