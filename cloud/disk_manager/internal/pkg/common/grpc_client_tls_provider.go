package common

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/contrib/go/cityhash"
)

////////////////////////////////////////////////////////////////////////////////

type GRPCClientTLSProviderConfig struct {
	RootCertsFile string
}

type GRPCClientTLSProvider struct {
	tlsConfig *tls.Config
}

func NewGRPCClientTLSProvider(
	config GRPCClientTLSProviderConfig,
	registry metrics.Registry,
) (*GRPCClientTLSProvider, error) {
	cfg := &tls.Config{}

	if config.RootCertsFile != "" {
		rootCerts, err := os.ReadFile(config.RootCertsFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read root cert file: %w", err)
		}

		pool := cfg.RootCAs
		if pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(rootCerts) {
			return nil, errors.New("failed to parse root certificate PEM")
		}
		cfg.RootCAs = pool

		fingerprint := cityhash.Hash64(rootCerts) & ((1 << 53) - 1)
		registry.WithTags(
			map[string]string{
				"subsystem": "certificates",
				"cert":      filepath.Base(config.RootCertsFile),
			},
		).Gauge("Fingerprint").Set(float64(fingerprint))
	}

	return &GRPCClientTLSProvider{tlsConfig: cfg}, nil
}

func (p *GRPCClientTLSProvider) GetTLSConfig() *tls.Config {
	if p == nil || p.tlsConfig == nil {
		return nil
	}

	return p.tlsConfig.Clone()
}
