package common

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"

	storage_grpc "github.com/ydb-platform/nbs/cloud/storage/core/go/grpc"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	"github.com/ydb-platform/nbs/contrib/go/cityhash"
)

////////////////////////////////////////////////////////////////////////////////

type GRPCClientTLSProviderConfig struct {
	Insecure      bool
	RootCertsFile string
}

type GRPCClientTLSProvider struct {
	mutex     sync.RWMutex
	tlsConfig *tls.Config
}

// A provider is not created for insecure clients or when system roots are used.
func NewGRPCClientTLSProvider(
	config GRPCClientTLSProviderConfig,
	registry metrics.Registry,
) (storage_grpc.TLSConfigProvider, error) {

	if config.Insecure || config.RootCertsFile == "" {
		return nil, nil
	}

	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	rootCerts, err := os.ReadFile(config.RootCertsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read root cert file: %v", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(rootCerts) {
		return nil, errors.New("failed to parse root certificate PEM")
	}

	cfg.RootCAs = pool

	// Metrics gauges use float64. Keep 53 bits so the fingerprint can be
	// represented without precision loss.
	fingerprint := cityhash.Hash64(rootCerts) & ((1 << 53) - 1)
	registry.WithTags(
		map[string]string{
			"subsystem": "certificates",
			"path":      config.RootCertsFile,
		},
	).Gauge("Fingerprint").Set(float64(fingerprint))

	return &GRPCClientTLSProvider{tlsConfig: cfg}, nil
}

func (p *GRPCClientTLSProvider) GetTLSConfig() *tls.Config {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.tlsConfig == nil {
		return nil
	}

	return p.tlsConfig
}
