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

	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	grpc_credentials "google.golang.org/grpc/credentials"
)

////////////////////////////////////////////////////////////////////////////////

// How often certificate validity metrics should be updated.
const certificateValidationPeriod = 24 * time.Hour

// Report a certificate as invalid if it will expire one week after now.
const certificateValidationThreshold = 7 * 24 * time.Hour

type certificateExpiration struct {
	path          string
	after         time.Time
	expireTsGauge metrics.Gauge
	validityGauge metrics.Gauge
}

type GrpcServerCertificateConfig struct {
	CertFile       string
	PrivateKeyFile string
}

////////////////////////////////////////////////////////////////////////////////

type GrpcServerTlsProvider struct {
	configs []GrpcServerCertificateConfig

	mutex        sync.RWMutex
	certificates []tls.Certificate
	expirations  []certificateExpiration
}

// Certificates are loaded once during construction. Certificates that are
// already expired are accepted here so that the service is able to start.
// Zero refreshPeriod disables automatic certificate refresh.
func NewGrpcServerTlsProvider(
	ctx context.Context,
	certs []GrpcServerCertificateConfig,
	refreshPeriod time.Duration,
	registry metrics.Registry,
) (*GrpcServerTlsProvider, error) {

	if refreshPeriod < 0 {
		return nil, fmt.Errorf(
			"refresh period must not be negative: %v",
			refreshPeriod,
		)
	}

	certificates := make([]tls.Certificate, 0, len(certs))
	expirations := make([]certificateExpiration, 0, len(certs))
	for _, cert := range certs {
		certificate, chain, err := readServerCertificate(cert)
		if err != nil {
			return nil, err
		}

		certificates = append(certificates, certificate)
		expirations = append(expirations, certificateExpiration{
			path:  cert.CertFile,
			after: certificateChainExpiration(chain),
		})
	}

	for i := range expirations {
		expiration := &expirations[i]
		certRegistry := registry.WithTags(
			map[string]string{"path": expiration.path},
		)
		expiration.expireTsGauge = certRegistry.Gauge("expireTs")
		expiration.validityGauge = certRegistry.Gauge("certificateValidity")
	}

	provider := &GrpcServerTlsProvider{
		configs:      append([]GrpcServerCertificateConfig(nil), certs...),
		certificates: certificates,
		expirations:  expirations,
	}

	provider.reportCertificateExpirations()
	provider.reportCertificateValidity(time.Now())
	go provider.monitorCertificates(ctx, refreshPeriod)

	return provider, nil
}

func (p *GrpcServerTlsProvider) monitorCertificates(
	ctx context.Context,
	refreshPeriod time.Duration,
) {

	validityTicker := time.NewTicker(certificateValidationPeriod)
	defer validityTicker.Stop()

	// Receiving from a nil channel blocks forever, so refresh is disabled.
	var refreshTicks <-chan time.Time
	if refreshPeriod > 0 {
		refreshTicker := time.NewTicker(refreshPeriod)
		defer refreshTicker.Stop()
		refreshTicks = refreshTicker.C
	}

	for {
		select {
		case now := <-validityTicker.C:
			p.reportCertificateValidity(now)
		case now := <-refreshTicks:
			p.refresh(ctx, now)
		case <-ctx.Done():
			return
		}
	}
}

// Reloads certificates from disk. Every certificate is refreshed
// independently: the last successfully loaded one is kept if its files cannot
// be read or parsed, or if its chain is not valid at |now|.
func (p *GrpcServerTlsProvider) refresh(ctx context.Context, now time.Time) {
	for i, config := range p.configs {
		p.refreshCertificate(ctx, i, config, now)
	}
}

func (p *GrpcServerTlsProvider) refreshCertificate(
	ctx context.Context,
	index int,
	config GrpcServerCertificateConfig,
	now time.Time,
) {

	certificate, chain, err := readServerCertificate(config)
	if err != nil {
		p.warnRefreshFailure(ctx, config, err)
		return
	}

	p.mutex.RLock()
	unchanged := equalCertificateChains(
		p.certificates[index].Certificate,
		certificate.Certificate,
	)
	p.mutex.RUnlock()

	if unchanged {
		return
	}

	err = validateCertificateChain(chain, now)
	if err != nil {
		p.warnRefreshFailure(ctx, config, err)
		return
	}

	p.mutex.Lock()
	p.certificates[index] = certificate
	p.expirations[index].after = certificateChainExpiration(chain)
	expiration := p.expirations[index]
	p.mutex.Unlock()

	logging.Info(
		ctx,
		"Refreshed GRPC server certificate %v, expires at %v",
		config.CertFile,
		expiration.after,
	)

	expiration.expireTsGauge.Set(float64(expiration.after.Unix()))
	expiration.validityGauge.Set(certificateValidity(expiration.after, now))
}

func (p *GrpcServerTlsProvider) warnRefreshFailure(
	ctx context.Context,
	config GrpcServerCertificateConfig,
	err error,
) {

	logging.Warn(
		ctx,
		"Failed to refresh GRPC server certificate %v: %v",
		config.CertFile,
		err,
	)
}

func (p *GrpcServerTlsProvider) reportCertificateExpirations() {
	expirations := p.getExpirations()
	for _, expiration := range expirations {
		expiration.expireTsGauge.Set(float64(expiration.after.Unix()))
	}
}

func (p *GrpcServerTlsProvider) reportCertificateValidity(
	now time.Time,
) {

	expirations := p.getExpirations()
	for _, expiration := range expirations {
		expiration.validityGauge.Set(
			certificateValidity(expiration.after, now),
		)
	}
}

func (p *GrpcServerTlsProvider) getExpirations() []certificateExpiration {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	return append([]certificateExpiration(nil), p.expirations...)
}

func (p *GrpcServerTlsProvider) NewTransportCredentials() grpc_credentials.TransportCredentials {

	cfg := &tls.Config{
		GetCertificate: p.getCertificate,
		MinVersion:     tls.VersionTLS12,
	}

	return grpc_credentials.NewTLS(cfg)
}

func (p *GrpcServerTlsProvider) getCertificate(
	info *tls.ClientHelloInfo,
) (*tls.Certificate, error) {

	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if len(p.certificates) == 0 {
		return nil, errors.New("no server certificates loaded")
	}

	if info != nil {
		for _, certificate := range p.certificates {
			if info.SupportsCertificate(&certificate) == nil {
				return &certificate, nil
			}
		}
	}

	certificate := p.certificates[0]
	return &certificate, nil
}

////////////////////////////////////////////////////////////////////////////////

func certificateValidity(expiration time.Time, now time.Time) float64 {
	if expiration.Sub(now) <= certificateValidationThreshold {
		return 0
	}

	return 1
}

// Returns the earliest NotAfter value from the whole chain.
func certificateChainExpiration(chain []*x509.Certificate) time.Time {
	expiration := chain[0].NotAfter
	for _, certificate := range chain[1:] {
		if certificate.NotAfter.Before(expiration) {
			expiration = certificate.NotAfter
		}
	}

	return expiration
}

// Checks that every certificate in the chain is valid at |now|.
func validateCertificateChain(
	chain []*x509.Certificate,
	now time.Time,
) error {

	for i, certificate := range chain {
		if now.Before(certificate.NotBefore) {
			return fmt.Errorf(
				"certificate #%v is not valid before %v",
				i,
				certificate.NotBefore,
			)
		}

		if !now.Before(certificate.NotAfter) {
			return fmt.Errorf(
				"certificate #%v expired at %v",
				i,
				certificate.NotAfter,
			)
		}
	}

	return nil
}

func equalCertificateChains(first [][]byte, second [][]byte) bool {
	if len(first) != len(second) {
		return false
	}

	for i := range first {
		if !bytes.Equal(first[i], second[i]) {
			return false
		}
	}

	return true
}

// Loads a certificate with its private key and parses the whole chain. The
// returned chain is never empty and its first element is the leaf.
func readServerCertificate(
	cert GrpcServerCertificateConfig,
) (tls.Certificate, []*x509.Certificate, error) {

	certPEM, err := os.ReadFile(cert.CertFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf(
			"failed to read cert file %v: %w",
			cert.CertFile,
			err,
		)
	}

	keyPEM, err := os.ReadFile(cert.PrivateKeyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf(
			"failed to read private key file %v: %w",
			cert.PrivateKeyFile,
			err,
		)
	}

	chain, err := parsePEMCertificates(certPEM)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf(
			"failed to parse cert file %v: %w",
			cert.CertFile,
			err,
		)
	}

	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, task_errors.NewNonRetriableErrorf(
			"failed to load cert file %v: %w",
			cert.CertFile,
			err,
		)
	}

	if len(certificate.Certificate) != len(chain) {
		return tls.Certificate{}, nil, fmt.Errorf(
			"unexpected number of certificates in cert file %v: %v != %v",
			cert.CertFile,
			len(certificate.Certificate),
			len(chain),
		)
	}

	certificate.Leaf = chain[0]
	return certificate, chain, nil
}
