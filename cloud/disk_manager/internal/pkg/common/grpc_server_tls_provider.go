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

	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	grpc_credentials "google.golang.org/grpc/credentials"
)

////////////////////////////////////////////////////////////////////////////////

// How often certificate validity metrics should be updated.
const certificateValidationPeriod = 24 * time.Hour

// Report a certificate as invalid if it will expire one week after now.
const certificateValidationThreshold = 7 * 24 * time.Hour

////////////////////////////////////////////////////////////////////////////////

// Certificate files are rewritten by external tools, not necessarily
// atomically, and a partially written file may be syntactically valid, e.g. a
// chain without its intermediate certificate. Therefore new content is applied
// only after it has been read unchanged twice in a row (stable-read). This is
// a heuristic that reduces the chance of picking up an intermediate state of a
// rewrite, not a guarantee: a writer that stalls for longer than the check
// interval is indistinguishable from a finished one.
type stableReadDecision int

const (
	stableReadUnchanged stableReadDecision = iota
	stableReadWait
	stableReadApply
)

// Files are checked once per refresh period. New content is re-checked after
// half a period, so that a change takes effect within one and a half periods
// without reading unchanged files more often.
func refreshInterval(period time.Duration, pending bool) time.Duration {
	if pending {
		return max(period/2, time.Nanosecond)
	}

	return period
}

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

// Raw file contents, kept to detect changes without parsing.
type serverCertificatePEM struct {
	cert []byte
	key  []byte
}

func (p serverCertificatePEM) equal(other serverCertificatePEM) bool {
	return bytes.Equal(p.cert, other.cert) && bytes.Equal(p.key, other.key)
}

////////////////////////////////////////////////////////////////////////////////

type GrpcServerTlsProvider struct {
	configs []GrpcServerCertificateConfig

	mutex        sync.RWMutex
	certificates []tls.Certificate
	pems         []serverCertificatePEM
	// Content that differs from pems and has been read once, nil otherwise.
	// See stableReadDecision.
	pendingPEMs []*serverCertificatePEM
	expirations []certificateExpiration
}

// Certificates are loaded once during construction. Unlike on refresh, chains
// are not validated here, e.g. expired certificates are accepted, so that the
// service is able to start: there is no last known good certificate to fall
// back to. Zero refreshPeriod disables automatic certificate refresh.
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
	pems := make([]serverCertificatePEM, 0, len(certs))
	expirations := make([]certificateExpiration, 0, len(certs))
	for _, cert := range certs {
		pem, err := readServerCertificatePEM(cert)
		if err != nil {
			return nil, err
		}

		certificate, chain, err := parseServerCertificate(cert, pem)
		if err != nil {
			return nil, err
		}

		certificates = append(certificates, certificate)
		pems = append(pems, pem)
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
		pems:         pems,
		pendingPEMs:  make([]*serverCertificatePEM, len(certs)),
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
	var refreshTimer *time.Timer
	var refreshTicks <-chan time.Time
	if refreshPeriod > 0 {
		refreshTimer = time.NewTimer(refreshPeriod)
		defer refreshTimer.Stop()
		refreshTicks = refreshTimer.C
	}

	for {
		select {
		case now := <-validityTicker.C:
			p.reportCertificateValidity(now)
		case now := <-refreshTicks:
			pending := p.refresh(ctx, now)
			refreshTimer.Reset(refreshInterval(refreshPeriod, pending))
		case <-ctx.Done():
			return
		}
	}
}

// Reloads certificates from disk. Every certificate is refreshed
// independently. New content is applied only after it has been read unchanged
// twice in a row, a read error restarts the count. The last successfully
// loaded certificate is kept if its files cannot be read or parsed, or if its
// chain is not valid at |now|; new content that fails these checks is reported
// on every tick until the files change. Returns true if any certificate is
// waiting for a stable read.
func (p *GrpcServerTlsProvider) refresh(
	ctx context.Context,
	now time.Time,
) bool {

	pending := false
	for i, config := range p.configs {
		if p.refreshCertificate(ctx, i, config, now) {
			pending = true
		}
	}

	return pending
}

// Returns true if the certificate is waiting for a stable read.
func (p *GrpcServerTlsProvider) refreshCertificate(
	ctx context.Context,
	index int,
	config GrpcServerCertificateConfig,
	now time.Time,
) bool {

	pem, err := readServerCertificatePEM(config)
	if err != nil {
		p.clearPending(index)
		p.warnRefreshFailure(ctx, config, err)
		return false
	}

	switch p.decide(index, pem) {
	case stableReadUnchanged:
		return false
	case stableReadWait:
		logging.Info(
			ctx,
			"New GRPC server certificate %v, waiting for a stable read",
			config.CertFile,
		)
		return true
	}

	certificate, chain, err := parseServerCertificate(config, pem)
	if err == nil {
		err = validateCertificateChain(chain, now)
	}

	if err != nil {
		p.warnRefreshFailure(ctx, config, err)
		return false
	}

	p.mutex.Lock()
	p.certificates[index] = certificate
	p.pems[index] = pem
	p.pendingPEMs[index] = nil
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
	return false
}

func (p *GrpcServerTlsProvider) decide(
	index int,
	pem serverCertificatePEM,
) stableReadDecision {

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.pems[index].equal(pem) {
		p.pendingPEMs[index] = nil
		return stableReadUnchanged
	}

	pending := p.pendingPEMs[index]
	p.pendingPEMs[index] = &pem
	if pending == nil || !pending.equal(pem) {
		return stableReadWait
	}

	return stableReadApply
}

func (p *GrpcServerTlsProvider) clearPending(index int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.pendingPEMs[index] = nil
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

func certificateChainExpiration(chain []*x509.Certificate) time.Time {
	expiration := chain[0].NotAfter
	for _, certificate := range chain[1:] {
		if certificate.NotAfter.Before(expiration) {
			expiration = certificate.NotAfter
		}
	}

	return expiration
}

// Checks that every certificate in the chain is valid at |now| and that the
// chain can be built from the leaf up to the last certificate the same way
// clients do it: issuer names, signatures, CA and name constraints. The last
// certificate serves as the trust anchor: the server has no trust store, and
// whether the chain ends at a trusted root is the client's job anyway.
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

	roots := x509.NewCertPool()
	roots.AddCert(chain[len(chain)-1])

	intermediates := x509.NewCertPool()
	for i := 1; i+1 < len(chain); i++ {
		intermediates.AddCert(chain[i])
	}

	_, err := chain[0].Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		CurrentTime:   now,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	})
	if err != nil {
		return fmt.Errorf("failed to build certificate chain: %w", err)
	}

	return nil
}

func readServerCertificatePEM(
	cert GrpcServerCertificateConfig,
) (serverCertificatePEM, error) {

	certPEM, err := os.ReadFile(cert.CertFile)
	if err != nil {
		return serverCertificatePEM{}, fmt.Errorf(
			"failed to read cert file %v: %w",
			cert.CertFile,
			err,
		)
	}

	keyPEM, err := os.ReadFile(cert.PrivateKeyFile)
	if err != nil {
		return serverCertificatePEM{}, fmt.Errorf(
			"failed to read private key file %v: %w",
			cert.PrivateKeyFile,
			err,
		)
	}

	return serverCertificatePEM{cert: certPEM, key: keyPEM}, nil
}

// Parses a certificate with its private key and the whole chain. The returned
// chain is never empty and its first element is the leaf.
func parseServerCertificate(
	cert GrpcServerCertificateConfig,
	pem serverCertificatePEM,
) (tls.Certificate, []*x509.Certificate, error) {

	certificate, err := tls.X509KeyPair(pem.cert, pem.key)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf(
			"failed to load cert file %v: %w",
			cert.CertFile,
			err,
		)
	}

	if len(certificate.Certificate) == 0 {
		return tls.Certificate{}, nil, fmt.Errorf(
			"certificate chain is empty for cert file %v",
			cert.CertFile,
		)
	}

	chain := make([]*x509.Certificate, 0, len(certificate.Certificate))
	for i, certificateBytes := range certificate.Certificate {
		parsed, err := x509.ParseCertificate(certificateBytes)
		if err != nil {
			return tls.Certificate{}, nil, fmt.Errorf(
				"failed to parse certificate #%v from cert file %v: %w",
				i,
				cert.CertFile,
				err,
			)
		}

		chain = append(chain, parsed)
	}

	certificate.Leaf = chain[0]
	return certificate, chain, nil
}
