package common

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
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

type GRPCServerCertificateConfig struct {
	CertFile       string
	PrivateKeyFile string
}

////////////////////////////////////////////////////////////////////////////////

type GRPCServerTLSProvider struct {
	mutex        sync.RWMutex
	certificates []tls.Certificate
	expirations  []certificateExpiration
}

func NewGRPCServerTLSProvider(
	ctx context.Context,
	certs []GRPCServerCertificateConfig,
	registry metrics.Registry,
) (*GRPCServerTLSProvider, error) {
	certificates := make([]tls.Certificate, 0, len(certs))
	expirations := make([]certificateExpiration, 0, len(certs))
	for _, cert := range certs {
		certificate, expiration, err := readServerCertificate(cert)
		if err != nil {
			return nil, err
		}

		certificates = append(certificates, certificate)
		expirations = append(expirations, certificateExpiration{
			path:  cert.CertFile,
			after: expiration,
		})
	}

	for i := range expirations {
		expiration := &expirations[i]
		certRegistry := registry.WithTags(
			map[string]string{"path": expiration.path},
		)
		expiration.expireTsGauge = certRegistry.Gauge("ExpireTs")
		expiration.validityGauge = certRegistry.Gauge("certificateValidity")
	}

	provider := &GRPCServerTLSProvider{
		certificates: certificates,
		expirations:  expirations,
	}
	provider.reportCertificateExpirations()
	provider.reportCertificateValidity(time.Now())
	go provider.monitorCertificateValidity(ctx)

	return provider, nil
}

func (p *GRPCServerTLSProvider) monitorCertificateValidity(
	ctx context.Context,
) {
	ticker := time.NewTicker(certificateValidationPeriod)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			p.reportCertificateValidity(now)
		case <-ctx.Done():
			return
		}
	}
}

func (p *GRPCServerTLSProvider) reportCertificateExpirations() {
	expirations := p.getCertificateExpirations()
	for _, expiration := range expirations {
		expiration.expireTsGauge.Set(float64(expiration.after.Unix()))
	}
}

func (p *GRPCServerTLSProvider) reportCertificateValidity(
	now time.Time,
) {
	expirations := p.getCertificateExpirations()
	for _, expiration := range expirations {
		validity := float64(1)
		if expiration.after.Sub(now) <= certificateValidationThreshold {
			validity = 0
		}
		expiration.validityGauge.Set(validity)
	}
}

func (p *GRPCServerTLSProvider) getCertificateExpirations() []certificateExpiration {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	return append([]certificateExpiration(nil), p.expirations...)
}

func (p *GRPCServerTLSProvider) NewTransportCredentials() grpc_credentials.TransportCredentials {
	cfg := &tls.Config{
		GetCertificate: p.getCertificate,
		MinVersion:     tls.VersionTLS12,
	}
	return grpc_credentials.NewTLS(cfg)
}

func (p *GRPCServerTLSProvider) getCertificate(
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
				result := certificate
				return &result, nil
			}
		}
	}

	result := p.certificates[0]
	return &result, nil
}

func readServerCertificate(
	cert GRPCServerCertificateConfig,
) (tls.Certificate, time.Time, error) {
	certificate, err := tls.LoadX509KeyPair(
		cert.CertFile,
		cert.PrivateKeyFile,
	)
	if err != nil {
		return tls.Certificate{}, time.Time{}, task_errors.NewNonRetriableErrorf(
			"failed to load cert file %v: %w",
			cert.CertFile,
			err,
		)
	}
	if len(certificate.Certificate) == 0 {
		return tls.Certificate{}, time.Time{}, fmt.Errorf(
			"certificate chain is empty for cert file %v",
			cert.CertFile,
		)
	}

	var expiration time.Time
	for i, certificateBytes := range certificate.Certificate {
		parsed, err := x509.ParseCertificate(certificateBytes)
		if err != nil {
			return tls.Certificate{}, time.Time{}, fmt.Errorf(
				"failed to parse certificate #%v from cert file %v: %w",
				i,
				cert.CertFile,
				err,
			)
		}
		if expiration.IsZero() || parsed.NotAfter.Before(expiration) {
			expiration = parsed.NotAfter
		}
	}

	return certificate, expiration, nil
}
