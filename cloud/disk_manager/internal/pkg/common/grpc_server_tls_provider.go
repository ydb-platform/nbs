package common

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"time"

	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	grpc_credentials "google.golang.org/grpc/credentials"
)

////////////////////////////////////////////////////////////////////////////////

type certificateExpiration struct {
	path  string
	after time.Time
}

type GRPCServerTLSProvider struct {
	certs []*server_config.Cert

	lock         sync.RWMutex
	certificates []tls.Certificate
	expirations  []certificateExpiration
}

func NewGRPCServerTLSProvider(
	ctx context.Context,
	certs []*server_config.Cert,
	refreshPeriod time.Duration,
	registry metrics.Registry,
) (*GRPCServerTLSProvider, error) {
	if len(certs) == 0 {
		return nil, errors.New("empty cert list")
	}

	provider := &GRPCServerTLSProvider{
		certs: certs,
	}

	certificates, expirations, err := provider.readCertificates()
	if err != nil {
		return nil, err
	}

	provider.certificates = certificates
	provider.expirations = expirations
	provider.publishCertificateMetrics(registry)

	if refreshPeriod > 0 {
		go provider.runRefreshLoop(ctx, refreshPeriod, registry)
	}

	return provider, nil
}

func (p *GRPCServerTLSProvider) NewTransportCredentials() grpc_credentials.TransportCredentials {
	cfg := &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: p.getCertificate,
	}
	return grpc_credentials.NewTLS(cfg)
}

func (p *GRPCServerTLSProvider) getCertificate(
	info *tls.ClientHelloInfo,
) (*tls.Certificate, error) {
	p.lock.RLock()
	certificates := p.certificates
	p.lock.RUnlock()

	if len(certificates) == 0 {
		return nil, errors.New("no server certificates loaded")
	}

	if info != nil {
		for _, certificate := range certificates {
			if info.SupportsCertificate(&certificate) == nil {
				result := certificate
				return &result, nil
			}
		}
	}

	result := certificates[0]
	return &result, nil
}

func (p *GRPCServerTLSProvider) runRefreshLoop(
	ctx context.Context,
	refreshPeriod time.Duration,
	registry metrics.Registry,
) {
	ticker := time.NewTicker(refreshPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.lock.RLock()
			certificates := append([]tls.Certificate(nil), p.certificates...)
			expirations := append(
				[]certificateExpiration(nil),
				p.expirations...,
			)
			p.lock.RUnlock()

			for i, cert := range p.certs {
				certificate, expiration, err := p.readCertificate(cert, true)
				if err != nil {
					logging.Warn(
						ctx,
						"Failed to refresh GRPC server certificate %v: %v",
						cert.GetCertFile(),
						err,
					)
					continue
				}

				certificates[i] = certificate
				expirations[i] = expiration
			}

			if len(certificates) == 0 {
				logging.Warn(
					ctx,
					"Failed to refresh GRPC server certificates: no certificates loaded",
				)
				continue
			}

			p.lock.Lock()
			p.certificates = certificates
			p.expirations = expirations
			p.lock.Unlock()
			p.publishCertificateMetrics(registry)
		case <-ctx.Done():
			return
		}
	}
}

func (p *GRPCServerTLSProvider) publishCertificateMetrics(
	registry metrics.Registry,
) {
	p.lock.RLock()
	expirations := make([]certificateExpiration, len(p.expirations))
	copy(expirations, p.expirations)
	p.lock.RUnlock()

	for _, expiration := range expirations {
		gauge := registry.WithTags(
			map[string]string{
				"path": expiration.path,
			},
		).Gauge("ExpireTs")
		gauge.Set(float64(expiration.after.Unix()))
	}
}

func (p *GRPCServerTLSProvider) readCertificates() (
	[]tls.Certificate,
	[]certificateExpiration,
	error,
) {
	certificates := make([]tls.Certificate, 0, len(p.certs))
	expirations := make([]certificateExpiration, 0, len(p.certs))

	for _, cert := range p.certs {
		certificate, expiration, err := p.readCertificate(cert, false)
		if err != nil {
			return nil, nil, err
		}

		certificates = append(certificates, certificate)
		expirations = append(expirations, expiration)
	}

	return certificates, expirations, nil
}

func (p *GRPCServerTLSProvider) readCertificate(
	cert *server_config.Cert,
	validateValidity bool,
) (tls.Certificate, certificateExpiration, error) {
	certificate, err := tls.LoadX509KeyPair(
		cert.GetCertFile(),
		cert.GetPrivateKeyFile(),
	)
	if err != nil {
		return tls.Certificate{}, certificateExpiration{}, fmt.Errorf(
			"failed to load cert file %v: %w",
			cert.CertFile,
			err,
		)
	}
	if len(certificate.Certificate) == 0 {
		return tls.Certificate{}, certificateExpiration{}, fmt.Errorf(
			"certificate chain is empty for cert file %v",
			cert.CertFile,
		)
	}

	parsed, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return tls.Certificate{}, certificateExpiration{}, fmt.Errorf(
			"failed to parse cert file %v: %w",
			cert.CertFile,
			err,
		)
	}
	if validateValidity {
		if err := validateCertificateCurrentlyValid(parsed, time.Now()); err != nil {
			return tls.Certificate{}, certificateExpiration{}, fmt.Errorf(
				"failed to validate cert file %v: %w",
				cert.CertFile,
				err,
			)
		}
	}

	return certificate, certificateExpiration{
		path:  cert.GetCertFile(),
		after: parsed.NotAfter,
	}, nil
}
