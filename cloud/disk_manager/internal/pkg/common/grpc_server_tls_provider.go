package common

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
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
	path  string
	after time.Time
	gauge metrics.Gauge
}

////////////////////////////////////////////////////////////////////////////////

type GRPCServerTLSProvider struct {
	certificates []tls.Certificate
}

func NewGRPCServerTLSProvider(
	ctx context.Context,
	certs []*server_config.Cert,
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
			path:  cert.GetCertFile(),
			after: expiration,
		})
	}

	for i := range expirations {
		expiration := &expirations[i]
		certRegistry := registry.WithTags(
			map[string]string{"path": expiration.path},
		)
		certRegistry.Gauge("ExpireTs").Set(float64(expiration.after.Unix()))
		expiration.gauge = certRegistry.Gauge("certificateValidity")
	}
	reportCertificateValidity(expirations, time.Now())
	go monitorCertificateValidity(ctx, expirations)

	return &GRPCServerTLSProvider{certificates: certificates}, nil
}

func monitorCertificateValidity(
	ctx context.Context,
	expirations []certificateExpiration,
) {
	ticker := time.NewTicker(certificateValidationPeriod)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			reportCertificateValidity(expirations, now)
		case <-ctx.Done():
			return
		}
	}
}

func reportCertificateValidity(
	expirations []certificateExpiration,
	now time.Time,
) {
	for _, expiration := range expirations {
		validity := float64(1)
		if expiration.after.Sub(now) <= certificateValidationThreshold {
			validity = 0
		}
		expiration.gauge.Set(validity)
	}
}

func (p *GRPCServerTLSProvider) NewTransportCredentials() grpc_credentials.TransportCredentials {
	cfg := &tls.Config{
		Certificates: p.certificates,
		MinVersion:   tls.VersionTLS12,
	}
	// TODO: https://golang.org/doc/go1.14#crypto/tls
	// nolint:SA1019
	cfg.BuildNameToCertificate()
	return grpc_credentials.NewTLS(cfg)
}

func readServerCertificate(
	cert *server_config.Cert,
) (tls.Certificate, time.Time, error) {
	certificate, err := tls.LoadX509KeyPair(
		cert.GetCertFile(),
		cert.GetPrivateKeyFile(),
	)
	if err != nil {
		return tls.Certificate{}, time.Time{}, task_errors.NewNonRetriableErrorf(
			"failed to load cert file %v: %w",
			cert.GetCertFile(),
			err,
		)
	}
	if len(certificate.Certificate) == 0 {
		return tls.Certificate{}, time.Time{}, fmt.Errorf(
			"certificate chain is empty for cert file %v",
			cert.GetCertFile(),
		)
	}

	var expiration time.Time
	for i, certificateBytes := range certificate.Certificate {
		parsed, err := x509.ParseCertificate(certificateBytes)
		if err != nil {
			return tls.Certificate{}, time.Time{}, fmt.Errorf(
				"failed to parse certificate #%v from cert file %v: %w",
				i,
				cert.GetCertFile(),
				err,
			)
		}
		if expiration.IsZero() || parsed.NotAfter.Before(expiration) {
			expiration = parsed.NotAfter
		}
	}

	return certificate, expiration, nil
}
