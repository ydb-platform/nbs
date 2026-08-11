package common

import (
	"crypto/x509"
	"fmt"
	"time"
)

////////////////////////////////////////////////////////////////////////////////

func parseCertificateChain(
	certificateChain [][]byte,
) ([]*x509.Certificate, error) {
	if len(certificateChain) == 0 {
		return nil, fmt.Errorf("certificate chain is empty")
	}

	certificates := make([]*x509.Certificate, 0, len(certificateChain))
	for i, certificateBytes := range certificateChain {
		certificate, err := x509.ParseCertificate(certificateBytes)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to parse certificate #%v: %w",
				i,
				err,
			)
		}
		certificates = append(certificates, certificate)
	}

	return certificates, nil
}

func validateCertificateChainCurrentlyValid(
	certificates []*x509.Certificate,
	now time.Time,
) error {
	for i, certificate := range certificates {
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

func getCertificateChainExpiration(
	certificates []*x509.Certificate,
) time.Time {
	expiration := certificates[0].NotAfter
	for _, certificate := range certificates[1:] {
		if certificate.NotAfter.Before(expiration) {
			expiration = certificate.NotAfter
		}
	}

	return expiration
}
