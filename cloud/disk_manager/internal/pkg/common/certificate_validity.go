package common

import (
	"crypto/x509"
	"fmt"
	"time"
)

////////////////////////////////////////////////////////////////////////////////

func validateCertificateCurrentlyValid(
	certificate *x509.Certificate,
	now time.Time,
) error {
	if now.Before(certificate.NotBefore) {
		return fmt.Errorf(
			"certificate is not valid before %v",
			certificate.NotBefore,
		)
	}
	if !now.Before(certificate.NotAfter) {
		return fmt.Errorf("certificate expired at %v", certificate.NotAfter)
	}

	return nil
}
