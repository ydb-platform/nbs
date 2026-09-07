package common

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
)

////////////////////////////////////////////////////////////////////////////////

var pemBeginMarker = []byte("-----BEGIN")

// Parses all certificates from PEM encoded data. Blocks of other types are
// skipped. Unlike x509.CertPool.AppendCertsFromPEM and tls.X509KeyPair, which
// silently stop at the first malformed block, this rejects data that contains
// a malformed or truncated block, e.g. a file that is being rewritten
// non-atomically.
func parsePEMCertificates(data []byte) ([]*x509.Certificate, error) {
	var certificates []*x509.Certificate

	for {
		block, rest := pem.Decode(data)
		if block == nil {
			break
		}

		// pem.Decode skips malformed blocks that are followed by a valid one,
		// so the consumed data must contain exactly one begin marker.
		consumed := data[:len(data)-len(rest)]
		if bytes.Count(consumed, pemBeginMarker) != 1 {
			return nil, errors.New("malformed PEM block")
		}

		data = rest

		if block.Type != "CERTIFICATE" {
			continue
		}

		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to parse certificate #%v: %w",
				len(certificates),
				err,
			)
		}

		certificates = append(certificates, certificate)
	}

	// A trailing malformed block is not decoded at all.
	if bytes.Contains(data, pemBeginMarker) {
		return nil, errors.New("malformed trailing PEM block")
	}

	if len(certificates) == 0 {
		return nil, errors.New("no certificates found in PEM data")
	}

	return certificates, nil
}
