package driver

import (
	"crypto/sha256"
	"fmt"
)

func generateDiskID(name string) (string, error) {
	h := sha256.New()
	if _, err := h.Write([]byte(name)); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
