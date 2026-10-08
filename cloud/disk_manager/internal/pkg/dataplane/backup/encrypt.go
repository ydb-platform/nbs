package backup

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	return aead, nil
}

func encrypt(
	aead cipher.AEAD,
	plaintext []byte,
	additionalData []byte,
) ([]byte, error) {

	iv := make([]byte, aead.NonceSize())
	_, err := rand.Read(iv)
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	sealed := aead.Seal(nil, iv, plaintext, additionalData)
	return append(iv, sealed...), nil
}

func decrypt(
	aead cipher.AEAD,
	ciphertext []byte,
	additionalData []byte,
) ([]byte, error) {

	if len(ciphertext) < aead.NonceSize()+aead.Overhead() {
		return nil, errors.NewNonRetriableErrorf(
			"ciphertext of size %v is too short",
			len(ciphertext),
		)
	}

	iv := ciphertext[:aead.NonceSize()]
	sealed := ciphertext[aead.NonceSize():]
	plaintext, err := aead.Open(nil, iv, sealed, additionalData)
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	return plaintext, nil
}

func (s *S3) decryptDEK(encryptedDEK []byte) (cipher.AEAD, error) {
	dek, err := decrypt(s.kek, encryptedDEK, []byte(s.kekID))
	if err != nil {
		return nil, err
	}

	return newAEAD(dek)
}

func (s *S3) CheckEncryptedDEK(encryptedDEK []byte) error {
	if s.kek == nil {
		if len(encryptedDEK) != 0 {
			return errors.NewNonRetriableErrorf(
				"dek is set but kek is not configured",
			)
		}

		return nil
	}

	_, err := s.decryptDEK(encryptedDEK)
	return err
}
