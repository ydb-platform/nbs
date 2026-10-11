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

////////////////////////////////////////////////////////////////////////////////

// Kind bound to the sealed keys of an export destination.
const ExportCredentialsSecretKind = "export-s3-credentials"

// Secrets that travel through the task storage, such as keys of a destination
// bucket, use the same envelope as follower objects: a fresh DEK under the
// kek, the secret under the DEK. The kek id binds the DEK, the secret kind
// binds the ciphertext, so a sealed value cannot be replayed as another kind.

func (s *S3) EncryptSecret(
	kind string,
	plaintext []byte,
) (encryptedDEK []byte, ciphertext []byte, err error) {

	encryptedDEK, err = s.NewEncryptedDEK()
	if err != nil {
		return nil, nil, err
	}

	aead, err := s.decryptDEK(encryptedDEK)
	if err != nil {
		return nil, nil, err
	}

	ciphertext, err = encrypt(aead, plaintext, []byte(kind))
	if err != nil {
		return nil, nil, err
	}

	return encryptedDEK, ciphertext, nil
}

func (s *S3) DecryptSecret(
	kind string,
	encryptedDEK []byte,
	ciphertext []byte,
) ([]byte, error) {

	if s.kek == nil {
		return nil, errors.NewNonRetriableErrorf("kek is not configured")
	}

	aead, err := s.decryptDEK(encryptedDEK)
	if err != nil {
		return nil, err
	}

	return decrypt(aead, ciphertext, []byte(kind))
}
