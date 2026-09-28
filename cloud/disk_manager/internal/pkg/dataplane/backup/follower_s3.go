package backup

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

const (
	keySize = 32

	keyIDMetadataKey        = "Key-Id"
	encryptedDEKMetadataKey = "Encrypted-Dek"
)

////////////////////////////////////////////////////////////////////////////////

type FollowerS3 struct {
	s3        *persistence.S3Client
	bucket    string
	keyPrefix string
	kekID     string
	kek       cipher.AEAD
}

func NewFollowerS3(
	s3 *persistence.S3Client,
	bucket string,
	keyPrefix string,
	kekID string,
	kek []byte,
) (*FollowerS3, error) {

	if len(kek) != keySize {
		return nil, errors.NewNonRetriableErrorf(
			"kek %v has size %v, expected %v",
			kekID,
			len(kek),
			keySize,
		)
	}

	aead, err := newAEAD(kek)
	if err != nil {
		return nil, err
	}

	return &FollowerS3{
		s3:        s3,
		bucket:    bucket,
		keyPrefix: keyPrefix,
		kekID:     kekID,
		kek:       aead,
	}, nil
}

func (s *FollowerS3) NewEncryptedDEK() ([]byte, error) {
	dek := make([]byte, keySize)
	_, err := rand.Read(dek)
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	return seal(s.kek, dek, nil)
}

func (s *FollowerS3) PutObject(
	ctx context.Context,
	key string,
	encryptedDEK []byte,
	object persistence.S3Object,
) error {

	dek, err := s.openDEK(encryptedDEK)
	if err != nil {
		return err
	}

	data, err := seal(dek, object.Data, []byte(key))
	if err != nil {
		return err
	}

	metadata := make(map[string]*string, len(object.Metadata)+2)
	for name, value := range object.Metadata {
		metadata[name] = value
	}

	kekID := s.kekID
	metadata[keyIDMetadataKey] = &kekID
	encodedDEK := base64.StdEncoding.EncodeToString(encryptedDEK)
	metadata[encryptedDEKMetadataKey] = &encodedDEK

	return s.s3.PutObject(ctx, s.bucket, s.Key(key), persistence.S3Object{
		Data:         data,
		Metadata:     metadata,
		StorageClass: object.StorageClass,
	})
}

func (s *FollowerS3) GetObject(
	ctx context.Context,
	key string,
) (persistence.S3Object, error) {

	object, err := s.s3.GetObject(ctx, s.bucket, s.Key(key))
	if err != nil {
		return persistence.S3Object{}, err
	}

	kekID := object.Metadata[keyIDMetadataKey]
	if kekID == nil || *kekID != s.kekID {
		return persistence.S3Object{}, errors.NewNonRetriableErrorf(
			"object %v is not encrypted by kek %v",
			key,
			s.kekID,
		)
	}

	encodedDEK := object.Metadata[encryptedDEKMetadataKey]
	if encodedDEK == nil {
		return persistence.S3Object{}, errors.NewNonRetriableErrorf(
			"object %v has no encrypted dek",
			key,
		)
	}

	encryptedDEK, err := base64.StdEncoding.DecodeString(*encodedDEK)
	if err != nil {
		return persistence.S3Object{}, errors.NewNonRetriableError(err)
	}

	dek, err := s.openDEK(encryptedDEK)
	if err != nil {
		return persistence.S3Object{}, err
	}

	object.Data, err = open(dek, object.Data, []byte(key))
	if err != nil {
		return persistence.S3Object{}, err
	}

	delete(object.Metadata, keyIDMetadataKey)
	delete(object.Metadata, encryptedDEKMetadataKey)
	return object, nil
}

func (s *FollowerS3) Key(key string) string {
	if len(s.keyPrefix) == 0 {
		return key
	}

	return fmt.Sprintf("%v/%v", s.keyPrefix, key)
}

////////////////////////////////////////////////////////////////////////////////

func (s *FollowerS3) openDEK(encryptedDEK []byte) (cipher.AEAD, error) {
	dek, err := open(s.kek, encryptedDEK, nil)
	if err != nil {
		return nil, err
	}

	return newAEAD(dek)
}

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

func seal(
	aead cipher.AEAD,
	plaintext []byte,
	additionalData []byte,
) ([]byte, error) {

	iv := make([]byte, aead.NonceSize())
	_, err := rand.Read(iv)
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	return aead.Seal(iv, iv, plaintext, additionalData), nil
}

func open(
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
	plaintext, err := aead.Open(
		nil,
		iv,
		ciphertext[aead.NonceSize():],
		additionalData,
	)
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	return plaintext, nil
}
