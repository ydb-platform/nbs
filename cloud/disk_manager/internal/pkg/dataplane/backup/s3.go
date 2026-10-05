package backup

import (
	"context"
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

type S3 struct {
	s3        *persistence.S3Client
	bucket    string
	keyPrefix string
	kekID     string
	kek       cipher.AEAD
}

func NewS3(
	s3 *persistence.S3Client,
	bucket string,
	keyPrefix string,
	kekID string,
	kek []byte,
) (*S3, error) {

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

	return &S3{
		s3:        s3,
		bucket:    bucket,
		keyPrefix: keyPrefix,
		kekID:     kekID,
		kek:       aead,
	}, nil
}

func (s *S3) NewEncryptedDEK() ([]byte, error) {
	dek := make([]byte, keySize)
	_, err := rand.Read(dek)
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	return seal(s.kek, dek, nil)
}

func (s *S3) PutObject(
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

	// Enrich a copy with Key-Id and Encrypted-Dek. The incoming
	// map may be nil and must stay unchanged for the caller.
	metadata := make(map[string]*string)
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

func (s *S3) GetObject(
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

	// PutObject added these. The caller stored only its own fields.
	delete(object.Metadata, keyIDMetadataKey)
	delete(object.Metadata, encryptedDEKMetadataKey)
	return object, nil
}

func (s *S3) Key(key string) string {
	if len(s.keyPrefix) == 0 {
		return key
	}

	return fmt.Sprintf("%v/%v", s.keyPrefix, key)
}
