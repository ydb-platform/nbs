package backup

import (
	"bytes"
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

	kekIDMetadataKey        = "Key-Id"
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

	if len(kekID) == 0 && len(kek) == 0 {
		return &S3{
			s3:        s3,
			bucket:    bucket,
			keyPrefix: keyPrefix,
		}, nil
	}

	if len(kekID) == 0 {
		return nil, errors.NewNonRetriableErrorf("kek id is empty")
	}

	// A key file written by an editor is 32 bytes plus a trailing
	// newline. A file that is already 32 bytes is kept as-is: its
	// first or last byte may itself be whitespace.
	if len(kek) != keySize {
		kek = bytes.TrimSpace(kek)
	}

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
	if s.kek == nil {
		return nil, errors.NewNonRetriableErrorf(
			"kek is not configured",
		)
	}

	dek := make([]byte, keySize)
	_, err := rand.Read(dek)
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	return encrypt(s.kek, dek, []byte(s.kekID))
}

func (s *S3) EnsureEncryptedDEK(encryptedDEK []byte) ([]byte, error) {
	if s.kek == nil {
		if len(encryptedDEK) != 0 {
			return nil, errors.NewNonRetriableErrorf(
				"dek is set but kek is not configured",
			)
		}

		return nil, nil
	}

	if len(encryptedDEK) != 0 {
		return encryptedDEK, nil
	}

	return s.NewEncryptedDEK()
}

func (s *S3) PutObject(
	ctx context.Context,
	key string,
	encryptedDEK []byte,
	object persistence.S3Object,
) error {

	if s.kek == nil {
		if len(encryptedDEK) != 0 {
			return errors.NewNonRetriableErrorf(
				"dek is set but kek is not configured",
			)
		}

		return s.s3.PutObject(ctx, s.bucket, s.Key(key), object)
	}

	dek, err := s.decryptDEK(encryptedDEK)
	if err != nil {
		return err
	}

	data, err := encrypt(dek, object.Data, []byte(key))
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
	metadata[kekIDMetadataKey] = &kekID
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

	if s.kek == nil {
		kekID := object.Metadata[kekIDMetadataKey]
		if kekID != nil {
			err = errors.NewNonRetriableErrorf(
				"object %v is encrypted",
				key,
			)
			return persistence.S3Object{}, err
		}

		return object, nil
	}

	kekID := object.Metadata[kekIDMetadataKey]
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

	dek, err := s.decryptDEK(encryptedDEK)
	if err != nil {
		return persistence.S3Object{}, err
	}

	object.Data, err = decrypt(dek, object.Data, []byte(key))
	if err != nil {
		return persistence.S3Object{}, err
	}

	// PutObject added these. The caller stored only its own fields.
	delete(object.Metadata, kekIDMetadataKey)
	delete(object.Metadata, encryptedDEKMetadataKey)
	return object, nil
}

func (s *S3) DeleteObject(ctx context.Context, key string) error {
	return s.s3.DeleteObject(ctx, s.bucket, s.Key(key))
}

func (s *S3) DeleteSnapshotMeta(
	ctx context.Context,
	diskID string,
	snapshotID string,
) error {

	return s.DeleteObject(ctx, SnapshotMetaKey(diskID, snapshotID))
}

func (s *S3) DeleteImageMeta(ctx context.Context, imageID string) error {
	return s.DeleteObject(ctx, ImageMetaKey(imageID))
}

func (s *S3) DeleteChunk(ctx context.Context, chunkID string) error {
	return s.DeleteObject(ctx, ChunkKey(chunkID))
}

func (s *S3) DeleteChunkMap(ctx context.Context, snapshotID string) error {
	return s.DeleteObject(ctx, ChunkMapKey(snapshotID))
}

func (s *S3) Key(key string) string {
	if len(s.keyPrefix) == 0 {
		return key
	}

	return fmt.Sprintf("%v/%v", s.keyPrefix, key)
}
