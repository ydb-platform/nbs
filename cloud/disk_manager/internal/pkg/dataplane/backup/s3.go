package backup

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/bucket"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
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

// The follower bucket: the objects of a snapshot bucket, encrypted with a
// DEK per snapshot. One instance per process: every write goes through it
// and waits in its limiter.
type S3 struct {
	*bucket.S3
	kekID    string
	kek      cipher.AEAD
	limiter  *BandwidthLimiter
	registry metrics.Registry
}

// uploadBytesPerSecond caps what this host writes to the bucket; 0 disables
// the cap.
func NewS3(
	s3 *persistence.S3Client,
	bucketName string,
	keyPrefix string,
	kekID string,
	kek []byte,
	uploadBytesPerSecond uint64,
	registry metrics.Registry,
) (*S3, error) {

	backupS3 := &S3{
		S3:       bucket.NewS3(s3, bucketName, keyPrefix),
		limiter:  NewBandwidthLimiter(uploadBytesPerSecond),
		registry: registry,
	}

	if len(kekID) == 0 && len(kek) == 0 {
		return backupS3, nil
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

	backupS3.kekID = kekID
	backupS3.kek = aead
	return backupS3, nil
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

		return s.put(ctx, key, object)
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

	return s.put(ctx, key, persistence.S3Object{
		Data:         data,
		Metadata:     metadata,
		StorageClass: object.StorageClass,
	})
}

func (s *S3) put(
	ctx context.Context,
	key string,
	object persistence.S3Object,
) error {

	waitStart := time.Now()
	err := s.limiter.Wait(ctx, len(object.Data))
	if err != nil {
		return err
	}
	s.registry.Counter("backup/bandwidthWaitMs").Add(
		time.Since(waitStart).Milliseconds(),
	)

	inflight := s.registry.Gauge("backup/putInflight")
	inflight.Add(1)
	defer inflight.Add(-1)

	putStart := time.Now()
	err = s.S3.PutObject(ctx, key, object)
	s.registry.Timer("backup/putTime").RecordDuration(time.Since(putStart))
	return err
}

func (s *S3) GetObject(
	ctx context.Context,
	key string,
) (persistence.S3Object, error) {

	object, err := s.S3.GetObject(ctx, key)
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
