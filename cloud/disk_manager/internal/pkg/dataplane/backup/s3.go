package backup

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type S3 struct {
	s3        *persistence.S3Client
	bucket    string
	keyPrefix string
}

func NewS3(
	s3 *persistence.S3Client,
	bucket string,
	keyPrefix string,
) *S3 {

	return &S3{
		s3:        s3,
		bucket:    bucket,
		keyPrefix: keyPrefix,
	}
}

func (s *S3) PutObject(
	ctx context.Context,
	key string,
	object persistence.S3Object,
) error {

	return s.s3.PutObject(ctx, s.bucket, s.Key(key), object)
}

func (s *S3) GetObject(
	ctx context.Context,
	key string,
) (persistence.S3Object, error) {

	return s.s3.GetObject(ctx, s.bucket, s.Key(key))
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
