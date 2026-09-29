package backup

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type FollowerS3 struct {
	s3        *persistence.S3Client
	bucket    string
	keyPrefix string
}

func NewFollowerS3(
	s3 *persistence.S3Client,
	bucket string,
	keyPrefix string,
) *FollowerS3 {

	return &FollowerS3{
		s3:        s3,
		bucket:    bucket,
		keyPrefix: keyPrefix,
	}
}

func (s *FollowerS3) PutObject(
	ctx context.Context,
	key string,
	data []byte,
) error {

	return s.s3.PutObject(
		ctx,
		s.bucket,
		s.Key(key),
		persistence.S3Object{Data: data},
	)
}

func (s *FollowerS3) Key(key string) string {
	if len(s.keyPrefix) == 0 {
		return key
	}

	return fmt.Sprintf("%v/%v", s.keyPrefix, key)
}
