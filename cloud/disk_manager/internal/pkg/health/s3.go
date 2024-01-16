package health

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type s3Check struct {
	s3     *persistence.S3Client
	bucket string
}

func (s s3Check) Check(ctx context.Context) bool {
	key := "healthCheckKey"

	expectedData := byte(0xff)
	err := s.s3.PutObject(ctx, s.bucket, key, persistence.S3Object{
		Data:     []byte{expectedData},
		Metadata: map[string]*string{},
	})
	if err != nil {
		logging.Warn(ctx, "S3 health check put failed: %v", err)
		return false
	}

	object, err := s.s3.GetObject(ctx, s.bucket, key)
	if err != nil {
		logging.Warn(ctx, "S3 health check get failed: %v", err)
		return false
	}

	if len(object.Data) != 1 {
		logging.Error(
			ctx,
			"S3 health check failed, got incorrect data size: %v",
			len(object.Data),
		)
	}

	if expectedData != object.Data[0] {
		logging.Error(
			ctx,
			"S3 health check failed, got unexpected data: '%v'",
			object.Data,
		)
	}

	return true
}

////////////////////////////////////////////////////////////////////////////////

func newS3Check(s3 *persistence.S3Client, bucket string) *s3Check {
	return &s3Check{
		s3:     s3,
		bucket: bucket,
	}
}
