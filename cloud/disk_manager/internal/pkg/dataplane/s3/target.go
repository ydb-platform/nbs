package s3

import (
	"context"
	"sync"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type s3Target struct {
	s3       *persistence.S3Client
	bucket   string
	key      string
	uploadId string

	mutex          sync.RWMutex
	completedParts *[]*aws_s3.CompletedPart
}

func (t *s3Target) Write(
	ctx context.Context,
	chunk common.Chunk,
) error {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	list, err := t.s3.List(ctx, t.bucket, t.key)
	if err != nil {
		return err
	}
	for _, upload := range list {
		logging.Info(ctx, "KEK Upload ID: %s, Key: %s\n", *upload.UploadId, *upload.Key)
	}

	var completedPart *aws_s3.CompletedPart
	// if chunk.Zero {
	// data := make([]byte, 4194304)
	// 	completedPart, err = t.s3.UploadPart(ctx, t.bucket, t.key, 1, t.uploadId, data)
	// 	if err != nil {
	// 		return err
	// 	}
	// } else {
	completedPart, err = t.s3.UploadPart(ctx, t.bucket, t.key, 1, t.uploadId, chunk.Data)
	if err != nil {
		return err
	}

	logging.Info(ctx, "writed chunk %+v", chunk)
	// }

	*t.completedParts = append(*t.completedParts, completedPart)
	return nil
}

func (t *s3Target) Close(ctx context.Context) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// close(t.completedParts)
}

////////////////////////////////////////////////////////////////////////////////

func NewS3Target(
	ctx context.Context,
	s3 *persistence.S3Client,
	bucket string,
	key string,
	uploadId string,
	completedParts *[]*aws_s3.CompletedPart,
) (common.Target, error) {

	return &s3Target{
		s3:             s3,
		bucket:         bucket,
		key:            key,
		uploadId:       uploadId,
		completedParts: completedParts,
	}, nil
}
