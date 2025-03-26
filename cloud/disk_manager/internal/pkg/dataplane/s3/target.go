package s3

import (
	"context"
	"sync"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

const (
	ChunkSize = uint64(5 * 1024 * 1024) // 5 GiB
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

	var data []byte
	if chunk.Zero {
		data = make([]byte, ChunkSize)
	} else {
		data = chunk.Data
	}

	completedPart, err := t.s3.UploadPart(
		ctx,
		t.bucket,
		t.key,
		int64(chunk.Index)+1,
		t.uploadId,
		data,
	)
	if err != nil {
		return err
	}

	*t.completedParts = append(*t.completedParts, completedPart)
	return nil
}

func (t *s3Target) Close(ctx context.Context) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
}

////////////////////////////////////////////////////////////////////////////////

func NewS3Target(
	ctx context.Context,
	s3 *persistence.S3Client,
	bucket string,
	key string,
	uploadId string,
	completedParts *[]*aws_s3.CompletedPart,
) common.Target {

	return &s3Target{
		s3:             s3,
		bucket:         bucket,
		key:            key,
		uploadId:       uploadId,
		completedParts: completedParts,
	}
}
