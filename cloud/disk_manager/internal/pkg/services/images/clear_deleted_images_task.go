package images

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type clearDeletedImagesTask struct {
	storage           resources.Storage
	expirationTimeout time.Duration
	limit             int
}

func (t *clearDeletedImagesTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *clearDeletedImagesTask) Load(_, _ []byte) error {
	return nil
}

func (t *clearDeletedImagesTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	deletedBefore := time.Now().Add(-t.expirationTimeout)
	return t.storage.ClearDeletedImages(ctx, deletedBefore, t.limit)
}

func (t *clearDeletedImagesTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *clearDeletedImagesTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *clearDeletedImagesTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
