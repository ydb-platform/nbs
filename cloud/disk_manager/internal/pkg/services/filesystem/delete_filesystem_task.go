package filesystem

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api/yandex/cloud/priv/disk_manager/v1"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type deleteFilesystemTask struct {
	storage resources.Storage
	factory nfs.Factory
	request *protos.DeleteFilesystemRequest
	state   *protos.DeleteFilesystemTaskState
}

func (t *deleteFilesystemTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteFilesystemTask) Load(request, state []byte) error {
	t.request = &protos.DeleteFilesystemRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.DeleteFilesystemTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteFilesystemTask) deleteFilesystem(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	client, err := t.factory.NewClient(ctx, t.request.Filesystem.ZoneId)
	if err != nil {
		return err
	}
	defer client.Close()

	selfTaskID := execCtx.GetTaskID()

	filesystemMeta, err := t.storage.DeleteFilesystem(
		ctx,
		t.request.Filesystem.FilesystemId,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if filesystemMeta == nil {
		return errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			t.request.Filesystem.FilesystemId,
		)
	}

	err = client.Delete(ctx, t.request.Filesystem.FilesystemId)
	if err != nil {
		return err
	}

	return t.storage.FilesystemDeleted(
		ctx,
		t.request.Filesystem.FilesystemId,
		time.Now(),
	)
}

func (t *deleteFilesystemTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.deleteFilesystem(ctx, execCtx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *deleteFilesystemTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.deleteFilesystem(ctx, execCtx)
}

func (t *deleteFilesystemTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &disk_manager.DeleteFilesystemMetadata{
		FilesystemId: &disk_manager.FilesystemId{
			ZoneId:       t.request.Filesystem.ZoneId,
			FilesystemId: t.request.Filesystem.FilesystemId,
		},
	}, nil
}

func (t *deleteFilesystemTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
