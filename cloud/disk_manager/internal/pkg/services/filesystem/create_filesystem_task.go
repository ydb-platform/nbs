package filesystem

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type createFilesystemTask struct {
	storage resources.Storage
	factory nfs.Factory
	request *protos.CreateFilesystemRequest
	state   *protos.CreateFilesystemTaskState
}

func (t *createFilesystemTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createFilesystemTask) Load(request, state []byte) error {
	t.request = &protos.CreateFilesystemRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateFilesystemTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createFilesystemTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	client, err := t.factory.NewClient(ctx, t.request.Filesystem.ZoneId)
	if err != nil {
		return err
	}
	defer client.Close()

	selfTaskID := execCtx.GetTaskID()

	filesystemMeta, err := t.storage.CreateFilesystem(ctx, resources.FilesystemMeta{
		ID:          t.request.Filesystem.FilesystemId,
		ZoneID:      t.request.Filesystem.ZoneId,
		BlocksCount: t.request.BlocksCount,
		BlockSize:   t.request.BlockSize,
		Kind:        fsKindToString(t.request.Kind),
		CloudID:     t.request.CloudId,
		FolderID:    t.request.FolderId,

		CreateRequest: t.request,
		CreateTaskID:  selfTaskID,
		CreatingAt:    time.Now(),
		CreatedBy:     "", // TODO: Extract CreatedBy from execCtx
	})
	if err != nil {
		return err
	}

	if filesystemMeta == nil {
		return errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			t.request.Filesystem.FilesystemId,
		)
	}

	err = client.Create(ctx, t.request.Filesystem.FilesystemId, nfs.CreateFilesystemParams{
		CloudID:     t.request.CloudId,
		FolderID:    t.request.FolderId,
		BlocksCount: t.request.BlocksCount,
		BlockSize:   t.request.BlockSize,
		Kind:        t.request.Kind,
	})
	if err != nil {
		return err
	}

	filesystemMeta.CreatedAt = time.Now()
	return t.storage.FilesystemCreated(ctx, *filesystemMeta)
}

func (t *createFilesystemTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	client, err := t.factory.NewClient(ctx, t.request.Filesystem.ZoneId)
	if err != nil {
		return err
	}
	defer client.Close()

	selfTaskID := execCtx.GetTaskID()

	fs, err := t.storage.DeleteFilesystem(
		ctx,
		t.request.Filesystem.FilesystemId,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if fs == nil {
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

func (t *createFilesystemTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *createFilesystemTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
