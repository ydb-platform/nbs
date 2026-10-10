package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

// Deletes the chunk map of a deleted snapshot from the follower. The chunks
// are not touched here: each one is queued for deletion when it is deleted
// from chunk_blobs, see dataplane.DeleteBackupChunks.
type deleteBackupSnapshotDataTask struct {
	storage  storage.Storage
	backupS3 *backup.S3
	request  *protos.DeleteBackupSnapshotDataRequest
}

func (t *deleteBackupSnapshotDataTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *deleteBackupSnapshotDataTask) Load(request, _ []byte) error {
	t.request = &protos.DeleteBackupSnapshotDataRequest{}
	return proto.Unmarshal(request, t.request)
}

func (t *deleteBackupSnapshotDataTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// Deletion of the snapshot waits for its backup copy, so once the
	// snapshot is gone nothing writes its chunk map or meta.json anymore.
	meta, err := t.storage.GetSnapshotMeta(ctx, t.request.SnapshotId)
	if err != nil {
		return err
	}

	if meta != nil {
		return errors.NewInterruptExecutionError()
	}

	return t.backupS3.DeleteChunkMap(ctx, t.request.SnapshotId)
}

func (t *deleteBackupSnapshotDataTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *deleteBackupSnapshotDataTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *deleteBackupSnapshotDataTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
