package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type deleteBackupSnapshotDataTask struct {
	storage   storage.Storage
	backupS3  *backup.S3
	batchSize int
	request   *protos.DeleteBackupSnapshotDataRequest
	state     *protos.DeleteBackupSnapshotDataTaskState
}

func (t *deleteBackupSnapshotDataTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteBackupSnapshotDataTask) Load(request, state []byte) error {
	t.request = &protos.DeleteBackupSnapshotDataRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.DeleteBackupSnapshotDataTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteBackupSnapshotDataTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.SnapshotId

	meta, err := t.storage.GetSnapshotMeta(ctx, snapshotID)
	if err != nil {
		return err
	}

	if meta != nil {
		return errors.NewInterruptExecutionError()
	}

	chunkIDs, err := t.readChunkIDs(ctx, snapshotID)
	if err != nil {
		return err
	}

	err = t.deleteChunks(ctx, execCtx, chunkIDs)
	if err != nil {
		return err
	}

	return t.backupS3.DeleteChunkMap(ctx, snapshotID)
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

////////////////////////////////////////////////////////////////////////////////

func (t *deleteBackupSnapshotDataTask) readChunkIDs(
	ctx context.Context,
	snapshotID string,
) ([]string, error) {

	object, err := t.backupS3.GetObject(ctx, backup.ChunkMapKey(snapshotID))
	if errors.As(err, &persistence.ObjectNotFoundError{}) {
		// No chunk map: the snapshot was not backed up, or a previous
		// attempt already removed the map after the chunks.
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	chunkMap := &protos.BackupChunkMap{}
	err = proto.Unmarshal(object.Data, chunkMap)
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	return chunkMap.ChunkIds, nil
}

func (t *deleteBackupSnapshotDataTask) deleteChunks(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	chunkIDs []string,
) error {

	chunkCount := uint32(len(chunkIDs))
	for t.state.MilestoneChunkIndex < chunkCount {
		end := t.state.MilestoneChunkIndex + uint32(t.batchSize)
		if end > chunkCount {
			end = chunkCount
		}

		batch := chunkIDs[t.state.MilestoneChunkIndex:end]
		err := t.deleteChunkBatch(ctx, batch)
		if err != nil {
			return err
		}

		t.state.MilestoneChunkIndex = end
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *deleteBackupSnapshotDataTask) deleteChunkBatch(
	ctx context.Context,
	chunkIDs []string,
) error {

	var ids []string
	for _, chunkID := range chunkIDs {
		// ChunkIds is sized to ChunkCount. A slot without a chunk is empty.
		if len(chunkID) != 0 {
			ids = append(ids, chunkID)
		}
	}

	if len(ids) == 0 {
		return nil
	}

	existing, err := t.storage.FilterExistingChunkIDs(ctx, ids)
	if err != nil {
		return err
	}

	alive := tasks_common.NewStringSet(existing...)
	for _, chunkID := range ids {
		if alive.Has(chunkID) {
			continue
		}

		err = t.backupS3.DeleteChunk(ctx, chunkID)
		if err != nil {
			return err
		}
	}

	return nil
}
