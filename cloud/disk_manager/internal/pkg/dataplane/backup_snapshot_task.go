package dataplane

import (
	"context"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type backupSnapshotTask struct {
	storage          storage.Storage
	s3               *persistence.S3Client
	bucket           string
	keyPrefix        string
	enqueueBatchSize int
	request          *protos.BackupSnapshotRequest
	state            *protos.BackupSnapshotTaskState
}

func (t *backupSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *backupSnapshotTask) Load(request, state []byte) error {
	t.request = &protos.BackupSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.BackupSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.SnapshotId

	// Snapshot is locked until its chunks are copied, otherwise
	// dataplane.DeleteSnapshot removes the chunks from under the backup.
	locked, err := t.storage.LockSnapshot(
		ctx,
		snapshotID,
		execCtx.GetTaskID(),
	)
	if err != nil {
		return err
	}

	if !locked {
		logging.Info(
			ctx,
			"Snapshot with id %v is deleted, nothing to back up",
			snapshotID,
		)
		return nil
	}

	meta, err := t.storage.CheckSnapshotReady(ctx, snapshotID)
	if err != nil {
		return err
	}

	t.state.ChunkCount = meta.ChunkCount

	err = t.enqueueChunks(ctx, execCtx)
	if err != nil {
		return err
	}

	hasEntries, err := t.storage.HasBackupQueueEntries(ctx, snapshotID)
	if err != nil {
		return err
	}

	if hasEntries {
		logging.Debug(
			ctx,
			"Backup of snapshot with id %v is waiting for its chunks",
			snapshotID,
		)
		return errors.NewInterruptExecutionError()
	}

	err = t.writeChunkMap(ctx, meta)
	if err != nil {
		return err
	}

	err = t.storage.UnlockSnapshot(ctx, snapshotID, execCtx.GetTaskID())
	if err != nil {
		return err
	}

	t.state.Progress = 1
	return nil
}

func (t *backupSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.storage.UnlockSnapshot(
		ctx,
		t.request.SnapshotId,
		execCtx.GetTaskID(),
	)
}

func (t *backupSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.BackupSnapshotMetadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *backupSnapshotTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

// Puts the own chunks of the snapshot into backup_queue. Chunks inherited from
// the base snapshot are copied by the backup of that snapshot.
func (t *backupSnapshotTask) enqueueChunks(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.SnapshotId

	if t.state.MilestoneChunkIndex >= t.state.ChunkCount {
		return nil
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, errs := t.storage.ReadChunkMap(
		readCtx,
		snapshotID,
		t.state.MilestoneChunkIndex,
	)

	var batch []storage.BackupQueueEntry
	var lastChunkIndex uint32

	flush := func() error {
		err := t.storage.EnqueueBackupChunks(ctx, batch)
		if err != nil {
			return err
		}

		t.state.EnqueuedChunkCount += uint32(len(batch))
		t.state.MilestoneChunkIndex = lastChunkIndex + 1
		batch = nil
		return t.saveProgress(ctx, execCtx)
	}

	for entry := range entries {
		lastChunkIndex = entry.ChunkIndex

		if len(entry.ChunkID) == 0 || !isOwnChunk(entry.ChunkID, snapshotID) {
			continue
		}

		if !entry.StoredInS3 {
			return errors.NewNonRetriableErrorf(
				"chunk %v of snapshot %v is not stored in s3",
				entry.ChunkID,
				snapshotID,
			)
		}

		batch = append(batch, storage.BackupQueueEntry{
			SnapshotID: snapshotID,
			ChunkID:    entry.ChunkID,
		})

		if len(batch) >= t.enqueueBatchSize {
			err := flush()
			if err != nil {
				return err
			}
		}
	}

	err := <-errs
	if err != nil {
		return err
	}

	err = flush()
	if err != nil {
		return err
	}

	t.state.MilestoneChunkIndex = t.state.ChunkCount
	return t.saveProgress(ctx, execCtx)
}

// Chunk map is the marker of a complete backup, so it is written after all the
// chunks of the snapshot are copied.
func (t *backupSnapshotTask) writeChunkMap(
	ctx context.Context,
	meta storage.SnapshotMeta,
) error {

	chunkMap := &protos.BackupChunkMap{
		ChunkIds: make([]string, meta.ChunkCount),
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, errs := t.storage.ReadChunkMap(readCtx, meta.ID, 0)
	for entry := range entries {
		if entry.ChunkIndex >= meta.ChunkCount {
			return errors.NewNonRetriableErrorf(
				"chunk index %v of snapshot %v is out of range %v",
				entry.ChunkIndex,
				meta.ID,
				meta.ChunkCount,
			)
		}

		chunkMap.ChunkIds[entry.ChunkIndex] = entry.ChunkID
	}

	err := <-errs
	if err != nil {
		return err
	}

	data, err := proto.Marshal(chunkMap)
	if err != nil {
		return errors.NewNonRetriableError(err)
	}

	return t.s3.PutObject(
		ctx,
		t.bucket,
		backup.ChunkMapKey(t.keyPrefix, meta.ID),
		persistence.S3Object{Data: data},
	)
}

func (t *backupSnapshotTask) saveProgress(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.state.ChunkCount != 0 {
		t.state.Progress =
			float64(t.state.MilestoneChunkIndex) / float64(t.state.ChunkCount)
	}

	logging.Debug(ctx, "saving state %+v", t.state)
	return execCtx.SaveState(ctx)
}

////////////////////////////////////////////////////////////////////////////////

// Chunk id is <create task id>.<snapshot id>.<chunk index>.
func isOwnChunk(chunkID string, snapshotID string) bool {
	parts := strings.Split(chunkID, ".")
	return len(parts) == 3 && parts[1] == snapshotID
}
