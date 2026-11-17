package storage

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type storageYDB struct {
	db                       *persistence.YDBClient
	tablesPath               string
	metrics                  metrics.Metrics
	deleteWorkerCount        int
	shallowCopyWorkerCount   int
	shallowCopyInflightLimit int
	chunkCompression         string
	// Chunks copied to the follower bucket or queued for it.
	followerChunksTableName string
	chunkStorageS3          *chunks.StorageS3
	chunkStorageYDB         *chunks.StorageYDB
}

var _ ChunkMapRangeReader = (*storageYDB)(nil)

func (s *storageYDB) CreateSnapshot(
	ctx context.Context,
	snapshotMeta SnapshotMeta,
) (*SnapshotMeta, error) {

	var created *SnapshotMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			created, err = s.createSnapshot(
				ctx,
				session,
				snapshotMeta,
			)
			return err
		},
	)
	return created, err
}

func (s *storageYDB) SnapshotCreated(
	ctx context.Context,
	snapshotID string,
	size uint64,
	storageSize uint64,
	chunkCount uint32,
	encryption *types.EncryptionDesc,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.snapshotCreated(
				ctx,
				session,
				snapshotID,
				size,
				storageSize,
				chunkCount,
				encryption,
			)
		},
	)
}

func (s *storageYDB) DeletingSnapshot(
	ctx context.Context,
	snapshotID string,
	taskID string,
) (*SnapshotMeta, error) {

	var snapshotMeta *SnapshotMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			snapshotMeta, err = s.deletingSnapshot(
				ctx,
				session,
				snapshotID,
				taskID,
			)
			return err
		},
	)
	return snapshotMeta, err
}

func (s *storageYDB) DeleteSnapshotData(
	ctx context.Context,
	snapshotID string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.deleteSnapshotData(
				ctx,
				session,
				snapshotID,
			)
		},
	)
}

func (s *storageYDB) ShallowCopySnapshot(
	ctx context.Context,
	srcSnapshotID string,
	dstSnapshotID string,
	milestoneChunkIndex uint32,
	saveProgress func(context.Context, uint32) error,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.shallowCopySnapshot(
				ctx,
				session,
				srcSnapshotID,
				dstSnapshotID,
				milestoneChunkIndex,
				saveProgress,
			)
		},
	)
}

func (s *storageYDB) ReadChunkMap(
	ctx context.Context,
	snapshotID string,
	milestoneChunkIndex uint32,
) (<-chan ChunkMapEntry, <-chan error) {

	return s.readChunkMapWithEnd(ctx, snapshotID, milestoneChunkIndex, ^uint32(0))
}

// ReadChunkMapRange applies both boundaries to the YDB primary-key range.
// The end boundary is exclusive; ReadChunkMap keeps its existing semantics.
func (s *storageYDB) ReadChunkMapRange(
	ctx context.Context,
	snapshotID string,
	start uint32,
	end uint32,
) (<-chan ChunkMapEntry, <-chan error) {

	if err := ctx.Err(); err != nil {
		return closedChunkMap(err)
	}

	if start > end {
		return closedChunkMap(
			task_errors.NewNonRetriableErrorf(
				"invalid chunk map range [%v, %v)", start, end,
			),
		)
	}

	if start == end {
		return closedChunkMap(nil)
	}

	return s.readChunkMapWithEnd(ctx, snapshotID, start, end-1)
}

func closedChunkMap(err error) (<-chan ChunkMapEntry, <-chan error) {
	entries := make(chan ChunkMapEntry)
	errors := make(chan error, 1)
	errors <- err
	close(entries)
	close(errors)
	return entries, errors
}

func (s *storageYDB) readChunkMapWithEnd(
	ctx context.Context,
	snapshotID string,
	milestoneChunkIndex uint32,
	lastChunkIndex uint32,
) (<-chan ChunkMapEntry, <-chan error) {

	var entries <-chan ChunkMapEntry
	var errors <-chan error

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			entries, errors = s.readChunkMap(
				ctx,
				session,
				snapshotID,
				milestoneChunkIndex,
				lastChunkIndex,
				nil,
			)
			return nil
		},
	)
	if err != nil {
		return closedChunkMap(err)
	}

	return entries, errors
}

func (s *storageYDB) DeleteDiskFromIncremental(
	ctx context.Context,
	zoneID string,
	diskID string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.deleteDiskFromIncremental(
				ctx,
				session,
				zoneID,
				diskID,
			)
		},
	)
}

func (s *storageYDB) LockSnapshot(
	ctx context.Context,
	snapshotID string,
	lockTaskID string,
) (locked bool, err error) {

	err = s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			locked, err = s.lockSnapshot(ctx, session, snapshotID, lockTaskID)
			return err
		},
	)
	return locked, err
}

func (s *storageYDB) UnlockSnapshot(
	ctx context.Context,
	snapshotID string,
	lockTaskID string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.unlockSnapshot(ctx, session, snapshotID, lockTaskID)
		},
	)
}

func (s *storageYDB) GetSnapshotMeta(
	ctx context.Context,
	snapshotID string,
) (*SnapshotMeta, error) {

	var snapshotMeta *SnapshotMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			snapshotMeta, err = s.getSnapshotMeta(
				ctx,
				session,
				snapshotID,
			)
			return err
		},
	)
	return snapshotMeta, err
}

func (s *storageYDB) GetIncremental(
	ctx context.Context,
	disk *types.Disk,
) (snapshotID string, checkpointID string, err error) {

	err = s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) (err error) {
			snapshotID, checkpointID, err = s.getIncremental(
				ctx,
				session,
				disk,
			)
			return err
		},
	)
	return snapshotID, checkpointID, err
}

func (s *storageYDB) ListSnapshots(
	ctx context.Context,
) (ids tasks_common.StringSet, err error) {

	err = s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			ids, err = s.listSnapshots(ctx, session)
			return err
		},
	)
	return ids, err
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) HoldSnapshotForBackup(
	ctx context.Context,
	snapshotID string,
	taskID string,
) (held bool, err error) {

	defer s.metrics.StatOperation("HoldSnapshotForBackup")(&err)

	if len(taskID) == 0 {
		return false, task_errors.NewNonRetriableErrorf("empty backup task ID")
	}

	err = s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			held, err = s.holdSnapshotForBackup(ctx, session, snapshotID, taskID)
			return err
		},
	)
	return held, err
}

func (s *storageYDB) EnqueueBackupChunks(
	ctx context.Context,
	snapshotID string,
	entries []BackupChunkQueueEntry,
) (err error) {

	defer s.metrics.StatOperation("EnqueueBackupChunks")(&err)

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.enqueueBackupChunks(ctx, session, snapshotID, entries)
		},
	)
}

func (s *storageYDB) GetQueuedChunksToBackup(
	ctx context.Context,
	firstShardID uint64,
	lastShardID uint64,
	after *BackupChunkQueueEntry,
	limit int,
) (entries []BackupChunkQueueEntry, err error) {

	defer s.metrics.StatOperation("GetQueuedChunksToBackup")(&err)

	err = s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			entries, err = s.getQueuedChunksToBackup(
				ctx,
				session,
				firstShardID,
				lastShardID,
				after,
				limit,
			)
			return err
		},
	)
	return entries, err
}

func (s *storageYDB) GetFirstQueuedChunkShardID(
	ctx context.Context,
	fromShardID uint64,
) (shardID uint64, found bool, err error) {

	defer s.metrics.StatOperation("GetFirstQueuedChunkShardID")(&err)

	err = s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			shardID, found, err = s.getFirstQueuedChunkShardID(
				ctx,
				session,
				fromShardID,
			)
			return err
		},
	)
	return shardID, found, err
}
