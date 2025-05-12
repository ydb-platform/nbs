package storage

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	task_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type legacyStorage struct {
	db         *persistence.YDBClient
	tablesPath string
	metrics    metrics.Metrics
}

////////////////////////////////////////////////////////////////////////////////

func (s *legacyStorage) ReadChunkMap(
	ctx context.Context,
	snapshotID string,
	milestoneChunkIndex uint32,
) (<-chan ChunkMapEntry, <-chan error) {

	var entries <-chan ChunkMapEntry
	var errors <-chan error

	replyError := func(err error) (<-chan ChunkMapEntry, <-chan error) {
		entries := make(chan ChunkMapEntry)
		errors := make(chan error, 1)
		errors <- err
		close(entries)
		close(errors)
		return entries, errors
	}

	if len(s.tablesPath) == 0 {
		return replyError(task_errors.NewNonRetriableErrorf(
			"legacy snapshot storage does not exist",
		))
	}

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			entries, errors = s.readChunkMap(
				ctx,
				session,
				snapshotID,
				milestoneChunkIndex,
				nil,
			)
			return nil
		},
	)
	if err != nil {
		return replyError(err)
	}

	return entries, errors
}

func (s *legacyStorage) ReadChunk(
	ctx context.Context,
	chunk *common.Chunk,
) error {

	if len(s.tablesPath) == 0 {
		return task_errors.NewNonRetriableErrorf(
			"legacy snapshot storage does not exist",
		)
	}

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.readChunk(ctx, session, chunk)
		},
	)
}

func (s *legacyStorage) CheckSnapshotReady(
	ctx context.Context,
	snapshotID string,
) (SnapshotMeta, error) {

	if len(s.tablesPath) == 0 {
		return SnapshotMeta{}, task_errors.NewNonRetriableErrorf(
			"legacy snapshot storage does not exist",
		)
	}

	var meta SnapshotMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			meta, err = s.checkSnapshotReady(
				ctx,
				session,
				snapshotID,
			)
			return err
		},
	)
	return meta, err
}

////////////////////////////////////////////////////////////////////////////////

func (s *legacyStorage) CreateSnapshot(
	ctx context.Context,
	snapshotMeta SnapshotMeta,
) (*SnapshotMeta, error) {

	return nil, task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) SnapshotCreated(
	ctx context.Context,
	snapshotID string,
	size uint64,
	storageSize uint64,
	chunkCount uint32,
	encryption *types.EncryptionDesc,
) error {

	return task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) DeletingSnapshot(
	ctx context.Context,
	snapshotID string,
	taskID string,
) (*SnapshotMeta, error) {

	return nil, task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) GetSnapshotsToDelete(
	ctx context.Context,
	deletingBefore time.Time,
	limit int,
) ([]*protos.DeletingSnapshotKey, error) {

	return nil, task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) DeleteSnapshotData(
	ctx context.Context,
	snapshotID string,
) error {

	return task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) ClearDeletingSnapshots(
	ctx context.Context,
	keys []*protos.DeletingSnapshotKey,
) error {

	return task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) ShallowCopySnapshot(
	ctx context.Context,
	srcSnapshotID string,
	dstSnapshotID string,
	milestoneChunkIndex uint32,
	saveProgress func(context.Context, uint32) error,
) error {

	return task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) ShallowCopyChunk(
	ctx context.Context,
	srcEntry ChunkMapEntry,
	dstSnapshotID string,
) error {

	return task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) WriteChunk(
	ctx context.Context,
	uniqueID string,
	snapshotID string,
	chunk common.Chunk,
	useS3 bool,
) (string, error) {

	return "", task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) RewriteChunk(
	ctx context.Context,
	uniqueID string,
	snapshotID string,
	chunk common.Chunk,
	useS3 bool,
) (string, error) {

	return "", task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) CheckSnapshotAlive(
	ctx context.Context,
	snapshotID string,
) error {

	return task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) GetDataChunkCount(
	ctx context.Context,
	snapshotID string,
) (uint64, error) {

	return 0, task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) GetDeletingSnapshotCount(ctx context.Context) (uint64, error) {
	return 0, task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) GetSnapshotCount(ctx context.Context) (uint64, error) {
	return 0, task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) GetTotalSnapshotSize(ctx context.Context) (size uint64, err error) {
	return 0, task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) GetTotalSnapshotStorageSize(ctx context.Context) (storageSize uint64, err error) {
	return 0, task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) DeleteDiskFromIncremental(
	ctx context.Context,
	zoneID string,
	diskID string,
) error {

	return task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) LockSnapshot(
	ctx context.Context,
	snapshotID string,
	lockTaskID string,
) (locked bool, err error) {

	return false, task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) UnlockSnapshot(
	ctx context.Context,
	snapshotID string,
	lockTaskID string,
) error {

	return task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) GetSnapshotMeta(
	ctx context.Context,
	snapshotID string,
) (*SnapshotMeta, error) {

	return nil, task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) GetIncremental(
	ctx context.Context,
	disk *types.Disk,
) (snapshotID string, checkpointID string, err error) {

	return "", "", task_errors.NewNonRetriableErrorf("not implemented")
}

func (s *legacyStorage) ListAllSnapshots(ctx context.Context) (task_storage.StringSet, error) {
	return task_storage.NewStringSet(), task_errors.NewNonRetriableErrorf("not implemented")
}
