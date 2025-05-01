package storage

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
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
	chunkStorageS3           *chunks.StorageS3
	chunkStorageYDB          *chunks.StorageYDB
}

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
				nil,
			)
			return nil
		},
	)
	if err != nil {
		entries := make(chan ChunkMapEntry)
		errors := make(chan error, 1)
		errors <- err
		close(entries)
		close(errors)
		return entries, errors
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

func (s *storageYDB) ListAllSnapshots(
	ctx context.Context,
) (ids map[string]struct{}, err error) {

	err = s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			ids, err = s.listAllSnapshots(ctx, session)
			return err
		},
	)
	return ids, err
}
