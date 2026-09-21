package mocks

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type StorageMock struct {
	mock.Mock
}

func (s *StorageMock) CreateSnapshot(
	ctx context.Context,
	snapshotMeta storage.SnapshotMeta,
) (*storage.SnapshotMeta, error) {

	args := s.Called(ctx, snapshotMeta)
	return args.Get(0).(*storage.SnapshotMeta), args.Error(1)
}

func (s *StorageMock) SnapshotCreated(
	ctx context.Context,
	snapshotID string,
	size uint64,
	storageSize uint64,
	chunkCount uint32,
	encryption *types.EncryptionDesc,
) error {

	args := s.Called(ctx, snapshotID, size, storageSize, chunkCount, encryption)
	return args.Error(0)
}

func (s *StorageMock) DeletingSnapshot(
	ctx context.Context,
	snapshotID string,
	taskID string,
) (*storage.SnapshotMeta, error) {

	args := s.Called(ctx, snapshotID, taskID)
	return args.Get(0).(*storage.SnapshotMeta), args.Error(1)
}

func (s *StorageMock) GetSnapshotsToDelete(
	ctx context.Context,
	deletingBefore time.Time,
	limit int,
) ([]*protos.DeletingSnapshotKey, error) {

	args := s.Called(ctx, deletingBefore, limit)
	return args.Get(0).([]*protos.DeletingSnapshotKey), args.Error(1)
}

func (s *StorageMock) DeleteSnapshotData(
	ctx context.Context,
	snapshotID string,
) error {

	args := s.Called(ctx, snapshotID)
	return args.Error(0)
}

func (s *StorageMock) ClearDeletingSnapshots(
	ctx context.Context,
	keys []*protos.DeletingSnapshotKey,
) error {

	args := s.Called(ctx, keys)
	return args.Error(0)
}

func (s *StorageMock) ShallowCopySnapshot(
	ctx context.Context,
	srcSnapshotID string,
	dstSnapshotID string,
	milestoneChunkIndex uint32,
	saveProgress func(context.Context, uint32) error,
) error {

	args := s.Called(
		ctx,
		srcSnapshotID,
		dstSnapshotID,
		milestoneChunkIndex,
		saveProgress,
	)
	return args.Error(0)
}

func (s *StorageMock) ShallowCopyChunk(
	ctx context.Context,
	srcEntry storage.ChunkMapEntry,
	dstSnapshotID string,
) error {

	args := s.Called(ctx, srcEntry, dstSnapshotID)
	return args.Error(0)
}

func (s *StorageMock) WriteChunk(
	ctx context.Context,
	uniqueID string,
	snapshotID string,
	chunk common.Chunk,
	useS3 bool,
) (string, error) {

	args := s.Called(ctx, uniqueID, snapshotID, chunk, useS3)
	return args.String(0), args.Error(1)
}

func (s *StorageMock) RewriteChunk(
	ctx context.Context,
	uniqueID string,
	snapshotID string,
	chunk common.Chunk,
	useS3 bool,
) (string, error) {

	args := s.Called(ctx, uniqueID, snapshotID, chunk, useS3)
	return args.String(0), args.Error(1)
}

func (s *StorageMock) ReadChunkMap(
	ctx context.Context,
	snapshotID string,
	milestoneChunkIndex uint32,
	includeShallowCopied bool,
) (<-chan storage.ChunkMapEntry, <-chan error) {

	args := s.Called(ctx, snapshotID, milestoneChunkIndex, includeShallowCopied)
	return args.Get(0).(<-chan storage.ChunkMapEntry), args.Get(1).(<-chan error)
}

func (s *StorageMock) ReadChunkBlob(
	ctx context.Context,
	chunkID string,
) (persistence.S3Object, error) {

	args := s.Called(ctx, chunkID)
	return args.Get(0).(persistence.S3Object), args.Error(1)
}

func (s *StorageMock) ReadChunk(
	ctx context.Context,
	chunk *common.Chunk,
) error {

	args := s.Called(ctx, chunk)
	return args.Error(0)
}

func (s *StorageMock) CheckSnapshotReady(
	ctx context.Context,
	snapshotID string,
) (storage.SnapshotMeta, error) {

	args := s.Called(ctx, snapshotID)
	return args.Get(0).(storage.SnapshotMeta), args.Error(1)
}

func (s *StorageMock) CheckSnapshotAlive(
	ctx context.Context,
	snapshotID string,
) error {

	args := s.Called(ctx, snapshotID)
	return args.Error(0)
}

func (s *StorageMock) GetDataChunkCount(
	ctx context.Context,
	snapshotID string,
) (uint64, error) {

	args := s.Called(ctx, snapshotID)
	return args.Get(0).(uint64), args.Error(0)
}

func (s *StorageMock) GetDeletingSnapshotCount(ctx context.Context) (uint64, error) {
	args := s.Called(ctx)
	return args.Get(0).(uint64), args.Error(1)
}

func (s *StorageMock) GetSnapshotCount(ctx context.Context) (uint64, error) {
	args := s.Called(ctx)
	return args.Get(0).(uint64), args.Error(1)
}

func (s *StorageMock) GetTotalSnapshotSize(ctx context.Context) (size uint64, err error) {
	args := s.Called(ctx)
	return args.Get(0).(uint64), args.Error(1)
}

func (s *StorageMock) GetTotalSnapshotStorageSize(ctx context.Context) (storageSize uint64, err error) {
	args := s.Called(ctx)
	return args.Get(0).(uint64), args.Error(1)
}

func (s *StorageMock) DeleteDiskFromIncremental(
	ctx context.Context,
	zoneID string,
	diskID string,
) error {

	args := s.Called(ctx, zoneID, diskID)
	return args.Error(0)
}

func (s *StorageMock) LockSnapshot(
	ctx context.Context,
	snapshotID string,
	lockTaskID string,
) (locked bool, err error) {

	args := s.Called(ctx, snapshotID, lockTaskID)
	return args.Get(0).(bool), args.Error(1)
}

func (s *StorageMock) UnlockSnapshot(
	ctx context.Context,
	snapshotID string,
	lockTaskID string,
) error {

	args := s.Called(ctx, snapshotID, lockTaskID)
	return args.Error(0)
}

func (s *StorageMock) GetSnapshotMeta(
	ctx context.Context,
	snapshotID string,
) (*storage.SnapshotMeta, error) {

	args := s.Called(ctx, snapshotID)
	return args.Get(0).(*storage.SnapshotMeta), args.Error(1)
}

func (s *StorageMock) GetIncremental(
	ctx context.Context,
	disk *types.Disk,
) (snapshotID string, checkpointID string, err error) {

	args := s.Called(ctx, snapshotID)
	return args.String(0), args.String(1), args.Error(2)
}

func (s *StorageMock) ListSnapshots(
	ctx context.Context,
) (tasks_common.StringSet, error) {

	args := s.Called(ctx)
	return args.Get(0).(tasks_common.StringSet), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewStorageMock() *StorageMock {
	return &StorageMock{}
}

func (s *StorageMock) EnqueueBackupChunks(
	ctx context.Context,
	entries []storage.BackupChunkQueueEntry,
) error {

	args := s.Called(ctx, entries)
	return args.Error(0)
}

func (s *StorageMock) GetBackupChunkQueue(
	ctx context.Context,
	limit int,
) ([]storage.BackupChunkQueueEntry, error) {

	args := s.Called(ctx, limit)
	return args.Get(0).([]storage.BackupChunkQueueEntry), args.Error(1)
}

func (s *StorageMock) HasBackupChunkQueueEntries(
	ctx context.Context,
	snapshotID string,
) (bool, error) {

	args := s.Called(ctx, snapshotID)
	return args.Bool(0), args.Error(1)
}

func (s *StorageMock) ChunksBackupCompleted(
	ctx context.Context,
	entries []storage.BackupChunkQueueEntry,
) error {

	args := s.Called(ctx, entries)
	return args.Error(0)
}

func (s *StorageMock) GetBackupChunkQueueLength(
	ctx context.Context,
) (uint64, error) {

	args := s.Called(ctx)
	return args.Get(0).(uint64), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that StorageMock implements storage.Storage.
func assertStorageMockIsStorage(arg *StorageMock) storage.Storage {
	return arg
}
