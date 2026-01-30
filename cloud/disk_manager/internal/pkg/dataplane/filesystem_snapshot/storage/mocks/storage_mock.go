package mocks

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/storage/protos"
	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
)

////////////////////////////////////////////////////////////////////////////////

type StorageMock struct {
	mock.Mock
}

func (s *StorageMock) CreateFilesystemSnapshot(
	ctx context.Context,
	snapshotMeta storage.FilesystemSnapshotMeta,
) (*storage.FilesystemSnapshotMeta, error) {

	args := s.Called(ctx, snapshotMeta)
	return args.Get(0).(*storage.FilesystemSnapshotMeta), args.Error(1)
}

func (s *StorageMock) FilesystemSnapshotCreated(
	ctx context.Context,
	snapshotID string,
	size uint64,
	storageSize uint64,
	chunkCount uint32,
) error {

	args := s.Called(ctx, snapshotID, size, storageSize, chunkCount)
	return args.Error(0)
}

func (s *StorageMock) DeletingFilesystemSnapshot(
	ctx context.Context,
	snapshotID string,
	taskID string,
) (*storage.FilesystemSnapshotMeta, error) {

	args := s.Called(ctx, snapshotID, taskID)
	return args.Get(0).(*storage.FilesystemSnapshotMeta), args.Error(1)
}

func (s *StorageMock) GetFilesystemSnapshotsToDelete(
	ctx context.Context,
	deletingBefore time.Time,
	limit int,
) ([]*protos.DeletingFilesystemSnapshotKey, error) {

	args := s.Called(ctx, deletingBefore, limit)
	return args.Get(0).([]*protos.DeletingFilesystemSnapshotKey), args.Error(1)
}

func (s *StorageMock) ClearDeletingFilesystemSnapshots(
	ctx context.Context,
	keys []*protos.DeletingFilesystemSnapshotKey,
) error {

	args := s.Called(ctx, keys)
	return args.Error(0)
}

func (s *StorageMock) CheckFilesystemSnapshotAlive(
	ctx context.Context,
	snapshotID string,
) error {

	args := s.Called(ctx, snapshotID)
	return args.Error(0)
}

func (s *StorageMock) GetFilesystemSnapshotCount(ctx context.Context) (uint64, error) {
	args := s.Called(ctx)
	return args.Get(0).(uint64), args.Error(1)
}

func (s *StorageMock) GetTotalFilesystemSnapshotSize(ctx context.Context) (size uint64, err error) {
	args := s.Called(ctx)
	return args.Get(0).(uint64), args.Error(1)
}

func (s *StorageMock) GetTotalFilesystemSnapshotStorageSize(ctx context.Context) (storageSize uint64, err error) {
	args := s.Called(ctx)
	return args.Get(0).(uint64), args.Error(1)
}

func (s *StorageMock) LockFilesystemSnapshot(
	ctx context.Context,
	snapshotID string,
	lockTaskID string,
) (locked bool, err error) {

	args := s.Called(ctx, snapshotID, lockTaskID)
	return args.Get(0).(bool), args.Error(1)
}

func (s *StorageMock) UnlockFilesystemSnapshot(
	ctx context.Context,
	snapshotID string,
	lockTaskID string,
) error {

	args := s.Called(ctx, snapshotID, lockTaskID)
	return args.Error(0)
}

func (s *StorageMock) GetFilesystemSnapshotMeta(
	ctx context.Context,
	snapshotID string,
) (*storage.FilesystemSnapshotMeta, error) {

	args := s.Called(ctx, snapshotID)
	return args.Get(0).(*storage.FilesystemSnapshotMeta), args.Error(1)
}

func (s *StorageMock) ListFilesystemSnapshots(
	ctx context.Context,
) (tasks_common.StringSet, error) {

	args := s.Called(ctx)
	return args.Get(0).(tasks_common.StringSet), args.Error(1)
}

func (s *StorageMock) ScheduleRootNodeForListing(
	ctx context.Context,
	snapshotID string,
) error {

	args := s.Called(ctx, snapshotID)
	return args.Error(0)
}

func (s *StorageMock) SelectNodesToList(
	ctx context.Context,
	snapshotID string,
	nodesToExclude map[uint64]struct{},
	limit uint64,
) ([]storage.NodeQueueEntry, error) {

	args := s.Called(ctx, snapshotID, nodesToExclude, limit)
	return args.Get(0).([]storage.NodeQueueEntry), args.Error(1)
}

func (s *StorageMock) ScheduleChildNodesForListing(
	ctx context.Context,
	snapshotID string,
	nodeID uint64,
	nextCookie string,
	depth uint64,
	children []nfs.Node,
) error {

	args := s.Called(ctx, snapshotID, nodeID, nextCookie, depth, children)
	return args.Error(0)
}

////////////////////////////////////////////////////////////////////////////////

func NewStorageMock() *StorageMock {
	return &StorageMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that StorageMock implements storage.Storage.
func assertStorageMockIsStorage(arg *StorageMock) storage.Storage {
	return arg
}
