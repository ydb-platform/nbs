package mocks

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type StorageMock struct {
	mock.Mock
}

func (s *StorageMock) CreateDisk(
	ctx context.Context,
	disk resources.DiskMeta,
) (*resources.DiskMeta, error) {

	args := s.Called(ctx, disk)
	return args.Get(0).(*resources.DiskMeta), args.Error(1)
}

func (s *StorageMock) DiskCreated(
	ctx context.Context,
	disk resources.DiskMeta,
) error {

	args := s.Called(ctx, disk)
	return args.Error(0)
}

func (s *StorageMock) GetDiskMeta(
	ctx context.Context,
	diskID string,
) (*resources.DiskMeta, error) {

	args := s.Called(ctx, diskID)
	return args.Get(0).(*resources.DiskMeta), args.Error(1)
}

func (s *StorageMock) DeleteDisk(
	ctx context.Context,
	diskID string,
	taskID string,
	deletingAt time.Time,
) (*resources.DiskMeta, error) {

	args := s.Called(ctx, diskID, taskID, deletingAt)
	return args.Get(0).(*resources.DiskMeta), args.Error(1)
}

func (s *StorageMock) DiskDeleted(
	ctx context.Context,
	diskID string,
	deletedAt time.Time,
) error {

	args := s.Called(ctx, diskID, deletedAt)
	return args.Error(0)
}

func (s *StorageMock) ClearDeletedDisks(
	ctx context.Context,
	deletedBefore time.Time,
	limit int,
) error {

	args := s.Called(ctx, deletedBefore, limit)
	return args.Error(0)
}

func (s *StorageMock) ListDisks(
	ctx context.Context,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	args := s.Called(ctx, folderID, creatingBefore)
	return args.Get(0).([]string), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func (s *StorageMock) CreateImage(
	ctx context.Context,
	image resources.ImageMeta,
) (resources.ImageMeta, error) {

	args := s.Called(ctx, image)
	return args.Get(0).(resources.ImageMeta), args.Error(1)
}

func (s *StorageMock) ImageCreated(
	ctx context.Context,
	imageID string,
	checkpointID string,
	createdAt time.Time,
	imageSize uint64,
	imageStorageSize uint64,
) error {

	args := s.Called(
		ctx,
		imageID,
		checkpointID,
		createdAt,
		imageSize,
		imageStorageSize,
	)
	return args.Error(0)
}

func (s *StorageMock) GetImageMeta(
	ctx context.Context,
	imageID string,
) (*resources.ImageMeta, error) {

	args := s.Called(ctx, imageID)
	return args.Get(0).(*resources.ImageMeta), args.Error(1)
}

func (s *StorageMock) DeleteImage(
	ctx context.Context,
	imageID string,
	taskID string,
	deletingAt time.Time,
) (*resources.ImageMeta, error) {

	args := s.Called(ctx, imageID, taskID, deletingAt)
	return args.Get(0).(*resources.ImageMeta), args.Error(1)
}

func (s *StorageMock) ImageDeleted(
	ctx context.Context,
	imageID string,
	deletedAt time.Time,
) error {

	args := s.Called(ctx, imageID, deletedAt)
	return args.Error(0)
}

func (s *StorageMock) ClearDeletedImages(
	ctx context.Context,
	deletedBefore time.Time,
	limit int,
) error {

	args := s.Called(ctx, deletedBefore, limit)
	return args.Error(0)
}

func (s *StorageMock) ListImages(
	ctx context.Context,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	args := s.Called(ctx, folderID, creatingBefore)
	return args.Get(0).([]string), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func (s *StorageMock) CreateSnapshot(
	ctx context.Context,
	snapshot resources.SnapshotMeta,
) (resources.SnapshotMeta, error) {

	args := s.Called(ctx, snapshot)
	return args.Get(0).(resources.SnapshotMeta), args.Error(1)
}

func (s *StorageMock) SnapshotCreated(
	ctx context.Context,
	snapshotID string,
	checkpointID string,
	createdAt time.Time,
	snapshotSize uint64,
	snapshotStorageSize uint64,
) error {

	args := s.Called(
		ctx,
		snapshotID,
		checkpointID,
		createdAt,
		snapshotSize,
		snapshotStorageSize,
	)
	return args.Error(0)
}

func (s *StorageMock) GetSnapshotMeta(
	ctx context.Context,
	snapshotID string,
) (*resources.SnapshotMeta, error) {

	args := s.Called(ctx, snapshotID)
	return args.Get(0).(*resources.SnapshotMeta), args.Error(1)
}

func (s *StorageMock) DeleteSnapshot(
	ctx context.Context,
	snapshotID string,
	taskID string,
	deletingAt time.Time,
) (*resources.SnapshotMeta, error) {

	args := s.Called(ctx, snapshotID, taskID, deletingAt)
	return args.Get(0).(*resources.SnapshotMeta), args.Error(1)
}

func (s *StorageMock) SnapshotDeleted(
	ctx context.Context,
	snapshotID string,
	deletedAt time.Time,
) error {

	args := s.Called(ctx, snapshotID, deletedAt)
	return args.Error(0)
}

func (s *StorageMock) ClearDeletedSnapshots(
	ctx context.Context,
	deletedBefore time.Time,
	limit int,
) error {

	args := s.Called(ctx, deletedBefore, limit)
	return args.Error(0)
}

func (s *StorageMock) LockSnapshot(
	ctx context.Context,
	snapshotID string,
	lockTaskID string,
) (bool, error) {

	args := s.Called(ctx, snapshotID, lockTaskID)
	return args.Bool(0), args.Error(1)
}

func (s *StorageMock) UnlockSnapshot(
	ctx context.Context,
	snapshotID string,
	lockTaskID string,
) error {

	args := s.Called(ctx, snapshotID, lockTaskID)
	return args.Error(0)
}

func (s *StorageMock) ListSnapshots(
	ctx context.Context,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	args := s.Called(ctx, folderID, creatingBefore)
	return args.Get(0).([]string), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func (s *StorageMock) CreateFilesystem(
	ctx context.Context,
	filesystem resources.FilesystemMeta,
) (*resources.FilesystemMeta, error) {

	args := s.Called(ctx, filesystem)
	return args.Get(0).(*resources.FilesystemMeta), args.Error(1)
}

func (s *StorageMock) FilesystemCreated(
	ctx context.Context,
	filesystem resources.FilesystemMeta,
) error {

	args := s.Called(ctx, filesystem)
	return args.Error(0)
}

func (s *StorageMock) GetFilesystemMeta(
	ctx context.Context,
	filesystemID string,
) (*resources.FilesystemMeta, error) {

	args := s.Called(ctx, filesystemID)
	return args.Get(0).(*resources.FilesystemMeta), args.Error(1)
}

func (s *StorageMock) DeleteFilesystem(
	ctx context.Context,
	filesystemID string,
	taskID string,
	deletingAt time.Time,
) (*resources.FilesystemMeta, error) {

	args := s.Called(ctx, filesystemID, taskID, deletingAt)
	return args.Get(0).(*resources.FilesystemMeta), args.Error(1)
}

func (s *StorageMock) FilesystemDeleted(
	ctx context.Context,
	filesystemID string,
	deletedAt time.Time,
) error {

	args := s.Called(ctx, filesystemID, deletedAt)
	return args.Error(0)
}

func (s *StorageMock) ClearDeletedFilesystems(
	ctx context.Context,
	deletedBefore time.Time,
	limit int,
) error {

	args := s.Called(ctx, deletedBefore, limit)
	return args.Error(0)
}

func (s *StorageMock) ListFilesystems(
	ctx context.Context,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	args := s.Called(ctx, folderID, creatingBefore)
	return args.Get(0).([]string), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func (s *StorageMock) CreatePlacementGroup(
	ctx context.Context,
	placementGroup resources.PlacementGroupMeta,
) (*resources.PlacementGroupMeta, error) {

	args := s.Called(ctx, placementGroup)
	return args.Get(0).(*resources.PlacementGroupMeta), args.Error(1)
}

func (s *StorageMock) PlacementGroupCreated(
	ctx context.Context,
	placementGroup resources.PlacementGroupMeta,
) error {

	args := s.Called(ctx, placementGroup)
	return args.Error(0)
}

func (s *StorageMock) GetPlacementGroupMeta(
	ctx context.Context,
	placementGroupID string,
) (*resources.PlacementGroupMeta, error) {

	args := s.Called(ctx, placementGroupID)
	return args.Get(0).(*resources.PlacementGroupMeta), args.Error(1)
}

func (s *StorageMock) CheckPlacementGroupReady(
	ctx context.Context,
	placementGroupID string,
) (*resources.PlacementGroupMeta, error) {

	args := s.Called(ctx, placementGroupID)
	return args.Get(0).(*resources.PlacementGroupMeta), args.Error(1)
}

func (s *StorageMock) DeletePlacementGroup(
	ctx context.Context,
	placementGroupID string,
	taskID string,
	deletingAt time.Time,
) (*resources.PlacementGroupMeta, error) {

	args := s.Called(ctx, placementGroupID, taskID, deletingAt)
	return args.Get(0).(*resources.PlacementGroupMeta), args.Error(1)
}

func (s *StorageMock) PlacementGroupDeleted(
	ctx context.Context,
	placementGroupID string,
	deletedAt time.Time,
) error {

	args := s.Called(ctx, placementGroupID, deletedAt)
	return args.Error(0)
}

func (s *StorageMock) ClearDeletedPlacementGroups(
	ctx context.Context,
	deletedBefore time.Time,
	limit int,
) error {

	args := s.Called(ctx, deletedBefore, limit)
	return args.Error(0)
}

func (s *StorageMock) ListPlacementGroups(
	ctx context.Context,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	args := s.Called(ctx, folderID, creatingBefore)
	return args.Get(0).([]string), args.Error(1)
}

func (s *StorageMock) IncrementFillGeneration(
	ctx context.Context,
	diskID string,
) (uint64, error) {

	args := s.Called(ctx, diskID)
	return args.Get(0).(uint64), args.Error(1)
}

func (s *StorageMock) DiskScanned(
	ctx context.Context,
	diskID string,
	scannedAt time.Time,
	foundBrokenBlobs bool,
) error {

	args := s.Called(ctx, diskID, scannedAt, foundBrokenBlobs)
	return args.Error(0)
}

func (s *StorageMock) DiskRelocated(
	ctx context.Context,
	tx *persistence.Transaction,
	diskID string,
	srcZoneID string,
	dstZoneID string,
) error {

	args := s.Called(ctx, tx, diskID, srcZoneID, dstZoneID)
	return args.Error(0)
}

////////////////////////////////////////////////////////////////////////////////

func NewStorageMock() *StorageMock {
	return &StorageMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that StorageMock implements resources.Storage.
func assertStorageMockIsStorage(arg *StorageMock) resources.Storage {
	return arg
}
