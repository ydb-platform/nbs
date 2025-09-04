package resources

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func setupTest(
	t *testing.T,
) (context.Context, *persistence.YDBClient, Storage) {

	ctx := newContext()
	db, err := newYDB(ctx)
	require.NoError(t, err)
	storage := newStorage(t, ctx, db)
	return ctx, db, storage
}

func requireDisksAreEqual(t *testing.T, expected DiskMeta, actual DiskMeta) {
	// TODO: Get rid of boilerplate.
	require.Equal(t, expected.ID, actual.ID)
	require.Equal(t, expected.ZoneID, actual.ZoneID)
	require.Equal(t, expected.SrcImageID, actual.SrcImageID)
	require.Equal(t, expected.SrcSnapshotID, actual.SrcSnapshotID)
	require.Equal(t, expected.BlocksCount, actual.BlocksCount)
	require.Equal(t, expected.BlockSize, actual.BlockSize)
	require.Equal(t, expected.Kind, actual.Kind)
	require.Equal(t, expected.CloudID, actual.CloudID)
	require.Equal(t, expected.FolderID, actual.FolderID)
	require.Equal(t, expected.PlacementGroupID, actual.PlacementGroupID)

	require.True(t, proto.Equal(expected.CreateRequest, actual.CreateRequest))
	require.Equal(t, expected.CreateTaskID, actual.CreateTaskID)
	if !expected.CreatingAt.IsZero() {
		require.WithinDuration(t, expected.CreatingAt, actual.CreatingAt, time.Microsecond)
	}
	if !expected.CreatedAt.IsZero() {
		require.WithinDuration(t, expected.CreatedAt, actual.CreatedAt, time.Microsecond)
	}
	require.Equal(t, expected.CreatedBy, actual.CreatedBy)
	require.Equal(t, expected.DeleteTaskID, actual.DeleteTaskID)
}

////////////////////////////////////////////////////////////////////////////////

func TestDisksCreateDisk(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	disk := DiskMeta{
		ID:               "disk",
		ZoneID:           "zone",
		SrcImageID:       "image",
		SrcSnapshotID:    "snapshot",
		BlocksCount:      1000000,
		BlockSize:        4096,
		Kind:             "ssd",
		CloudID:          "cloud",
		FolderID:         "folder",
		PlacementGroupID: "group",

		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)

	expected := disk
	expected.CreateRequest = nil
	requireDisksAreEqual(t, expected, *actual)

	// Check idempotency.
	actual, err = storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)

	disk.CreatedAt = time.Now()
	err = storage.DiskCreated(ctx, disk)
	require.NoError(t, err)

	// Check idempotency.
	err = storage.DiskCreated(ctx, disk)
	require.NoError(t, err)

	// Check idempotency.
	actual, err = storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)

	expected = disk
	expected.CreateRequest = nil
	requireDisksAreEqual(t, expected, *actual)

	disk.CreateTaskID = "other"
	actual, err = storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.Nil(t, actual)
}

func TestDisksDeleteDisk(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	disk := DiskMeta{
		ID: "disk",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)

	expected := disk
	expected.CreateRequest = nil
	expected.DeleteTaskID = "delete"

	actual, err = storage.DeleteDisk(ctx, disk.ID, "delete", time.Now())
	require.NoError(t, err)
	requireDisksAreEqual(t, expected, *actual)

	disk.CreatedAt = time.Now()
	err = storage.DiskCreated(ctx, disk)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	// Check idempotency.
	actual, err = storage.DeleteDisk(ctx, disk.ID, "delete", time.Now())
	require.NoError(t, err)
	requireDisksAreEqual(t, expected, *actual)

	_, err = storage.CreateDisk(ctx, disk)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.DiskDeleted(ctx, disk.ID, time.Now())
	require.NoError(t, err)

	// Check idempotency.
	actual, err = storage.DeleteDisk(ctx, disk.ID, "delete", time.Now())
	require.NoError(t, err)
	requireDisksAreEqual(t, expected, *actual)

	_, err = storage.CreateDisk(ctx, disk)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.DiskCreated(ctx, disk)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
}

func TestDisksDeleteNonexistentDisk(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	disk := DiskMeta{
		ID:           "disk",
		DeleteTaskID: "delete",
	}

	err := storage.DiskDeleted(ctx, disk.ID, time.Now())
	require.NoError(t, err)

	created := disk
	created.CreatedAt = time.Now()
	err = storage.DiskCreated(ctx, created)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	deletingAt := time.Now()
	actual, err := storage.DeleteDisk(ctx, disk.ID, "delete", deletingAt)
	require.NoError(t, err)
	requireDisksAreEqual(t, disk, *actual)

	// Check idempotency.
	deletingAt = deletingAt.Add(time.Second)
	actual, err = storage.DeleteDisk(ctx, disk.ID, "delete", deletingAt)
	require.NoError(t, err)
	requireDisksAreEqual(t, disk, *actual)

	_, err = storage.CreateDisk(ctx, disk)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.DiskDeleted(ctx, disk.ID, time.Now())
	require.NoError(t, err)
}

func TestDisksClearDeletedDisks(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	deletedAt := time.Now()
	deletedBefore := deletedAt.Add(-time.Microsecond)

	err := storage.ClearDeletedDisks(ctx, deletedBefore, 10)
	require.NoError(t, err)

	disk := DiskMeta{
		ID: "disk",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)

	_, err = storage.DeleteDisk(ctx, disk.ID, "delete", deletedAt)
	require.NoError(t, err)

	err = storage.DiskDeleted(ctx, disk.ID, deletedAt)
	require.NoError(t, err)

	err = storage.ClearDeletedDisks(ctx, deletedBefore, 10)
	require.NoError(t, err)

	_, err = storage.CreateDisk(ctx, disk)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	deletedBefore = deletedAt.Add(time.Microsecond)
	err = storage.ClearDeletedDisks(ctx, deletedBefore, 10)
	require.NoError(t, err)

	actual, err = storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)
}

func TestDisksGetDisk(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	diskID := t.Name()

	d, err := storage.GetDiskMeta(ctx, diskID)
	require.NoError(t, err)
	require.Nil(t, d)

	disk := DiskMeta{
		ID:               diskID,
		ZoneID:           "zone",
		SrcImageID:       "image",
		SrcSnapshotID:    "snapshot",
		BlocksCount:      1000000,
		BlockSize:        4096,
		Kind:             "ssd",
		CloudID:          "cloud",
		FolderID:         "folder",
		PlacementGroupID: "group",

		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)

	disk.CreateRequest = nil

	d, err = storage.GetDiskMeta(ctx, diskID)
	require.NoError(t, err)
	require.NotNil(t, d)
	requireDisksAreEqual(t, disk, *d)
}

func TestDiskScanned(t *testing.T) {
	ctx, _, storage := setupTest(t)

	diskID := "disk"

	err := storage.DiskScanned(ctx, diskID, time.Now(), false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	disk := DiskMeta{
		ID: diskID,
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
	}

	actual, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)

	err = storage.DiskScanned(ctx, diskID, time.Now(), false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.DiskCreated(ctx, *actual)
	require.NoError(t, err)

	require.False(t, actual.ScanFoundBrokenBlobs)

	scannedAt := time.Now()
	err = storage.DiskScanned(ctx, diskID, scannedAt, true)
	require.NoError(t, err)

	actual, err = storage.GetDiskMeta(ctx, diskID)
	require.NoError(t, err)
	require.NotNil(t, actual)

	require.WithinDuration(t, scannedAt, actual.ScannedAt, time.Microsecond)
	require.True(t, actual.ScanFoundBrokenBlobs)

	scannedAt = time.Now()
	err = storage.DiskScanned(ctx, diskID, scannedAt, false)
	require.NoError(t, err)

	actual, err = storage.GetDiskMeta(ctx, diskID)
	require.NoError(t, err)
	require.NotNil(t, actual)

	require.WithinDuration(t, scannedAt, actual.ScannedAt, time.Microsecond)
	require.False(t, actual.ScanFoundBrokenBlobs)
}

func TestDiskRelocated(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	diskID := t.Name()
	oldZoneID := "zone"
	dstZoneID := "other"

	diskRelocated := func(ctx context.Context, diskID string, srcZoneID, dstZoneID string) error {
		return db.Execute(
			ctx,
			func(ctx context.Context, session *persistence.Session) error {
				tx, err := session.BeginRWTransaction(ctx)
				if err != nil {
					return err
				}
				defer tx.Rollback(ctx)

				err = storage.DiskRelocated(ctx, tx, diskID, srcZoneID, dstZoneID)
				if err != nil {
					return err
				}

				return tx.Commit(ctx)
			},
		)
	}

	err := diskRelocated(ctx, diskID, oldZoneID, dstZoneID)
	require.Error(t, err)
	require.ErrorContains(t, err, "unable to find disk")

	diskMeta := DiskMeta{
		ID:     diskID,
		ZoneID: oldZoneID,
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreateDisk(ctx, diskMeta)
	require.NoError(t, err)
	require.NotNil(t, actual)

	err = diskRelocated(ctx, diskID, "another", dstZoneID)
	require.Error(t, err)
	require.ErrorContains(
		t,
		err,
		"Disk has been already relocated by another competing task.",
	)

	err = diskRelocated(ctx, diskID, oldZoneID, dstZoneID)
	require.NoError(t, err)

	actual, err = storage.GetDiskMeta(ctx, diskID)
	require.NoError(t, err)
	require.Equal(t, dstZoneID, actual.ZoneID)
}
