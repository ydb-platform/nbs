package resources

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func requireSnapshotsAreEqual(t *testing.T, expected SnapshotMeta, actual SnapshotMeta) {
	require.Equal(t, expected.ID, actual.ID)
	require.Equal(t, expected.FolderID, actual.FolderID)
	require.True(t, proto.Equal(expected.Disk, actual.Disk))
	require.Equal(t, expected.CheckpointID, actual.CheckpointID)
	require.True(t, proto.Equal(expected.CreateRequest, actual.CreateRequest))
	require.Equal(t, expected.CreateTaskID, actual.CreateTaskID)
	if !expected.CreatingAt.IsZero() {
		require.WithinDuration(t, expected.CreatingAt, actual.CreatingAt, time.Microsecond)
	}
	require.Equal(t, expected.CreatedBy, actual.CreatedBy)
	require.Equal(t, expected.DeleteTaskID, actual.DeleteTaskID)
	require.Equal(t, expected.Size, actual.Size)
	require.Equal(t, expected.StorageSize, actual.StorageSize)
	require.Equal(t, expected.Ready, actual.Ready)
}

////////////////////////////////////////////////////////////////////////////////

func TestSnapshotsCreateSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	snapshot := SnapshotMeta{
		ID:       "snapshot",
		FolderID: "folder",
		Disk: &types.Disk{
			ZoneId: "zone",
			DiskId: "disk",
		},
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	created, err := storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)
	require.Equal(t, snapshot.ID, created.ID)

	// Check idempotency.
	created, err = storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)
	require.Equal(t, snapshot.ID, created.ID)

	err = storage.SnapshotCreated(ctx, snapshot.ID, "", time.Now(), 0, 0)
	require.NoError(t, err)

	// Check idempotency.
	err = storage.SnapshotCreated(ctx, snapshot.ID, "", time.Now(), 0, 0)
	require.NoError(t, err)

	// Check idempotency.
	created, err = storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)
	require.Equal(t, snapshot.ID, created.ID)

	snapshot.CreateTaskID = "other"
	_, err = storage.CreateSnapshot(ctx, snapshot)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonCancellableError()))
}

func TestSnapshotsDeleteSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	snapshot := SnapshotMeta{
		ID:       "snapshot",
		FolderID: "folder",
		Disk: &types.Disk{
			ZoneId: "zone",
			DiskId: "disk",
		},
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	created, err := storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)
	require.Equal(t, snapshot.ID, created.ID)

	expected := snapshot
	expected.CreateRequest = nil
	expected.DeleteTaskID = "delete"

	actual, err := storage.DeleteSnapshot(ctx, snapshot.ID, "delete", time.Now())
	require.NoError(t, err)
	requireSnapshotsAreEqual(t, expected, *actual)

	err = storage.SnapshotCreated(ctx, snapshot.ID, "", time.Now(), 0, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	// Check idempotency.
	actual, err = storage.DeleteSnapshot(ctx, snapshot.ID, "delete", time.Now())
	require.NoError(t, err)
	requireSnapshotsAreEqual(t, expected, *actual)

	_, err = storage.CreateSnapshot(ctx, snapshot)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.SnapshotDeleted(ctx, snapshot.ID, time.Now())
	require.NoError(t, err)

	// Check idempotency.
	actual, err = storage.DeleteSnapshot(ctx, snapshot.ID, "delete", time.Now())
	require.NoError(t, err)
	requireSnapshotsAreEqual(t, expected, *actual)

	_, err = storage.CreateSnapshot(ctx, snapshot)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.SnapshotCreated(ctx, snapshot.ID, "", time.Now(), 0, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
}

func TestSnapshotsDeleteNonexistentSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	snapshot := SnapshotMeta{
		ID:           "snapshot",
		Disk:         &types.Disk{},
		DeleteTaskID: "delete",
	}

	err = storage.SnapshotDeleted(ctx, snapshot.ID, time.Now())
	require.NoError(t, err)

	err = storage.SnapshotCreated(ctx, snapshot.ID, "", time.Now(), 0, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	deletingAt := time.Now()
	actual, err := storage.DeleteSnapshot(ctx, snapshot.ID, "delete", deletingAt)
	require.NoError(t, err)
	require.Nil(t, actual)

	// Check idempotency.
	deletingAt = deletingAt.Add(time.Second)
	actual, err = storage.DeleteSnapshot(ctx, snapshot.ID, "delete", deletingAt)
	require.NoError(t, err)
	require.Nil(t, actual)

	_, err = storage.CreateSnapshot(ctx, snapshot)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.SnapshotDeleted(ctx, snapshot.ID, time.Now())
	require.NoError(t, err)
}

func TestSnapshotsClearDeletedSnapshots(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	deletedAt := time.Now()
	deletedBefore := deletedAt.Add(-time.Microsecond)

	err = storage.ClearDeletedSnapshots(ctx, deletedBefore, 10)
	require.NoError(t, err)

	snapshot := SnapshotMeta{
		ID:       "snapshot",
		FolderID: "folder",
		Disk: &types.Disk{
			ZoneId: "zone",
			DiskId: "disk",
		},
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	created, err := storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)
	require.Equal(t, snapshot.ID, created.ID)

	_, err = storage.DeleteSnapshot(ctx, snapshot.ID, "delete", deletedAt)
	require.NoError(t, err)

	err = storage.SnapshotDeleted(ctx, snapshot.ID, deletedAt)
	require.NoError(t, err)

	err = storage.ClearDeletedSnapshots(ctx, deletedBefore, 10)
	require.NoError(t, err)

	_, err = storage.CreateSnapshot(ctx, snapshot)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	deletedBefore = deletedAt.Add(time.Microsecond)
	err = storage.ClearDeletedSnapshots(ctx, deletedBefore, 10)
	require.NoError(t, err)

	created, err = storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)
	require.Equal(t, snapshot.ID, created.ID)
}

func TestSnapshotsCreateSnapshotShouldFailIfImageAlreadyExists(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	image := ImageMeta{
		ID: "id",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreatingAt: time.Now(),
	}
	_, err = storage.CreateImage(ctx, image)
	require.NoError(t, err)

	_, err = storage.CreateSnapshot(ctx, SnapshotMeta{ID: image.ID})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonCancellableError()))
}

func TestSnapshotsDeleteSnapshotShouldFailIfImageAlreadyExists(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	image := ImageMeta{
		ID: "id",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreatingAt: time.Now(),
	}
	_, err = storage.CreateImage(ctx, image)
	require.NoError(t, err)

	created, err := storage.DeleteSnapshot(ctx, image.ID, "delete", time.Now())
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonCancellableError()))
	require.Nil(t, created)
}

func TestSnapshotsGetSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	snapshotID := t.Name()
	snapshotSize := uint64(2 * 1024 * 1024)
	snapshotStorageSize := uint64(3 * 1024 * 1024)
	checkpointID := "checkpoint"

	actualSnapshot, err := storage.GetSnapshotMeta(ctx, snapshotID)
	require.NoError(t, err)
	require.Nil(t, actualSnapshot)

	snapshot := SnapshotMeta{
		ID:       snapshotID,
		FolderID: "folder",
		Disk: &types.Disk{
			ZoneId: "zone",
			DiskId: "disk",
		},
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	created, err := storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)
	require.Equal(t, snapshot.ID, created.ID)

	expectedSnapshot := snapshot
	expectedSnapshot.CreateRequest = nil

	checkSnapshot := func() {
		actualSnapshot, err := storage.GetSnapshotMeta(ctx, snapshotID)
		require.NoError(t, err)
		require.NotNil(t, actualSnapshot)
		requireSnapshotsAreEqual(t, expectedSnapshot, *actualSnapshot)
	}
	checkSnapshot()

	err = storage.SnapshotCreated(
		ctx,
		snapshotID,
		checkpointID,
		time.Now(),
		snapshotSize,
		snapshotStorageSize,
	)
	require.NoError(t, err)

	expectedSnapshot.Size = snapshotSize
	expectedSnapshot.StorageSize = snapshotStorageSize
	expectedSnapshot.CheckpointID = checkpointID
	expectedSnapshot.Ready = true
	checkSnapshot()

	// Check idempotency.
	err = storage.SnapshotCreated(
		ctx,
		snapshotID,
		checkpointID,
		time.Now(),
		snapshotSize,
		snapshotStorageSize,
	)
	require.NoError(t, err)
	checkSnapshot()

	// Checkpoint id differs.
	err = storage.SnapshotCreated(
		ctx,
		snapshotID,
		"foo", // checkpointID
		time.Now(),
		snapshotSize,
		snapshotStorageSize,
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	checkSnapshot()

	// Snapshot size differs.
	err = storage.SnapshotCreated(
		ctx,
		snapshotID,
		checkpointID,
		time.Now(),
		42, // snapshotSize
		snapshotStorageSize,
	)
	require.NoError(t, err)
	checkSnapshot()

	// Snapshot storage size differs.
	err = storage.SnapshotCreated(
		ctx,
		snapshotID,
		checkpointID,
		time.Now(),
		snapshotSize,
		713, // snapshotStorageSize
	)
	require.NoError(t, err)
	checkSnapshot()
}
