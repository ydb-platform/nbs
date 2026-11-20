package resources

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
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
	require.True(t, actual.UseDataplaneTasks)
	require.Equal(t, expected.Size, actual.Size)
	require.Equal(t, expected.StorageSize, actual.StorageSize)
	require.Equal(t, expected.Ready, actual.Ready)
	require.Equal(t, expected.Checksum, actual.Checksum)
	require.Equal(t, expected.ChecksumCalculated, actual.ChecksumCalculated)
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
	require.True(t, created.UseDataplaneTasks)

	meta, err := storage.GetSnapshotMeta(ctx, snapshot.ID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.UseDataplaneTasks)

	// Check idempotency.
	created, err = storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)
	require.Equal(t, snapshot.ID, created.ID)

	err = storage.SnapshotCreated(ctx, snapshot.ID, "", time.Now(), 0, 0)
	require.NoError(t, err)

	meta, err = storage.GetSnapshotMeta(ctx, snapshot.ID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.UseDataplaneTasks)
	require.True(t, meta.Ready)

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

func TestSnapshotsBackup(t *testing.T) {
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

	_, err = storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)

	ids, err := storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)

	err = storage.SnapshotCreated(ctx, snapshot.ID, "checkpoint", time.Now(), 0, 0)
	require.NoError(t, err)

	// A snapshot is backed up only when it is queued explicitly.
	ids, err = storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)

	err = storage.EnqueueSnapshotBackup(ctx, snapshot.ID, "backup")
	require.NoError(t, err)

	ids, err = storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(
		t,
		[]SnapshotBackupRequest{{SnapshotID: snapshot.ID, BackupID: "backup"}},
		ids,
	)

	err = storage.RemoveSnapshotFromBackupQueue(ctx, snapshot.ID, "backup")
	require.NoError(t, err)

	ids, err = storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)

	// Check idempotency.
	err = storage.RemoveSnapshotFromBackupQueue(ctx, snapshot.ID, "backup")
	require.NoError(t, err)
}

func TestSnapshotsBackupQueuesNewReadySnapshotOfConfiguredFolder(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	for _, folderID := range []string{"backed", "other"} {
		snapshot := SnapshotMeta{
			ID:       "snapshot_" + folderID,
			FolderID: folderID,
			Disk: &types.Disk{
				ZoneId: "zone",
				DiskId: "disk",
			},
			CreateRequest: &wrappers.UInt64Value{
				Value: 1,
			},
			CreateTaskID: "create_" + folderID,
			CreatingAt:   time.Now(),
			CreatedBy:    "user",
		}

		_, err = storage.CreateSnapshot(ctx, snapshot)
		require.NoError(t, err)

		err = storage.SnapshotCreated(ctx, snapshot.ID, "checkpoint", time.Now(), 0, 0)
		require.NoError(t, err)
	}

	queue, err := storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []SnapshotBackupRequest{{
		SnapshotID: "snapshot_backed",
		BackupID:   "create_backed",
	}}, queue)
}

func TestSnapshotsBackupQueuesNewReadySnapshotsOfAllFoldersWithWildcard(
	t *testing.T,
) {

	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorageWithBackupFolders(t, ctx, db, []string{"*"})

	for _, folderID := range []string{"folder_a", "folder_b"} {
		snapshot := SnapshotMeta{
			ID:       "snapshot_" + folderID,
			FolderID: folderID,
			Disk: &types.Disk{
				ZoneId: "zone",
				DiskId: "disk_" + folderID,
			},
			CreateRequest: &wrappers.UInt64Value{
				Value: 1,
			},
			CreateTaskID: "create_" + folderID,
			CreatingAt:   time.Now(),
			CreatedBy:    "user",
		}

		_, err = storage.CreateSnapshot(ctx, snapshot)
		require.NoError(t, err)

		err = storage.SnapshotCreated(ctx, snapshot.ID, "checkpoint", time.Now(), 0, 0)
		require.NoError(t, err)
	}

	queue, err := storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.ElementsMatch(t, []SnapshotBackupRequest{
		{SnapshotID: "snapshot_folder_a", BackupID: "create_folder_a"},
		{SnapshotID: "snapshot_folder_b", BackupID: "create_folder_b"},
	}, queue)
}

func newSnapshotBackupTestStorage(t *testing.T) (context.Context, Storage) {
	t.Helper()
	ctx, cancel := context.WithCancel(newContext())
	t.Cleanup(cancel)
	db, err := newYDB(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(ctx)) })

	storage := newStorage(t, ctx, db)
	_, err = storage.CreateSnapshot(ctx, SnapshotMeta{
		ID:            "snapshot",
		FolderID:      "folder",
		Disk:          &types.Disk{ZoneId: "zone", DiskId: "disk"},
		CreateRequest: &wrappers.UInt64Value{Value: 1},
		CreateTaskID:  "create",
		CreatingAt:    time.Now(),
	})
	require.NoError(t, err)
	err = storage.SnapshotCreated(ctx, "snapshot", "cp", time.Now(), 0, 0)
	require.NoError(t, err)
	return ctx, storage
}

func TestEnqueueSnapshotBackupIsIdempotent(t *testing.T) {
	ctx, storage := newSnapshotBackupTestStorage(t)

	meta, err := storage.GetSnapshotMeta(ctx, "snapshot")
	require.NoError(t, err)
	require.False(t, meta.BackupCompleted)

	queue, err := storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)

	for _, backupID := range []string{"first", "second"} {
		err = storage.EnqueueSnapshotBackup(ctx, "snapshot", backupID)
		require.NoError(t, err)
	}
	queue, err = storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []SnapshotBackupRequest{{
		SnapshotID: "snapshot", BackupID: "first",
	}}, queue)

	meta, err = storage.GetSnapshotMeta(ctx, "snapshot")
	require.NoError(t, err)
	require.False(t, meta.BackupCompleted)

	err = storage.SnapshotBackupCompleted(ctx, "snapshot")
	require.NoError(t, err)
	err = storage.RemoveSnapshotFromBackupQueue(ctx, "snapshot", "first")
	require.NoError(t, err)
	meta, err = storage.GetSnapshotMeta(ctx, "snapshot")
	require.NoError(t, err)
	require.True(t, meta.BackupCompleted)

	err = storage.RemoveSnapshotFromBackupQueue(ctx, "snapshot", "first")
	require.NoError(t, err)
	err = storage.EnqueueSnapshotBackup(ctx, "snapshot", "third")
	require.NoError(t, err)
	queue, err = storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
	meta, err = storage.GetSnapshotMeta(ctx, "snapshot")
	require.NoError(t, err)
	require.True(t, meta.BackupCompleted)
}

func TestEnqueueSnapshotBackupAfterCancellation(t *testing.T) {
	ctx, storage := newSnapshotBackupTestStorage(t)

	err := storage.EnqueueSnapshotBackup(ctx, "snapshot", "first")
	require.NoError(t, err)
	err = storage.RemoveSnapshotFromBackupQueue(ctx, "snapshot", "first")
	require.NoError(t, err)
	meta, err := storage.GetSnapshotMeta(ctx, "snapshot")
	require.NoError(t, err)
	require.False(t, meta.BackupCompleted)

	err = storage.EnqueueSnapshotBackup(ctx, "snapshot", "second")
	require.NoError(t, err)

	// A stale attempt cannot remove the row of the next one.
	for i := 0; i != 2; i++ {
		err = storage.RemoveSnapshotFromBackupQueue(ctx, "snapshot", "first")
		require.NoError(t, err)
	}
	queue, err := storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []SnapshotBackupRequest{{
		SnapshotID: "snapshot", BackupID: "second",
	}}, queue)
	meta, err = storage.GetSnapshotMeta(ctx, "snapshot")
	require.NoError(t, err)
	require.False(t, meta.BackupCompleted)

	err = storage.RemoveSnapshotFromBackupQueue(ctx, "snapshot", "second")
	require.NoError(t, err)
	err = storage.EnqueueSnapshotBackup(ctx, "snapshot", "third")
	require.NoError(t, err)
	queue, err = storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []SnapshotBackupRequest{{
		SnapshotID: "snapshot", BackupID: "third",
	}}, queue)
}

func TestEnqueueSnapshotBackupRejectsUnavailableSnapshot(t *testing.T) {
	ctx, storage := newSnapshotBackupTestStorage(t)

	_, err := storage.CreateSnapshot(ctx, SnapshotMeta{
		ID:            "creating",
		Disk:          &types.Disk{ZoneId: "zone", DiskId: "disk"},
		CreateRequest: &wrappers.UInt64Value{Value: 1},
		CreateTaskID:  "create2",
		CreatingAt:    time.Now(),
	})
	require.NoError(t, err)
	for _, id := range []string{"", "missing", "creating"} {
		err = storage.EnqueueSnapshotBackup(ctx, id, "backup")
		require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	}
	err = storage.EnqueueSnapshotBackup(ctx, "snapshot", "")
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	_, err = storage.DeleteSnapshot(ctx, "snapshot", "delete", time.Now())
	require.NoError(t, err)
	err = storage.EnqueueSnapshotBackup(ctx, "snapshot", "backup")
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	err = storage.SnapshotDeleted(ctx, "snapshot", time.Now())
	require.NoError(t, err)
	err = storage.EnqueueSnapshotBackup(ctx, "snapshot", "backup")
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	queue, err := storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}

func TestEnqueueSnapshotBackupConcurrentRequests(t *testing.T) {
	ctx, storage := newSnapshotBackupTestStorage(t)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, id := range []string{"first", "second"} {
		backupID := id
		go func() {
			<-start
			results <- storage.EnqueueSnapshotBackup(ctx, "snapshot", backupID)
		}()
	}
	close(start)
	first, second := <-results, <-results
	require.NoError(t, first)
	require.NoError(t, second)

	queue, err := storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Len(t, queue, 1)
	require.Equal(t, "snapshot", queue[0].SnapshotID)
	require.Contains(t, []string{"first", "second"}, queue[0].BackupID)
}

func TestEnqueueSnapshotBackupRacesWithDeletion(t *testing.T) {
	ctx, storage := newSnapshotBackupTestStorage(t)
	start := make(chan struct{})
	enqueued := make(chan error, 1)
	deleted := make(chan error, 1)
	go func() {
		<-start
		enqueued <- storage.EnqueueSnapshotBackup(ctx, "snapshot", "backup")
	}()
	go func() {
		<-start
		_, err := storage.DeleteSnapshot(ctx, "snapshot", "delete", time.Now())
		deleted <- err
	}()
	close(start)
	enqueueErr, deleteErr := <-enqueued, <-deleted
	require.NoError(t, deleteErr)
	if enqueueErr != nil {
		require.True(
			t,
			errors.Is(enqueueErr, errors.NewEmptyNonRetriableError()),
		)
	}

	queue, err := storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}

func TestSnapshotsDeletionStopsBackup(t *testing.T) {
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

	_, err = storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)

	err = storage.SnapshotCreated(ctx, snapshot.ID, "checkpoint", time.Now(), 0, 0)
	require.NoError(t, err)

	require.NoError(t, storage.EnqueueSnapshotBackup(ctx, snapshot.ID, "manual"))
	_, err = storage.DeleteSnapshot(ctx, snapshot.ID, "delete", time.Now())
	require.NoError(t, err)

	ids, err := storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)

	snapshotBackupIDsForDeletion, err :=
		storage.GetSnapshotBackupDeleteQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(
		t,
		[]SnapshotBackupID{{
			DiskID:     "disk",
			SnapshotID: "snapshot",
		}},
		snapshotBackupIDsForDeletion,
	)

	_, err = storage.DeleteSnapshot(ctx, snapshot.ID, "delete", time.Now())
	require.NoError(t, err)

	snapshotBackupIDsForDeletion, err =
		storage.GetSnapshotBackupDeleteQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(
		t,
		[]SnapshotBackupID{{
			DiskID:     "disk",
			SnapshotID: "snapshot",
		}},
		snapshotBackupIDsForDeletion,
	)

	err = storage.SnapshotBackupDeletionsCompleted(ctx, []string{"snapshot"})
	require.NoError(t, err)

	snapshotBackupIDsForDeletion, err =
		storage.GetSnapshotBackupDeleteQueue(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, snapshotBackupIDsForDeletion)
}

func TestSnapshotBackupCompletedMarksOnlyReadySnapshot(t *testing.T) {
	ctx, storage := newSnapshotBackupTestStorage(t)
	_, err := storage.DeleteSnapshot(ctx, "snapshot", "delete", time.Now())
	require.NoError(t, err)

	require.NoError(t, storage.SnapshotBackupCompleted(ctx, "snapshot"))
	meta, err := storage.GetSnapshotMeta(ctx, "snapshot")
	require.NoError(t, err)
	require.False(t, meta.BackupCompleted)
}

func createReadySnapshotForBackup(
	t *testing.T,
	ctx context.Context,
	storage Storage,
	snapshotID string,
) {

	_, err := storage.CreateSnapshot(ctx, SnapshotMeta{
		ID:            snapshotID,
		FolderID:      "folder",
		Disk:          &types.Disk{ZoneId: "zone", DiskId: "disk"},
		CreateRequest: &wrappers.UInt64Value{Value: 1},
		CreateTaskID:  "create_" + snapshotID,
		CreatingAt:    time.Now(),
	})
	require.NoError(t, err)

	err = storage.SnapshotCreated(ctx, snapshotID, "cp", time.Now(), 0, 0)
	require.NoError(t, err)
}

func TestSnapshotsBackupQueueTracksScheduledAttempts(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	for _, snapshotID := range []string{"snap0", "snap1", "snap2"} {
		createReadySnapshotForBackup(t, ctx, storage, snapshotID)
		err = storage.EnqueueSnapshotBackup(ctx, snapshotID, "attempt")
		require.NoError(t, err)
	}

	stats, err := storage.GetSnapshotBackupQueueStats(ctx)
	require.NoError(t, err)
	require.Equal(t, SnapshotBackupQueueStats{Queued: 3, Scheduled: 0}, stats)

	enqueuedAt, err := storage.SnapshotBackupScheduled(
		ctx,
		"snap0",
		"attempt",
		"task0",
	)
	require.NoError(t, err)
	require.False(t, enqueuedAt.IsZero())
	require.True(t, enqueuedAt.Before(time.Now()))

	// A scheduled attempt is not listed again; the rest come oldest first.
	queue, err := storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []SnapshotBackupRequest{
		{SnapshotID: "snap1", BackupID: "attempt"},
		{SnapshotID: "snap2", BackupID: "attempt"},
	}, queue)

	scheduled, err := storage.ListScheduledSnapshotBackups(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []ScheduledSnapshotBackup{
		{SnapshotID: "snap0", BackupID: "attempt", TaskID: "task0"},
	}, scheduled)

	stats, err = storage.GetSnapshotBackupQueueStats(ctx)
	require.NoError(t, err)
	require.Equal(t, SnapshotBackupQueueStats{Queued: 2, Scheduled: 1}, stats)

	// Another attempt of a queued snapshot is not in the queue: nothing is
	// recorded.
	enqueuedAt, err = storage.SnapshotBackupScheduled(
		ctx,
		"snap1",
		"other",
		"task1",
	)
	require.NoError(t, err)
	require.True(t, enqueuedAt.IsZero())

	// The task removes its row when it ends, which frees the slot.
	err = storage.RemoveSnapshotFromBackupQueue(ctx, "snap0", "attempt")
	require.NoError(t, err)

	stats, err = storage.GetSnapshotBackupQueueStats(ctx)
	require.NoError(t, err)
	require.Equal(t, SnapshotBackupQueueStats{Queued: 2, Scheduled: 0}, stats)

	queue, err = storage.ListSnapshotsToBackup(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, []SnapshotBackupRequest{
		{SnapshotID: "snap1", BackupID: "attempt"},
	}, queue)
}

////////////////////////////////////////////////////////////////////////////////

func requireSnapshotChecksum(
	t *testing.T,
	ctx context.Context,
	storage Storage,
	snapshotID string,
	checksum uint32,
) {

	meta, err := storage.GetSnapshotMeta(ctx, snapshotID)
	require.NoError(t, err, snapshotID)
	require.NotNil(t, meta, snapshotID)
	require.True(t, meta.ChecksumCalculated, snapshotID)
	require.Equal(t, checksum, meta.Checksum, snapshotID)
}

func requireNoSnapshotChecksum(
	t *testing.T,
	ctx context.Context,
	storage Storage,
	snapshotID string,
) {

	meta, err := storage.GetSnapshotMeta(ctx, snapshotID)
	require.NoError(t, err, snapshotID)
	require.NotNil(t, meta, snapshotID)
	require.False(t, meta.ChecksumCalculated, snapshotID)
	require.Zero(t, meta.Checksum, snapshotID)
}

func TestSnapshotChecksumCalculated(t *testing.T) {
	ctx, storage := newSnapshotBackupTestStorage(t)

	requireNoSnapshotChecksum(t, ctx, storage, "snapshot")

	// Zero is a valid checksum and differs from a checksum that is not set.
	// A repeated value checks idempotency, another value replaces the stored
	// one.
	checksums := []uint32{0, 0, 0xDEADBEEF, 0xDEADBEEF, 0xFFFFFFFF, 1, 0}
	for _, checksum := range checksums {
		err := storage.SnapshotChecksumCalculated(
			ctx,
			"snapshot",
			"create",
			checksum,
		)
		require.NoError(t, err)

		requireSnapshotChecksum(t, ctx, storage, "snapshot", checksum)
	}
}

// The checksum is stored by rewriting the whole row, which should keep the
// other fields.
func TestSnapshotChecksumCalculatedKeepsOtherFields(t *testing.T) {
	ctx, storage := newSnapshotBackupTestStorage(t)

	snapshot := SnapshotMeta{
		BlockSize:     4096,
		ID:            "other",
		FolderID:      "folder",
		Disk:          &types.Disk{ZoneId: "zone", DiskId: "disk"},
		CreateRequest: &wrappers.UInt64Value{Value: 1},
		CreateTaskID:  "create_other",
		CreatingAt:    time.Now(),
		CreatedBy:     "user",
		Encryption: &types.EncryptionDesc{
			Mode: types.EncryptionMode_ENCRYPTION_AES_XTS,
			Key: &types.EncryptionDesc_KeyHash{
				KeyHash: []byte("key hash"),
			},
		},
	}

	_, err := storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)

	expected := snapshot
	expected.CreateRequest = nil
	expected.CheckpointID = "checkpoint"
	expected.Size = 2 * 1024 * 1024
	expected.StorageSize = 3 * 1024 * 1024
	expected.Ready = true
	expected.BackupCompleted = true

	err = storage.SnapshotCreated(
		ctx,
		snapshot.ID,
		expected.CheckpointID,
		time.Now(),
		expected.Size,
		expected.StorageSize,
	)
	require.NoError(t, err)

	err = storage.SnapshotBackupCompleted(ctx, snapshot.ID)
	require.NoError(t, err)

	checkSnapshot := func() {
		actual, err := storage.GetSnapshotMeta(ctx, snapshot.ID)
		require.NoError(t, err)
		require.NotNil(t, actual)
		requireSnapshotsAreEqual(t, expected, *actual)
		require.Equal(t, expected.BlockSize, actual.BlockSize)
		require.True(t, proto.Equal(expected.Encryption, actual.Encryption))
		require.Equal(t, expected.BackupCompleted, actual.BackupCompleted)
	}

	checkSnapshot()

	err = storage.SnapshotChecksumCalculated(
		ctx,
		snapshot.ID,
		snapshot.CreateTaskID,
		42,
	)
	require.NoError(t, err)

	expected.Checksum = 42
	expected.ChecksumCalculated = true
	checkSnapshot()

	// The create request is kept too: creation is still idempotent.
	created, err := storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)
	requireSnapshotsAreEqual(t, expected, created)
}

func TestSnapshotChecksumCalculatedRejectsUnavailableSnapshot(t *testing.T) {
	ctx, storage := newSnapshotBackupTestStorage(t)

	_, err := storage.CreateSnapshot(ctx, SnapshotMeta{
		ID:            "creating",
		FolderID:      "folder",
		Disk:          &types.Disk{ZoneId: "zone", DiskId: "disk"},
		CreateRequest: &wrappers.UInt64Value{Value: 1},
		CreateTaskID:  "create_creating",
		CreatingAt:    time.Now(),
	})
	require.NoError(t, err)

	// Its checksum was stored before the deletion.
	createReadySnapshotForBackup(t, ctx, storage, "deleting")
	err = storage.SnapshotChecksumCalculated(
		ctx,
		"deleting",
		"create_deleting",
		7,
	)
	require.NoError(t, err)
	_, err = storage.DeleteSnapshot(ctx, "deleting", "delete", time.Now())
	require.NoError(t, err)

	createReadySnapshotForBackup(t, ctx, storage, "deleted")
	_, err = storage.DeleteSnapshot(ctx, "deleted", "delete", time.Now())
	require.NoError(t, err)
	err = storage.SnapshotDeleted(ctx, "deleted", time.Now())
	require.NoError(t, err)

	// A ready image is not a snapshot.
	createReadyImage(t, ctx, storage, "image", "folder")

	testCases := []struct {
		snapshotID string
		checksum   uint32
		message    string
	}{
		{"missing", 42, "snapshot with id missing is not found"},
		{"image", 42, "snapshot with id image is not found"},
		{"creating", 42, "status creating can't be set"},
		{"deleting", 42, "status deleting can't be set"},
		// The stored value is not accepted either.
		{"deleting", 7, "status deleting can't be set"},
		{"deleted", 42, "status deleted can't be set"},
	}

	for _, testCase := range testCases {
		err = storage.SnapshotChecksumCalculated(
			ctx,
			testCase.snapshotID,
			"create_"+testCase.snapshotID,
			testCase.checksum,
		)
		require.Error(t, err, testCase.snapshotID)
		require.True(
			t,
			errors.Is(err, errors.NewEmptyNonRetriableError()),
			testCase.snapshotID,
		)
		require.True(t, errors.IsSilent(err), testCase.snapshotID)
		require.ErrorContains(t, err, testCase.message)
	}

	// Nothing is written.
	requireNoSnapshotChecksum(t, ctx, storage, "creating")
	requireSnapshotChecksum(t, ctx, storage, "deleting", 7)
	requireNoSnapshotChecksum(t, ctx, storage, "deleted")
	requireNoImageChecksum(t, ctx, storage, "image")

	for _, snapshotID := range []string{"missing", "image"} {
		meta, err := storage.GetSnapshotMeta(ctx, snapshotID)
		require.NoError(t, err, snapshotID)
		require.Nil(t, meta, snapshotID)
	}
}

// A snapshot that is deleted and created again with the same id is another
// snapshot: it is created by another task.
func TestSnapshotChecksumCalculatedRejectsSnapshotCreatedByAnotherTask(
	t *testing.T,
) {

	ctx, storage := newSnapshotBackupTestStorage(t)

	err := storage.SnapshotChecksumCalculated(ctx, "snapshot", "create", 7)
	require.NoError(t, err)

	err = storage.SnapshotChecksumCalculated(ctx, "snapshot", "another", 42)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.False(t, errors.IsSilent(err))
	require.ErrorContains(
		t,
		err,
		"it is created by task create, not by task another",
	)

	// Nothing is written.
	requireSnapshotChecksum(t, ctx, storage, "snapshot", 7)
}

// Guards the row mapping: a column that is written but not scanned (or the
// other way round) loses the checksum on the next whole-row rewrite.
func TestSnapshotChecksumSurvivesOtherUpdates(t *testing.T) {
	ctx, storage := newSnapshotBackupTestStorage(t)

	err := storage.SnapshotChecksumCalculated(ctx, "snapshot", "create", 42)
	require.NoError(t, err)
	requireSnapshotChecksum(t, ctx, storage, "snapshot", 42)

	// Updates a single column.
	err = storage.SnapshotBackupCompleted(ctx, "snapshot")
	require.NoError(t, err)
	requireSnapshotChecksum(t, ctx, storage, "snapshot", 42)

	// Rewrites the whole row.
	meta, err := storage.DeleteSnapshot(ctx, "snapshot", "delete", time.Now())
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.BackupCompleted)
	require.True(t, meta.ChecksumCalculated)
	require.EqualValues(t, 42, meta.Checksum)
	requireSnapshotChecksum(t, ctx, storage, "snapshot", 42)

	err = storage.SnapshotDeleted(ctx, "snapshot", time.Now())
	require.NoError(t, err)
	requireSnapshotChecksum(t, ctx, storage, "snapshot", 42)
}

func TestSnapshotChecksumCalculatedRacesWithDeletion(t *testing.T) {
	ctx, storage := newSnapshotBackupTestStorage(t)

	for i := 0; i < 8; i++ {
		snapshotID := fmt.Sprintf("snapshot_%v", i)
		createReadySnapshotForBackup(t, ctx, storage, snapshotID)

		// The deletion takes longer than storing the checksum, so the
		// checksum is delayed to meet the deletion at its different stages.
		delay := time.Duration(i) * time.Millisecond

		start := make(chan struct{})
		set := make(chan error, 1)
		deleted := make(chan error, 1)
		var deleting *SnapshotMeta
		go func() {
			<-start
			time.Sleep(delay)
			set <- storage.SnapshotChecksumCalculated(
				ctx,
				snapshotID,
				"create_"+snapshotID,
				42,
			)
		}()
		go func() {
			<-start
			var err error
			deleting, err = storage.DeleteSnapshot(
				ctx,
				snapshotID,
				"delete",
				time.Now(),
			)
			deleted <- err
		}()
		close(start)

		setErr, deleteErr := <-set, <-deleted
		require.NoError(t, deleteErr)
		require.NotNil(t, deleting)
		if setErr != nil {
			require.True(
				t,
				errors.Is(setErr, errors.NewEmptyNonRetriableError()),
			)
			require.ErrorContains(t, setErr, "status deleting can't be set")
		}

		// The checksum is stored only if it was set before the deletion, and
		// then the deletion has seen it.
		require.Equal(t, setErr == nil, deleting.ChecksumCalculated)

		meta, err := storage.GetSnapshotMeta(ctx, snapshotID)
		require.NoError(t, err)
		require.NotNil(t, meta)
		require.False(t, meta.Ready)

		if setErr == nil {
			requireSnapshotChecksum(t, ctx, storage, snapshotID, 42)
		} else {
			requireNoSnapshotChecksum(t, ctx, storage, snapshotID)
		}
	}
}

// Storing the checksum rewrites the whole row and should not lose a column
// that is updated at the same time.
func TestSnapshotChecksumCalculatedRacesWithBackupCompletion(t *testing.T) {
	ctx, storage := newSnapshotBackupTestStorage(t)

	// Several attempts give more interleavings of the two transactions.
	for i := 0; i < 8; i++ {
		snapshotID := fmt.Sprintf("snapshot_%v", i)
		createReadySnapshotForBackup(t, ctx, storage, snapshotID)

		start := make(chan struct{})
		set := make(chan error, 1)
		completed := make(chan error, 1)
		go func() {
			<-start
			set <- storage.SnapshotChecksumCalculated(
				ctx,
				snapshotID,
				"create_"+snapshotID,
				42,
			)
		}()
		go func() {
			<-start
			completed <- storage.SnapshotBackupCompleted(ctx, snapshotID)
		}()
		close(start)

		setErr, completeErr := <-set, <-completed
		require.NoError(t, setErr)
		require.NoError(t, completeErr)

		meta, err := storage.GetSnapshotMeta(ctx, snapshotID)
		require.NoError(t, err)
		require.NotNil(t, meta)
		require.True(t, meta.Ready)
		require.True(t, meta.BackupCompleted)
		require.True(t, meta.ChecksumCalculated)
		require.EqualValues(t, 42, meta.Checksum)
	}
}

// A snapshot created before the checksum columns were added has no checksum.
func TestSnapshotChecksumOfSnapshotCreatedBeforeMigration(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	// Should be in sync with newStorage.
	folder := fmt.Sprintf("%v/snapshots", t.Name())

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		"snapshots",
		withoutChecksumColumns(t, snapshotStateTableDescription()),
		false, // dropUnusedColumns
	)
	require.NoError(t, err)

	_, err = db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;
		declare $folder_id as Utf8;
		declare $size as Uint64;
		declare $status as Int64;

		upsert into snapshots (id, folder_id, size, status)
		values ($id, $folder_id, $size, $status)
	`, db.AbsolutePath(folder)),
		persistence.ValueParam("$id", persistence.UTF8Value("snapshot")),
		persistence.ValueParam("$folder_id", persistence.UTF8Value("folder")),
		persistence.ValueParam("$size", persistence.Uint64Value(42)),
		persistence.ValueParam(
			"$status",
			persistence.Int64Value(int64(snapshotStatusReady)),
		),
	)
	require.NoError(t, err)

	// The table has no checksum columns yet, so they can't be read.
	_, err = readStoredChecksum(ctx, db, folder, "snapshots", "snapshot")
	require.Error(t, err)

	// Adds the checksum columns to the existing table.
	storage := newStorage(t, ctx, db)

	// The existing row holds NULL in both columns...
	stored, err := readStoredChecksum(ctx, db, folder, "snapshots", "snapshot")
	require.NoError(t, err)
	require.Equal(
		t,
		storedChecksum{checksumIsNull: true, calculatedIsNull: true},
		stored,
	)

	// ... which is read as a checksum that is not calculated.
	checkSnapshot := func(checksumCalculated bool) {
		meta, err := storage.GetSnapshotMeta(ctx, "snapshot")
		require.NoError(t, err)
		require.NotNil(t, meta)
		require.True(t, meta.Ready)
		require.Equal(t, "folder", meta.FolderID)
		require.EqualValues(t, 42, meta.Size)
		require.Equal(t, checksumCalculated, meta.ChecksumCalculated)
		require.Zero(t, meta.Checksum)
	}

	checkSnapshot(false)

	// Zero differs from a checksum that is not calculated.
	err = storage.SnapshotChecksumCalculated(
		ctx,
		"snapshot",
		"", // createTaskID
		0,
	)
	require.NoError(t, err)
	checkSnapshot(true)

	stored, err = readStoredChecksum(ctx, db, folder, "snapshots", "snapshot")
	require.NoError(t, err)
	require.Equal(t, storedChecksum{calculated: true}, stored)

	// The next run of the migration keeps the checksum.
	storage = newStorage(t, ctx, db)
	checkSnapshot(true)
}
