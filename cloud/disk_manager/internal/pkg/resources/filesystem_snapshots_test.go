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

func requireFilesystemSnapshotsAreEqual(
	t *testing.T,
	expected FilesystemSnapshotMeta,
	actual FilesystemSnapshotMeta,
) {
	require.Equal(t, expected.ID, actual.ID)
	require.Equal(t, expected.FolderID, actual.FolderID)
	require.True(t, proto.Equal(expected.Filesystem, actual.Filesystem))
	require.True(t, proto.Equal(expected.CreateRequest, actual.CreateRequest))
	require.Equal(t, expected.CreateTaskID, actual.CreateTaskID)
	if !expected.CreatingAt.IsZero() {
		require.WithinDuration(t, expected.CreatingAt, actual.CreatingAt, time.Microsecond)
	}
	require.Equal(t, expected.DeleteTaskID, actual.DeleteTaskID)
	require.Equal(t, expected.Size, actual.Size)
	require.Equal(t, expected.StorageSize, actual.StorageSize)
	require.Equal(t, expected.Ready, actual.Ready)
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateFilesystemSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)
	filesystemSnapshot := FilesystemSnapshotMeta{
		ID:       "fs-snapshot",
		FolderID: "folder",
		Filesystem: &types.Filesystem{
			FilesystemId: "fs",
			ZoneId:       "zone",
		},
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
	}

	created, err := storage.CreateFilesystemSnapshot(ctx, filesystemSnapshot)
	require.NoError(t, err)
	require.Equal(t, filesystemSnapshot.ID, created.ID)

	// Check idempotency
	created, err = storage.CreateFilesystemSnapshot(ctx, filesystemSnapshot)
	require.NoError(t, err)
	require.Equal(t, filesystemSnapshot.ID, created.ID)

	err = storage.FilesystemSnapshotCreated(
		ctx,
		filesystemSnapshot.ID,
		time.Now(),
		0,
		0,
	)
	require.NoError(t, err)

	// Check idempotency
	err = storage.FilesystemSnapshotCreated(
		ctx,
		filesystemSnapshot.ID,
		time.Now(),
		0,
		0,
	)
	require.NoError(t, err)

	// Check idempotency
	created, err = storage.CreateFilesystemSnapshot(ctx, filesystemSnapshot)
	require.NoError(t, err)
	require.Equal(t, filesystemSnapshot.ID, created.ID)

	filesystemSnapshot.CreateTaskID = "other"
	_, err = storage.CreateFilesystemSnapshot(ctx, filesystemSnapshot)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonCancellableError())
}

////////////////////////////////////////////////////////////////////////////////

func TestDeleteFilesystemSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)
	filesystemSnapshot := FilesystemSnapshotMeta{
		ID:       "fs-snapshot",
		FolderID: "folder",
		Filesystem: &types.Filesystem{
			FilesystemId: "fs",
			ZoneId:       "zone",
		},
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
	}

	created, err := storage.CreateFilesystemSnapshot(ctx, filesystemSnapshot)
	require.NoError(t, err)
	require.Equal(t, filesystemSnapshot.ID, created.ID)

	expected := filesystemSnapshot
	expected.CreateRequest = nil
	expected.DeleteTaskID = "delete"
	actual, err := storage.DeleteFilesystemSnapshot(
		ctx,
		filesystemSnapshot.ID,
		"delete",
		time.Now(),
	)
	require.NoError(t, err)
	requireFilesystemSnapshotsAreEqual(t, expected, *actual)

	// Check idempotency
	actual, err = storage.DeleteFilesystemSnapshot(
		ctx,
		filesystemSnapshot.ID,
		"delete",
		time.Now(),
	)
	require.NoError(t, err)
	requireFilesystemSnapshotsAreEqual(t, expected, *actual)

	err = storage.FilesystemSnapshotDeleted(
		ctx,
		filesystemSnapshot.ID,
		time.Now(),
	)
	require.NoError(t, err)

	// Check idempotency
	actual, err = storage.DeleteFilesystemSnapshot(
		ctx,
		filesystemSnapshot.ID,
		"delete",
		time.Now(),
	)
	require.NoError(t, err)
	requireFilesystemSnapshotsAreEqual(t, expected, *actual)

	_, err = storage.CreateFilesystemSnapshot(ctx, filesystemSnapshot)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())

	err = storage.FilesystemSnapshotCreated(
		ctx,
		filesystemSnapshot.ID,
		time.Now(),
		0,
		0,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())
}

////////////////////////////////////////////////////////////////////////////////

func TestDeleteNonexistingFilesystemSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)
	filesystemSnapshot := FilesystemSnapshotMeta{
		ID: "fs-snapshot",
		Filesystem: &types.Filesystem{
			FilesystemId: "fs",
			ZoneId:       "zone",
		},
	}

	err = storage.FilesystemSnapshotDeleted(
		ctx,
		filesystemSnapshot.ID,
		time.Now(),
	)
	require.NoError(t, err)

	_, err = storage.DeleteFilesystemSnapshot(
		ctx,
		filesystemSnapshot.ID,
		"delete",
		time.Now(),
	)
	require.NoError(t, err)

	err = storage.FilesystemSnapshotCreated(
		ctx,
		filesystemSnapshot.ID,
		time.Now(),
		0,
		0,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())

	err = storage.FilesystemSnapshotDeleted(
		ctx,
		filesystemSnapshot.ID,
		time.Now(),
	)
	require.NoError(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateFilesystemSnapshotShouldFailDifferentRequestSameID(
	t *testing.T,
) {

	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)
	filesystemSnapshot := FilesystemSnapshotMeta{
		ID:       "fs-snapshot",
		FolderID: "folder",
		Filesystem: &types.Filesystem{
			FilesystemId: "fs",
			ZoneId:       "zone",
		},
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
	}

	created, err := storage.CreateFilesystemSnapshot(ctx, filesystemSnapshot)
	require.NoError(t, err)
	require.Equal(t, filesystemSnapshot.ID, created.ID)

	filesystemSnapshot.CreateRequest = &wrappers.UInt64Value{
		Value: 2,
	}
	_, err = storage.CreateFilesystemSnapshot(ctx, filesystemSnapshot)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonCancellableError())
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateFilesystemSnapshotShouldFailDifferentTaskID(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)
	filesystemSnapshot := FilesystemSnapshotMeta{
		ID:       "fs-snapshot",
		FolderID: "folder",
		Filesystem: &types.Filesystem{
			FilesystemId: "fs",
			ZoneId:       "zone",
		},
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
	}

	created, err := storage.CreateFilesystemSnapshot(ctx, filesystemSnapshot)
	require.NoError(t, err)
	require.Equal(t, filesystemSnapshot.ID, created.ID)

	filesystemSnapshot.CreateTaskID = "different-create-task"
	_, err = storage.CreateFilesystemSnapshot(ctx, filesystemSnapshot)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonCancellableError())
}

////////////////////////////////////////////////////////////////////////////////

func TestClearDeletedFilesystemSnapshots(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ClearDeletedFilesystemSnapshots(
		ctx,
		time.Now().Add(-time.Hour),
		10,
	)
	require.NoError(t, err)

	filesystemSnapshotTemplate := FilesystemSnapshotMeta{
		FolderID: "folder",
		Filesystem: &types.Filesystem{
			FilesystemId: "fs",
			ZoneId:       "zone",
		},
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
	}
	creatingSnapshot := filesystemSnapshotTemplate
	creatingSnapshot.ID = "creating-snapshot"
	_, err = storage.CreateFilesystemSnapshot(ctx, creatingSnapshot)
	require.NoError(t, err)

	createdSnapshot := filesystemSnapshotTemplate
	createdSnapshot.ID = "created-snapshot"
	_, err = storage.CreateFilesystemSnapshot(ctx, createdSnapshot)
	require.NoError(t, err)
	err = storage.FilesystemSnapshotCreated(
		ctx,
		createdSnapshot.ID,
		time.Now(),
		0,
		0,
	)
	require.NoError(t, err)

	deletingSnapshot := filesystemSnapshotTemplate
	deletingSnapshot.ID = "deleting-snapshot"
	_, err = storage.CreateFilesystemSnapshot(ctx, deletingSnapshot)
	require.NoError(t, err)
	deletingSnapshot.DeleteTaskID = "delete"
	_, err = storage.DeleteFilesystemSnapshot(
		ctx,
		deletingSnapshot.ID,
		"delete",
		time.Now(),
	)
	require.NoError(t, err)

	deletedSnapshot := filesystemSnapshotTemplate
	deletedSnapshot.ID = "deleted-snapshot"
	_, err = storage.CreateFilesystemSnapshot(ctx, deletedSnapshot)
	require.NoError(t, err)
	_, err = storage.DeleteFilesystemSnapshot(
		ctx,
		deletedSnapshot.ID,
		"delete",
		time.Now(),
	)
	require.NoError(t, err)
	err = storage.FilesystemSnapshotDeleted(
		ctx,
		deletedSnapshot.ID,
		time.Now(),
	)
	require.NoError(t, err)

	oldDeletedSnapshot := filesystemSnapshotTemplate
	oldDeletedSnapshot.ID = "old-deleted-snapshot"
	_, err = storage.CreateFilesystemSnapshot(ctx, oldDeletedSnapshot)
	require.NoError(t, err)
	oldDeletedSnapshot.DeleteTaskID = "delete"
	_, err = storage.DeleteFilesystemSnapshot(
		ctx,
		oldDeletedSnapshot.ID,
		"delete",
		time.Now().Add(-2*time.Hour),
	)
	require.NoError(t, err)
	err = storage.FilesystemSnapshotDeleted(
		ctx,
		oldDeletedSnapshot.ID,
		time.Now().Add(-2*time.Hour),
	)
	require.NoError(t, err)
	_, err = storage.CreateFilesystemSnapshot(ctx, oldDeletedSnapshot)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())

	err = storage.ClearDeletedFilesystemSnapshots(
		ctx,
		time.Now().Add(-time.Hour),
		10,
	)
	require.NoError(t, err)

	expectedListedSnapshots := []string{
		creatingSnapshot.ID,
		createdSnapshot.ID,
		deletingSnapshot.ID,
		deletedSnapshot.ID,
	}
	listed, err := storage.ListFilesystemSnapshots(
		ctx,
		creatingSnapshot.FolderID,
		time.Now().Add(time.Hour),
	)
	require.NoError(t, err)
	require.ElementsMatch(t, expectedListedSnapshots, listed)

	created, err := storage.CreateFilesystemSnapshot(ctx, oldDeletedSnapshot)
	require.NoError(t, err)
	require.Equal(t, oldDeletedSnapshot.ID, created.ID)
}
