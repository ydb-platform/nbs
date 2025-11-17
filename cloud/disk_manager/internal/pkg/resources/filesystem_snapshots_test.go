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
	require.Equal(t, expected.CheckpointID, actual.CheckpointID)
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
	const checkpointID = "checkpoint"
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
		CheckpointID: checkpointID,
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

	err = storage.SnapshotCreated(
		ctx,
		filesystemSnapshot.ID,
		checkpointID,
		time.Now(),
		0,
		0,
	)
	require.NoError(t, err)

	// Check idempotency
	err = storage.SnapshotCreated(
		ctx,
		filesystemSnapshot.ID,
		checkpointID,
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
	const checkpointID = "checkpoint"
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
		CheckpointID: checkpointID,
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
	}

	created, err := storage.CreateFilesystemSnapshot(ctx, filesystemSnapshot)
	require.NoError(t, err)
	require.Equal(t, filesystemSnapshot.ID, created.ID)

	expected := filesystemSnapshot
	expected.CreateRequest = nil
	expected.DeleteTaskID = "delete"
	actual, err := storage.DeleteFilesystemSnapshot(ctx, filesystemSnapshot.ID, "delete")
	require.NoError(t, err)
	requireFilesystemSnapshotsAreEqual(t, expected, *actual)

	// Check idempotency
	actual, err = storage.DeleteFilesystemSnapshot(ctx, filesystemSnapshot.ID, "delete")
	require.NoError(t, err)
	requireFilesystemSnapshotsAreEqual(t, expected, *actual)

	err = storage.FilesystemSnapshotDeleted(ctx, filesystemSnapshot.ID)
	require.NoError(t, err)

	// Check idempotency
	actual, err = storage.DeleteFilesystemSnapshot(ctx, filesystemSnapshot.ID, "delete")
	require.NoError(t, err)
	requireFilesystemSnapshotsAreEqual(t, expected, *actual)

	_, err = storage.CreateFilesystemSnapshot(ctx, filesystemSnapshot)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonCancellableError())

	err = storage.FilesystemSnapshotCreated(
		ctx,
		filesystemSnapshot.ID,
		checkpointID,
		time.Now(),
		0,
		0,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonCancellableError())
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
		CheckpointID: "checkpoint",
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
		"checkpoint",
		time.Now(),
		0,
		0,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonCancellableError())

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
	const checkpointID = "checkpoint"
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
		CheckpointID: checkpointID,
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
	const checkpointID = "checkpoint"
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
		CheckpointID: checkpointID,
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

	const checkpointID = "checkpoint"
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
		CheckpointID: checkpointID,
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
	}
}
