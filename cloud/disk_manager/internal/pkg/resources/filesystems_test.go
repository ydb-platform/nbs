package resources

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func requireFilesystemsAreEqual(t *testing.T, expected FilesystemMeta, actual FilesystemMeta) {
	// TODO: Get rid of boilerplate.
	require.Equal(t, expected.ID, actual.ID)
	require.Equal(t, expected.ZoneID, actual.ZoneID)
	require.Equal(t, expected.BlocksCount, actual.BlocksCount)
	require.Equal(t, expected.BlockSize, actual.BlockSize)
	require.Equal(t, expected.Kind, actual.Kind)
	require.Equal(t, expected.CloudID, actual.CloudID)
	require.Equal(t, expected.FolderID, actual.FolderID)
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
	require.Equal(t, expected.IsExternal, actual.IsExternal)
	require.Equal(t, expected.ExternalStorageClusterName, actual.ExternalStorageClusterName)
}

////////////////////////////////////////////////////////////////////////////////

func TestFilesystemsCreateFilesystem(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	filesystem := FilesystemMeta{
		ID:          "filesystem",
		ZoneID:      "zone",
		BlocksCount: 1000000,
		BlockSize:   4096,
		Kind:        "ssd",
		CloudID:     "cloud",
		FolderID:    "folder",

		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreateFilesystem(ctx, filesystem)
	require.NoError(t, err)
	require.NotNil(t, actual)

	expected := filesystem
	expected.CreateRequest = nil
	requireFilesystemsAreEqual(t, expected, *actual)

	// Check idempotency.
	actual, err = storage.CreateFilesystem(ctx, filesystem)
	require.NoError(t, err)
	require.NotNil(t, actual)

	filesystem.CreatedAt = time.Now()
	err = storage.FilesystemCreated(ctx, filesystem)
	require.NoError(t, err)

	// Check idempotency.
	err = storage.FilesystemCreated(ctx, filesystem)
	require.NoError(t, err)

	// Check idempotency.
	actual, err = storage.CreateFilesystem(ctx, filesystem)
	require.NoError(t, err)
	require.NotNil(t, actual)

	expected = filesystem
	expected.CreateRequest = nil
	requireFilesystemsAreEqual(t, expected, *actual)

	filesystem.CreateTaskID = "other"
	actual, err = storage.CreateFilesystem(ctx, filesystem)
	require.NoError(t, err)
	require.Nil(t, actual)
}

func TestFilesystemsDeleteFilesystem(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	filesystem := FilesystemMeta{
		ID: "filesystem",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreateFilesystem(ctx, filesystem)
	require.NoError(t, err)
	require.NotNil(t, actual)

	expected := filesystem
	expected.CreateRequest = nil
	expected.DeleteTaskID = "delete"

	actual, err = storage.DeleteFilesystem(ctx, filesystem.ID, "delete", time.Now())
	require.NoError(t, err)
	requireFilesystemsAreEqual(t, expected, *actual)

	filesystem.CreatedAt = time.Now()
	err = storage.FilesystemCreated(ctx, filesystem)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	// Check idempotency.
	actual, err = storage.DeleteFilesystem(ctx, filesystem.ID, "delete", time.Now())
	require.NoError(t, err)
	requireFilesystemsAreEqual(t, expected, *actual)

	_, err = storage.CreateFilesystem(ctx, filesystem)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.FilesystemDeleted(ctx, filesystem.ID, time.Now())
	require.NoError(t, err)

	// Check idempotency.
	actual, err = storage.DeleteFilesystem(ctx, filesystem.ID, "delete", time.Now())
	require.NoError(t, err)
	requireFilesystemsAreEqual(t, expected, *actual)

	_, err = storage.CreateFilesystem(ctx, filesystem)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.FilesystemCreated(ctx, filesystem)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
}

func TestFilesystemsDeleteNonexistentFilesystem(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	filesystem := FilesystemMeta{
		ID:           "filesystem",
		DeleteTaskID: "delete",
	}

	err = storage.FilesystemDeleted(ctx, filesystem.ID, time.Now())
	require.NoError(t, err)

	created := filesystem
	created.CreatedAt = time.Now()
	err = storage.FilesystemCreated(ctx, created)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	deletingAt := time.Now()
	actual, err := storage.DeleteFilesystem(ctx, filesystem.ID, "delete", deletingAt)
	require.NoError(t, err)
	requireFilesystemsAreEqual(t, filesystem, *actual)

	// Check idempotency.
	deletingAt = deletingAt.Add(time.Second)
	actual, err = storage.DeleteFilesystem(ctx, filesystem.ID, "delete", deletingAt)
	require.NoError(t, err)
	requireFilesystemsAreEqual(t, filesystem, *actual)

	_, err = storage.CreateFilesystem(ctx, filesystem)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.FilesystemDeleted(ctx, filesystem.ID, time.Now())
	require.NoError(t, err)
}

func TestFilesystemsClearDeletedFilesystems(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	deletedAt := time.Now()
	deletedBefore := deletedAt.Add(-time.Microsecond)

	err = storage.ClearDeletedFilesystems(ctx, deletedBefore, 10)
	require.NoError(t, err)

	filesystem := FilesystemMeta{
		ID: "filesystem",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreateFilesystem(ctx, filesystem)
	require.NoError(t, err)
	require.NotNil(t, actual)

	_, err = storage.DeleteFilesystem(ctx, filesystem.ID, "delete", deletedAt)
	require.NoError(t, err)

	err = storage.FilesystemDeleted(ctx, filesystem.ID, deletedAt)
	require.NoError(t, err)

	err = storage.ClearDeletedFilesystems(ctx, deletedBefore, 10)
	require.NoError(t, err)

	_, err = storage.CreateFilesystem(ctx, filesystem)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	deletedBefore = deletedAt.Add(time.Microsecond)
	err = storage.ClearDeletedFilesystems(ctx, deletedBefore, 10)
	require.NoError(t, err)

	actual, err = storage.CreateFilesystem(ctx, filesystem)
	require.NoError(t, err)
	require.NotNil(t, actual)
}

func TestFilesystemsGetFilesystem(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	filesystemID := t.Name()

	d, err := storage.GetFilesystemMeta(ctx, filesystemID)
	require.NoError(t, err)
	require.Nil(t, d)

	filesystem := FilesystemMeta{
		ID:          filesystemID,
		ZoneID:      "zone",
		BlocksCount: 1000000,
		BlockSize:   4096,
		Kind:        "ssd",
		CloudID:     "cloud",
		FolderID:    "folder",

		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreateFilesystem(ctx, filesystem)
	require.NoError(t, err)
	require.NotNil(t, actual)

	filesystem.CreateRequest = nil

	d, err = storage.GetFilesystemMeta(ctx, filesystemID)
	require.NoError(t, err)
	require.NotNil(t, d)
	requireFilesystemsAreEqual(t, filesystem, *d)
}

func TestFilesystemsSetExternalStorageClusterName(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	filesystemID := t.Name()

	filesystem := FilesystemMeta{
		ID:          filesystemID,
		ZoneID:      "zone",
		BlocksCount: 1000000,
		BlockSize:   4096,
		Kind:        "ssd",
		CloudID:     "cloud",
		FolderID:    "folder",

		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreateFilesystem(ctx, filesystem)
	require.NoError(t, err)
	require.NotNil(t, actual)

	externalStorageClusterName := "cluster-1"
	filesystem.ExternalStorageClusterName = externalStorageClusterName
	err = storage.SetExternalFilesystemStorageClusterName(
		ctx,
		filesystemID,
		externalStorageClusterName,
	)
	require.NoError(t, err)

	filesystem.CreateRequest = nil

	actual, err = storage.GetFilesystemMeta(ctx, filesystemID)
	require.NoError(t, err)
	require.NotNil(t, actual)
	requireFilesystemsAreEqual(t, filesystem, *actual)
}
