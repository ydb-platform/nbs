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

func requireImagesAreEqual(t *testing.T, expected ImageMeta, actual ImageMeta) {
	require.Equal(t, expected.ID, actual.ID)
	require.Equal(t, expected.FolderID, actual.FolderID)
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

func TestImagesCreateImage(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	image := ImageMeta{
		ID:        "image",
		FolderID:  "folder",
		SrcDiskID: "disk",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	created, err := storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.NotNil(t, created)

	// Check idempotency.
	created, err = storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.NotNil(t, created)

	err = storage.ImageCreated(ctx, image.ID, time.Now(), 0, 0)
	require.NoError(t, err)

	// Check idempotency.
	err = storage.ImageCreated(ctx, image.ID, time.Now(), 0, 0)
	require.NoError(t, err)

	// Check idempotency.
	created, err = storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.NotNil(t, created)

	require.EqualValues(t, "disk", created.SrcDiskID)

	image.CreateTaskID = "other"
	created, err = storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.Nil(t, created)
}

func TestImagesDeleteImage(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	image := ImageMeta{
		ID:       "image",
		FolderID: "folder",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	created, err := storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.NotNil(t, created)

	expected := image
	expected.CreateRequest = nil
	expected.DeleteTaskID = "delete"

	actual, err := storage.DeleteImage(ctx, image.ID, "delete", time.Now())
	require.NoError(t, err)
	requireImagesAreEqual(t, expected, *actual)

	err = storage.ImageCreated(ctx, image.ID, time.Now(), 0, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	// Check idempotency.
	actual, err = storage.DeleteImage(ctx, image.ID, "delete", time.Now())
	require.NoError(t, err)
	requireImagesAreEqual(t, expected, *actual)

	_, err = storage.CreateImage(ctx, image)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.ImageDeleted(ctx, image.ID, time.Now())
	require.NoError(t, err)

	// Check idempotency.
	actual, err = storage.DeleteImage(ctx, image.ID, "delete", time.Now())
	require.NoError(t, err)
	requireImagesAreEqual(t, expected, *actual)

	_, err = storage.CreateImage(ctx, image)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.ImageCreated(ctx, image.ID, time.Now(), 0, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
}

func TestImagesDeleteNonexistentImage(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	image := ImageMeta{
		ID:           "image",
		DeleteTaskID: "delete",
	}

	err = storage.ImageDeleted(ctx, image.ID, time.Now())
	require.NoError(t, err)

	err = storage.ImageCreated(ctx, image.ID, time.Now(), 0, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	deletingAt := time.Now()
	actual, err := storage.DeleteImage(ctx, image.ID, "delete", deletingAt)
	require.NoError(t, err)
	requireImagesAreEqual(t, image, *actual)

	// Check idempotency.
	deletingAt = deletingAt.Add(time.Second)
	actual, err = storage.DeleteImage(ctx, image.ID, "delete", deletingAt)
	require.NoError(t, err)
	requireImagesAreEqual(t, image, *actual)

	_, err = storage.CreateImage(ctx, image)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.ImageDeleted(ctx, image.ID, time.Now())
	require.NoError(t, err)
}

func TestImagesClearDeletedImages(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	deletedAt := time.Now()
	deletedBefore := deletedAt.Add(-time.Microsecond)

	err = storage.ClearDeletedImages(ctx, deletedBefore, 10)
	require.NoError(t, err)

	image := ImageMeta{
		ID:       "image",
		FolderID: "folder",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	created, err := storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.NotNil(t, created)

	_, err = storage.DeleteImage(ctx, image.ID, "delete", deletedAt)
	require.NoError(t, err)

	err = storage.ImageDeleted(ctx, image.ID, deletedAt)
	require.NoError(t, err)

	err = storage.ClearDeletedImages(ctx, deletedBefore, 10)
	require.NoError(t, err)

	_, err = storage.CreateImage(ctx, image)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	deletedBefore = deletedAt.Add(time.Microsecond)
	err = storage.ClearDeletedImages(ctx, deletedBefore, 10)
	require.NoError(t, err)

	created, err = storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.NotNil(t, created)
}

func TestImagesCreateImageShouldFailIfSnapshotAlreadyExists(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	snapshot := SnapshotMeta{
		ID: "id",
		Disk: &types.Disk{
			ZoneId: "zone",
			DiskId: "disk",
		},
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreatingAt: time.Now(),
	}
	_, err = storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)

	created, err := storage.CreateImage(ctx, ImageMeta{ID: snapshot.ID})
	require.NoError(t, err)
	require.Nil(t, created)
}

func TestImagesDeleteImageShouldFailIfSnapshotAlreadyExists(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	snapshot := SnapshotMeta{
		ID: "id",
		Disk: &types.Disk{
			ZoneId: "zone",
			DiskId: "disk",
		},
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreatingAt: time.Now(),
	}
	_, err = storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)

	created, err := storage.DeleteImage(ctx, snapshot.ID, "delete", time.Now())
	require.NoError(t, err)
	require.Nil(t, created)
}

func TestImagesGetImage(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	imageID := t.Name()
	imageSize := uint64(2 * 1024 * 1024)
	imageStorageSize := uint64(3 * 1024 * 1024)

	i, err := storage.GetImageMeta(ctx, imageID)
	require.NoError(t, err)
	require.Nil(t, i)

	image := ImageMeta{
		ID:       imageID,
		FolderID: "folder",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	created, err := storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.NotNil(t, created)

	image.CreateRequest = nil

	i, err = storage.GetImageMeta(ctx, imageID)
	require.NoError(t, err)
	require.NotNil(t, i)
	requireImagesAreEqual(t, image, *i)
	require.Equal(t, imageID, i.ID)
	require.Equal(t, "folder", i.FolderID)

	err = storage.ImageCreated(ctx, imageID, time.Now(), imageSize, imageStorageSize)
	require.NoError(t, err)

	image.Size = imageSize
	image.StorageSize = imageStorageSize
	image.Ready = true

	i, err = storage.GetImageMeta(ctx, imageID)
	require.NoError(t, err)
	require.NotNil(t, i)
	requireImagesAreEqual(t, image, *i)
}
