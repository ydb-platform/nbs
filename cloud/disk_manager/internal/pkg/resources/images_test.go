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
	require.Equal(t, expected.CheckpointID, actual.CheckpointID)
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
	require.Equal(t, image.ID, created.ID)
	require.True(t, created.UseDataplaneTasks)

	meta, err := storage.GetImageMeta(ctx, image.ID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.UseDataplaneTasks)

	// Check idempotency.
	created, err = storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.Equal(t, image.ID, created.ID)

	err = storage.ImageCreated(ctx, image.ID, "", time.Now(), 0, 0)
	require.NoError(t, err)

	meta, err = storage.GetImageMeta(ctx, image.ID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.UseDataplaneTasks)
	require.True(t, meta.Ready)

	// Check idempotency.
	err = storage.ImageCreated(ctx, image.ID, "", time.Now(), 0, 0)
	require.NoError(t, err)

	// Check idempotency.
	created, err = storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.Equal(t, image.ID, created.ID)

	require.EqualValues(t, "disk", created.SrcDiskID)

	image.CreateTaskID = "other"
	_, err = storage.CreateImage(ctx, image)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonCancellableError()))
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
	require.Equal(t, image.ID, created.ID)

	expected := image
	expected.CreateRequest = nil
	expected.DeleteTaskID = "delete"

	actual, err := storage.DeleteImage(ctx, image.ID, "delete", time.Now())
	require.NoError(t, err)
	requireImagesAreEqual(t, expected, *actual)

	err = storage.ImageCreated(ctx, image.ID, "", time.Now(), 0, 0)
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

	err = storage.ImageCreated(ctx, image.ID, "", time.Now(), 0, 0)
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

	err = storage.ImageCreated(ctx, image.ID, "", time.Now(), 0, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	deletingAt := time.Now()
	actual, err := storage.DeleteImage(ctx, image.ID, "delete", deletingAt)
	require.NoError(t, err)
	require.Nil(t, actual)

	// Check idempotency.
	deletingAt = deletingAt.Add(time.Second)
	actual, err = storage.DeleteImage(ctx, image.ID, "delete", deletingAt)
	require.NoError(t, err)
	require.Nil(t, actual)

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
	require.Equal(t, image.ID, created.ID)

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
	require.Equal(t, image.ID, created.ID)
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

	_, err = storage.CreateImage(ctx, ImageMeta{ID: snapshot.ID})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonCancellableError()))
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
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonCancellableError()))
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
	checkpointID := "checkpoint"

	actualImage, err := storage.GetImageMeta(ctx, imageID)
	require.NoError(t, err)
	require.Nil(t, actualImage)

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
	require.Equal(t, image.ID, created.ID)

	expectedImage := image
	expectedImage.CreateRequest = nil

	checkImage := func() {
		actualImage, err := storage.GetImageMeta(ctx, imageID)
		require.NoError(t, err)
		require.NotNil(t, actualImage)
		requireImagesAreEqual(t, expectedImage, *actualImage)
	}
	checkImage()

	err = storage.ImageCreated(
		ctx,
		imageID,
		checkpointID,
		time.Now(),
		imageSize,
		imageStorageSize,
	)
	require.NoError(t, err)

	expectedImage.Size = imageSize
	expectedImage.StorageSize = imageStorageSize
	expectedImage.CheckpointID = checkpointID
	expectedImage.Ready = true
	checkImage()

	// Check idempotency.
	err = storage.ImageCreated(
		ctx,
		imageID,
		checkpointID,
		time.Now(),
		imageSize,
		imageStorageSize,
	)
	require.NoError(t, err)
	checkImage()

	// Checkpoint id differs.
	err = storage.ImageCreated(
		ctx,
		imageID,
		"foo", // checkpointID
		time.Now(),
		imageSize,
		imageStorageSize,
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	checkImage()

	// Image size differs.
	err = storage.ImageCreated(
		ctx,
		imageID,
		checkpointID,
		time.Now(),
		42, // imageSize
		imageStorageSize,
	)
	require.NoError(t, err)
	checkImage()

	// Image storage size differs.
	err = storage.ImageCreated(
		ctx,
		imageID,
		checkpointID,
		time.Now(),
		imageSize,
		713, // imageStorageSize
	)
	require.NoError(t, err)
	checkImage()
}

func TestImagesBackup(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	image := ImageMeta{
		ID:       "image",
		FolderID: "backed",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	_, err = storage.CreateImage(ctx, image)
	require.NoError(t, err)

	ids, err := storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)

	err = storage.ImageCreated(ctx, image.ID, "checkpoint", time.Now(), 0, 0)
	require.NoError(t, err)

	ids, err = storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []ImageBackupRequest{{
		ImageID:  image.ID,
		BackupID: "create",
	}}, ids)

	enqueuedAt, err := storage.ImageBackupScheduled(ctx, image.ID, "create", "task")
	require.NoError(t, err)
	require.False(t, enqueuedAt.IsZero())

	ids, err = storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)

	scheduled, err := storage.ListScheduledImageBackups(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []ScheduledImageBackup{{
		ImageID:  image.ID,
		BackupID: "create",
		TaskID:   "task",
	}}, scheduled)

	err = storage.RemoveImageFromBackupQueue(ctx, image.ID, "create")
	require.NoError(t, err)

	scheduled, err = storage.ListScheduledImageBackups(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, scheduled)
}

func TestImagesDeletionStopsBackup(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	image := ImageMeta{
		ID:       "image",
		FolderID: "backed",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	_, err = storage.CreateImage(ctx, image)
	require.NoError(t, err)

	err = storage.ImageCreated(ctx, image.ID, "checkpoint", time.Now(), 0, 0)
	require.NoError(t, err)

	_, err = storage.DeleteImage(ctx, image.ID, "delete", time.Now())
	require.NoError(t, err)

	ids, err := storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)

	imageIDs, err := storage.GetImageBackupDeleteQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"image"}, imageIDs)

	_, err = storage.DeleteImage(ctx, image.ID, "delete", time.Now())
	require.NoError(t, err)

	imageIDs, err = storage.GetImageBackupDeleteQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"image"}, imageIDs)

	err = storage.ImageBackupDeletionsCompleted(ctx, imageIDs)
	require.NoError(t, err)

	imageIDs, err = storage.GetImageBackupDeleteQueue(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, imageIDs)
}

func createReadyImage(
	t *testing.T,
	ctx context.Context,
	storage Storage,
	imageID string,
	folderID string,
) {

	_, err := storage.CreateImage(ctx, ImageMeta{
		ID:       imageID,
		FolderID: folderID,
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create_" + imageID,
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	})
	require.NoError(t, err)

	err = storage.ImageCreated(ctx, imageID, "checkpoint", time.Now(), 0, 0)
	require.NoError(t, err)
}

func TestImagesBackupQueuesNewReadyImageOfConfiguredFolder(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	createReadyImage(t, ctx, storage, "image_backed", "backed")
	createReadyImage(t, ctx, storage, "image_other", "other")

	ids, err := storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []ImageBackupRequest{{
		ImageID:  "image_backed",
		BackupID: "create_image_backed",
	}}, ids)
}

// Any image may have a copy in the follower, queued by its folder or by hand,
// so deletion of every image queues deletion of the copy.
func TestImagesDeletionQueuesBackupDeletionOfEveryImage(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	createReadyImage(t, ctx, storage, "image_backed", "backed")
	createReadyImage(t, ctx, storage, "image_other", "other")

	for _, imageID := range []string{"image_backed", "image_other"} {
		_, err = storage.DeleteImage(ctx, imageID, "delete_"+imageID, time.Now())
		require.NoError(t, err)
	}

	imageIDs, err := storage.GetImageBackupDeleteQueue(ctx, 10)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"image_backed", "image_other"}, imageIDs)
}

func TestImagesBackupOfAllFoldersWithWildcard(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorageWithBackupFolders(t, ctx, db, []string{"*"})

	createReadyImage(t, ctx, storage, "image_a", "folder_a")
	createReadyImage(t, ctx, storage, "image_b", "folder_b")

	ids, err := storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.ElementsMatch(t, []ImageBackupRequest{
		{ImageID: "image_a", BackupID: "create_image_a"},
		{ImageID: "image_b", BackupID: "create_image_b"},
	}, ids)

	_, err = storage.DeleteImage(ctx, "image_a", "delete_image_a", time.Now())
	require.NoError(t, err)

	imageIDs, err := storage.GetImageBackupDeleteQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"image_a"}, imageIDs)
}

func newImageBackupTestStorage(t *testing.T) (context.Context, Storage) {
	t.Helper()
	ctx, cancel := context.WithCancel(newContext())
	t.Cleanup(cancel)
	db, err := newYDB(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(ctx)) })

	storage := newStorage(t, ctx, db)
	// Not a backed up folder: only a manual request queues it.
	createReadyImage(t, ctx, storage, "image", "other")
	return ctx, storage
}

func TestEnqueueImageBackupQueuesImageOfAnyFolder(t *testing.T) {
	ctx, storage := newImageBackupTestStorage(t)

	ids, err := storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)

	err = storage.EnqueueImageBackup(ctx, "image", "manual1")
	require.NoError(t, err)

	ids, err = storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []ImageBackupRequest{{
		ImageID:  "image",
		BackupID: "manual1",
	}}, ids)
}

func TestEnqueueImageBackupIsIdempotent(t *testing.T) {
	ctx, storage := newImageBackupTestStorage(t)

	err := storage.EnqueueImageBackup(ctx, "image", "manual1")
	require.NoError(t, err)

	// A request while an attempt is queued joins it.
	err = storage.EnqueueImageBackup(ctx, "image", "manual2")
	require.NoError(t, err)

	ids, err := storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []ImageBackupRequest{{
		ImageID:  "image",
		BackupID: "manual1",
	}}, ids)

	_, err = storage.ImageBackupScheduled(ctx, "image", "manual1", "task1")
	require.NoError(t, err)

	// A stale attempt cannot remove the row of another one.
	err = storage.RemoveImageFromBackupQueue(ctx, "image", "manual2")
	require.NoError(t, err)
	scheduled, err := storage.ListScheduledImageBackups(ctx, 10)
	require.NoError(t, err)
	require.Len(t, scheduled, 1)

	// After the attempt ends, a request starts a new attempt with its own ID,
	// so its task is not the ended one.
	err = storage.RemoveImageFromBackupQueue(ctx, "image", "manual1")
	require.NoError(t, err)
	err = storage.EnqueueImageBackup(ctx, "image", "manual3")
	require.NoError(t, err)

	ids, err = storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []ImageBackupRequest{{
		ImageID:  "image",
		BackupID: "manual3",
	}}, ids)
}

func TestEnqueueImageBackupRejectsUnavailableImage(t *testing.T) {
	ctx, storage := newImageBackupTestStorage(t)

	_, err := storage.CreateImage(ctx, ImageMeta{
		ID:            "creating",
		FolderID:      "other",
		CreateRequest: &wrappers.UInt64Value{Value: 1},
		CreateTaskID:  "create_creating",
		CreatingAt:    time.Now(),
	})
	require.NoError(t, err)

	createReadyImage(t, ctx, storage, "deleting", "other")
	_, err = storage.DeleteImage(ctx, "deleting", "delete", time.Now())
	require.NoError(t, err)

	for _, id := range []string{"", "missing", "creating", "deleting"} {
		err = storage.EnqueueImageBackup(ctx, id, "backup")
		require.Error(t, err, id)
		require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()), id)
	}

	err = storage.EnqueueImageBackup(ctx, "image", "")
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	ids, err := storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestImageBackupCompletedMarksOnlyReadyImage(t *testing.T) {
	ctx, storage := newImageBackupTestStorage(t)

	err := storage.ImageBackupCompleted(ctx, "image")
	require.NoError(t, err)

	meta, err := storage.GetImageMeta(ctx, "image")
	require.NoError(t, err)
	require.True(t, meta.BackupCompleted)

	// A deleting image is not marked: its copy may have copied nothing.
	createReadyImage(t, ctx, storage, "deleting", "other")
	_, err = storage.DeleteImage(ctx, "deleting", "delete", time.Now())
	require.NoError(t, err)

	err = storage.ImageBackupCompleted(ctx, "deleting")
	require.NoError(t, err)

	meta, err = storage.GetImageMeta(ctx, "deleting")
	require.NoError(t, err)
	require.False(t, meta.BackupCompleted)
}

func TestEnqueueImageBackupSkipsBackedUpImage(t *testing.T) {
	ctx, storage := newImageBackupTestStorage(t)

	err := storage.ImageBackupCompleted(ctx, "image")
	require.NoError(t, err)

	err = storage.EnqueueImageBackup(ctx, "image", "manual1")
	require.NoError(t, err)

	ids, err := storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestImageBackupQueueStats(t *testing.T) {
	ctx, storage := newImageBackupTestStorage(t)
	createReadyImage(t, ctx, storage, "image2", "other")

	stats, err := storage.GetImageBackupQueueStats(ctx)
	require.NoError(t, err)
	require.Equal(t, ImageBackupQueueStats{}, stats)

	for _, imageID := range []string{"image", "image2"} {
		err = storage.EnqueueImageBackup(ctx, imageID, "manual")
		require.NoError(t, err)
	}
	_, err = storage.ImageBackupScheduled(ctx, "image", "manual", "task")
	require.NoError(t, err)

	stats, err = storage.GetImageBackupQueueStats(ctx)
	require.NoError(t, err)
	require.Equal(t, ImageBackupQueueStats{Queued: 1, Scheduled: 1}, stats)
}
