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
	require.Equal(t, expected.Checksum, actual.Checksum)
	require.Equal(t, expected.ChecksumCalculated, actual.ChecksumCalculated)
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

////////////////////////////////////////////////////////////////////////////////

func requireImageChecksum(
	t *testing.T,
	ctx context.Context,
	storage Storage,
	imageID string,
	checksum uint32,
) {

	meta, err := storage.GetImageMeta(ctx, imageID)
	require.NoError(t, err, imageID)
	require.NotNil(t, meta, imageID)
	require.True(t, meta.ChecksumCalculated, imageID)
	require.Equal(t, checksum, meta.Checksum, imageID)
}

func requireNoImageChecksum(
	t *testing.T,
	ctx context.Context,
	storage Storage,
	imageID string,
) {

	meta, err := storage.GetImageMeta(ctx, imageID)
	require.NoError(t, err, imageID)
	require.NotNil(t, meta, imageID)
	require.False(t, meta.ChecksumCalculated, imageID)
	require.Zero(t, meta.Checksum, imageID)
}

func TestImageChecksumCalculated(t *testing.T) {
	ctx, storage := newImageBackupTestStorage(t)

	requireNoImageChecksum(t, ctx, storage, "image")

	// Zero is a valid checksum and differs from a checksum that is not set.
	// A repeated value checks idempotency, another value replaces the stored
	// one.
	checksums := []uint32{0, 0, 0xDEADBEEF, 0xDEADBEEF, 0xFFFFFFFF, 1, 0}
	for _, checksum := range checksums {
		err := storage.ImageChecksumCalculated(
			ctx,
			"image",
			"create_image",
			checksum,
		)
		require.NoError(t, err)

		requireImageChecksum(t, ctx, storage, "image", checksum)
	}
}

// The checksum is stored by rewriting the whole row, which should keep the
// other fields.
func TestImageChecksumCalculatedKeepsOtherFields(t *testing.T) {
	ctx, storage := newImageBackupTestStorage(t)

	image := ImageMeta{
		ID:            "other",
		FolderID:      "folder",
		SrcDiskID:     "src_disk",
		SrcImageID:    "src_image",
		SrcSnapshotID: "src_snapshot",
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

	_, err := storage.CreateImage(ctx, image)
	require.NoError(t, err)

	expected := image
	expected.CreateRequest = nil
	expected.CheckpointID = "checkpoint"
	expected.Size = 2 * 1024 * 1024
	expected.StorageSize = 3 * 1024 * 1024
	expected.Ready = true
	expected.BackupCompleted = true

	err = storage.ImageCreated(
		ctx,
		image.ID,
		expected.CheckpointID,
		time.Now(),
		expected.Size,
		expected.StorageSize,
	)
	require.NoError(t, err)

	err = storage.ImageBackupCompleted(ctx, image.ID)
	require.NoError(t, err)

	checkImage := func() {
		actual, err := storage.GetImageMeta(ctx, image.ID)
		require.NoError(t, err)
		require.NotNil(t, actual)
		requireImagesAreEqual(t, expected, *actual)
		require.Equal(t, expected.SrcDiskID, actual.SrcDiskID)
		require.Equal(t, expected.SrcImageID, actual.SrcImageID)
		require.Equal(t, expected.SrcSnapshotID, actual.SrcSnapshotID)
		require.True(t, proto.Equal(expected.Encryption, actual.Encryption))
		require.Equal(t, expected.BackupCompleted, actual.BackupCompleted)
	}

	checkImage()

	err = storage.ImageChecksumCalculated(
		ctx,
		image.ID,
		image.CreateTaskID,
		42,
	)
	require.NoError(t, err)

	expected.Checksum = 42
	expected.ChecksumCalculated = true
	checkImage()

	// The create request is kept too: creation is still idempotent.
	created, err := storage.CreateImage(ctx, image)
	require.NoError(t, err)
	requireImagesAreEqual(t, expected, created)
}

func TestImageChecksumCalculatedRejectsUnavailableImage(t *testing.T) {
	ctx, storage := newImageBackupTestStorage(t)

	_, err := storage.CreateImage(ctx, ImageMeta{
		ID:            "creating",
		FolderID:      "other",
		CreateRequest: &wrappers.UInt64Value{Value: 1},
		CreateTaskID:  "create_creating",
		CreatingAt:    time.Now(),
	})
	require.NoError(t, err)

	// Its checksum was stored before the deletion.
	createReadyImage(t, ctx, storage, "deleting", "other")
	err = storage.ImageChecksumCalculated(ctx, "deleting", "create_deleting", 7)
	require.NoError(t, err)
	_, err = storage.DeleteImage(ctx, "deleting", "delete", time.Now())
	require.NoError(t, err)

	createReadyImage(t, ctx, storage, "deleted", "other")
	_, err = storage.DeleteImage(ctx, "deleted", "delete", time.Now())
	require.NoError(t, err)
	err = storage.ImageDeleted(ctx, "deleted", time.Now())
	require.NoError(t, err)

	// A ready snapshot is not an image.
	createReadySnapshotForBackup(t, ctx, storage, "snapshot")

	testCases := []struct {
		imageID  string
		checksum uint32
		message  string
	}{
		{"missing", 42, "image with id missing is not found"},
		{"snapshot", 42, "image with id snapshot is not found"},
		{"creating", 42, "status creating can't be set"},
		{"deleting", 42, "status deleting can't be set"},
		// The stored value is not accepted either.
		{"deleting", 7, "status deleting can't be set"},
		{"deleted", 42, "status deleted can't be set"},
	}

	for _, testCase := range testCases {
		err = storage.ImageChecksumCalculated(
			ctx,
			testCase.imageID,
			"create_"+testCase.imageID,
			testCase.checksum,
		)
		require.Error(t, err, testCase.imageID)
		require.True(
			t,
			errors.Is(err, errors.NewEmptyNonRetriableError()),
			testCase.imageID,
		)
		require.True(t, errors.IsSilent(err), testCase.imageID)
		require.ErrorContains(t, err, testCase.message)
	}

	// Nothing is written.
	requireNoImageChecksum(t, ctx, storage, "creating")
	requireImageChecksum(t, ctx, storage, "deleting", 7)
	requireNoImageChecksum(t, ctx, storage, "deleted")
	requireNoSnapshotChecksum(t, ctx, storage, "snapshot")

	for _, imageID := range []string{"missing", "snapshot"} {
		meta, err := storage.GetImageMeta(ctx, imageID)
		require.NoError(t, err, imageID)
		require.Nil(t, meta, imageID)
	}
}

// An image that is deleted and created again with the same id is another
// image: it is created by another task.
func TestImageChecksumCalculatedRejectsImageCreatedByAnotherTask(
	t *testing.T,
) {

	ctx, storage := newImageBackupTestStorage(t)

	err := storage.ImageChecksumCalculated(ctx, "image", "create_image", 7)
	require.NoError(t, err)

	err = storage.ImageChecksumCalculated(ctx, "image", "another", 42)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.False(t, errors.IsSilent(err))
	require.ErrorContains(
		t,
		err,
		"it is created by task create_image, not by task another",
	)

	// Nothing is written.
	requireImageChecksum(t, ctx, storage, "image", 7)
}

// Guards the row mapping: a column that is written but not scanned (or the
// other way round) loses the checksum on the next whole-row rewrite.
func TestImageChecksumSurvivesOtherUpdates(t *testing.T) {
	ctx, storage := newImageBackupTestStorage(t)

	err := storage.ImageChecksumCalculated(ctx, "image", "create_image", 42)
	require.NoError(t, err)
	requireImageChecksum(t, ctx, storage, "image", 42)

	// Updates a single column.
	err = storage.ImageBackupCompleted(ctx, "image")
	require.NoError(t, err)
	requireImageChecksum(t, ctx, storage, "image", 42)

	// Rewrites the whole row.
	meta, err := storage.DeleteImage(ctx, "image", "delete", time.Now())
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.BackupCompleted)
	require.True(t, meta.ChecksumCalculated)
	require.EqualValues(t, 42, meta.Checksum)
	requireImageChecksum(t, ctx, storage, "image", 42)

	err = storage.ImageDeleted(ctx, "image", time.Now())
	require.NoError(t, err)
	requireImageChecksum(t, ctx, storage, "image", 42)
}

func TestImageChecksumCalculatedRacesWithDeletion(t *testing.T) {
	ctx, storage := newImageBackupTestStorage(t)

	for i := 0; i < 8; i++ {
		imageID := fmt.Sprintf("image_%v", i)
		createReadyImage(t, ctx, storage, imageID, "other")

		// The deletion takes longer than storing the checksum, so the
		// checksum is delayed to meet the deletion at its different stages.
		delay := time.Duration(i) * time.Millisecond

		start := make(chan struct{})
		set := make(chan error, 1)
		deleted := make(chan error, 1)
		var deleting *ImageMeta
		go func() {
			<-start
			time.Sleep(delay)
			set <- storage.ImageChecksumCalculated(
				ctx,
				imageID,
				"create_"+imageID,
				42,
			)
		}()
		go func() {
			<-start
			var err error
			deleting, err = storage.DeleteImage(
				ctx,
				imageID,
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

		meta, err := storage.GetImageMeta(ctx, imageID)
		require.NoError(t, err)
		require.NotNil(t, meta)
		require.False(t, meta.Ready)

		if setErr == nil {
			requireImageChecksum(t, ctx, storage, imageID, 42)
		} else {
			requireNoImageChecksum(t, ctx, storage, imageID)
		}
	}
}

// Storing the checksum rewrites the whole row and should not lose a column
// that is updated at the same time.
func TestImageChecksumCalculatedRacesWithBackupCompletion(t *testing.T) {
	ctx, storage := newImageBackupTestStorage(t)

	// Several attempts give more interleavings of the two transactions.
	for i := 0; i < 8; i++ {
		imageID := fmt.Sprintf("image_%v", i)
		createReadyImage(t, ctx, storage, imageID, "other")

		start := make(chan struct{})
		set := make(chan error, 1)
		completed := make(chan error, 1)
		go func() {
			<-start
			set <- storage.ImageChecksumCalculated(
				ctx,
				imageID,
				"create_"+imageID,
				42,
			)
		}()
		go func() {
			<-start
			completed <- storage.ImageBackupCompleted(ctx, imageID)
		}()
		close(start)

		setErr, completeErr := <-set, <-completed
		require.NoError(t, setErr)
		require.NoError(t, completeErr)

		meta, err := storage.GetImageMeta(ctx, imageID)
		require.NoError(t, err)
		require.NotNil(t, meta)
		require.True(t, meta.Ready)
		require.True(t, meta.BackupCompleted)
		require.True(t, meta.ChecksumCalculated)
		require.EqualValues(t, 42, meta.Checksum)
	}
}

// An image created before the checksum columns were added has no checksum.
func TestImageChecksumOfImageCreatedBeforeMigration(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	// Should be in sync with newStorage.
	folder := fmt.Sprintf("%v/images", t.Name())

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		"images",
		withoutChecksumColumns(t, imageStateTableDescription()),
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

		upsert into images (id, folder_id, size, status)
		values ($id, $folder_id, $size, $status)
	`, db.AbsolutePath(folder)),
		persistence.ValueParam("$id", persistence.UTF8Value("image")),
		persistence.ValueParam("$folder_id", persistence.UTF8Value("folder")),
		persistence.ValueParam("$size", persistence.Uint64Value(42)),
		persistence.ValueParam(
			"$status",
			persistence.Int64Value(int64(imageStatusReady)),
		),
	)
	require.NoError(t, err)

	// The table has no checksum columns yet, so they can't be read.
	_, err = readStoredChecksum(ctx, db, folder, "images", "image")
	require.Error(t, err)

	// Adds the checksum columns to the existing table.
	storage := newStorage(t, ctx, db)

	// The existing row holds NULL in both columns...
	stored, err := readStoredChecksum(ctx, db, folder, "images", "image")
	require.NoError(t, err)
	require.Equal(
		t,
		storedChecksum{checksumIsNull: true, calculatedIsNull: true},
		stored,
	)

	// ... which is read as a checksum that is not calculated.
	checkImage := func(checksumCalculated bool) {
		meta, err := storage.GetImageMeta(ctx, "image")
		require.NoError(t, err)
		require.NotNil(t, meta)
		require.True(t, meta.Ready)
		require.Equal(t, "folder", meta.FolderID)
		require.EqualValues(t, 42, meta.Size)
		require.Equal(t, checksumCalculated, meta.ChecksumCalculated)
		require.Zero(t, meta.Checksum)
	}

	checkImage(false)

	// Zero differs from a checksum that is not calculated.
	err = storage.ImageChecksumCalculated(
		ctx,
		"image",
		"", // createTaskID
		0,
	)
	require.NoError(t, err)
	checkImage(true)

	stored, err = readStoredChecksum(ctx, db, folder, "images", "image")
	require.NoError(t, err)
	require.Equal(t, storedChecksum{calculated: true}, stored)

	// The next run of the migration keeps the checksum.
	storage = newStorage(t, ctx, db)
	checkImage(true)
}
