package tests

import (
	"context"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
)

////////////////////////////////////////////////////////////////////////////////

const (
	checksumTestChunkCount = 40
	checksumTestDiskSize   = checksumTestChunkCount *
		testcommon.SnapshotChunkSize
)

////////////////////////////////////////////////////////////////////////////////

// Creates the disk that has nothing but zeroes.
func createEmptyChecksumTestDisk(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	diskID string,
) {

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: checksumTestDiskSize,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: defaultZoneID,
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)
}

// Creates the disk that has both data and zeroes.
// Returns CRC32 of the chunks of the disk by their indices, chunks that have
// nothing but zeroes are absent there. An image or a snapshot of the disk
// should have the same chunks, so that is what its checksum is expected to be
// made of.
// Returns indices of the chunks that are filled with random data as well.
func createChecksumTestDisk(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	diskID string,
) (map[uint32]uint32, []uint32) {

	createEmptyChecksumTestDisk(t, ctx, client, diskID)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, defaultZoneID)
	randomDataChunkIndices := testcommon.FillDiskWithDataAndZeroes(
		t,
		ctx,
		nbsClient,
		diskID,
	)

	chunkChecksums := testcommon.GetDiskChunkChecksums(
		t,
		ctx,
		nbsClient,
		diskID,
	)
	// The disk has chunks of random data, other chunks of data and chunks of
	// zeroes.
	for _, chunkIndex := range randomDataChunkIndices {
		require.Contains(t, chunkChecksums, chunkIndex)
	}

	require.Greater(t, len(chunkChecksums), len(randomDataChunkIndices))
	require.Less(t, len(chunkChecksums), checksumTestChunkCount)

	return chunkChecksums, randomDataChunkIndices
}

func createChecksumTestImage(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	request *disk_manager.CreateImageRequest,
) {

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateImage(reqCtx, request)
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)
}

// Creates the disk that has both data and zeroes, and the image of the disk.
// Returns the same as createChecksumTestDisk does.
func createChecksumTestDiskAndImage(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	diskID string,
	imageID string,
) (map[uint32]uint32, []uint32) {

	chunkChecksums, randomDataChunkIndices := createChecksumTestDisk(
		t,
		ctx,
		client,
		diskID,
	)

	createChecksumTestImage(t, ctx, client, &disk_manager.CreateImageRequest{
		Src: &disk_manager.CreateImageRequest_SrcDiskId{
			SrcDiskId: &disk_manager.DiskId{
				ZoneId: defaultZoneID,
				DiskId: diskID,
			},
		},
		DstImageId: imageID,
		FolderId:   "folder",
	})

	return chunkChecksums, randomDataChunkIndices
}

func deleteChecksumTestImage(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	imageID string,
) {

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.DeleteImage(
		reqCtx,
		&disk_manager.DeleteImageRequest{
			ImageId: imageID,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)
}

// Checks the checksum that is saved in the database of Disk Manager.
func requireSavedImageChecksum(
	t *testing.T,
	ctx context.Context,
	imageID string,
	expectedChecksum uint32,
) {

	storage, closeFunc := testcommon.NewResourceStorage(t, ctx)
	defer closeFunc()

	meta, err := storage.GetImageMeta(ctx, imageID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.ChecksumCalculated)
	require.Equal(t, expectedChecksum, meta.Checksum)
}

func requireImageChecksumIsNotSaved(
	t *testing.T,
	ctx context.Context,
	imageID string,
) {

	storage, closeFunc := testcommon.NewResourceStorage(t, ctx)
	defer closeFunc()

	meta, err := storage.GetImageMeta(ctx, imageID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.Ready)
	require.False(t, meta.ChecksumCalculated)
}

////////////////////////////////////////////////////////////////////////////////

func TestImageServiceSaveAndValidateImageChecksum(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	imageID := t.Name()

	chunkChecksums, _ := createChecksumTestDiskAndImage(
		t,
		ctx,
		client,
		diskID,
		imageID,
	)
	expectedChecksum := testcommon.GetExpectedSnapshotChecksum(chunkChecksums)

	requireImageChecksumIsNotSaved(t, ctx, imageID)

	// There is nothing to compare the checksum of the image with.
	_, err = testcommon.ValidateImageChecksum(t, ctx, client, imageID)
	require.ErrorContains(
		t,
		err,
		fmt.Sprintf("checksum of image %v is not saved", imageID),
	)
	requireImageChecksumIsNotSaved(t, ctx, imageID)

	checksum, err := testcommon.SaveImageChecksum(t, ctx, client, imageID)
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, checksum)
	requireSavedImageChecksum(t, ctx, imageID, expectedChecksum)

	checksum, err = testcommon.ValidateImageChecksum(t, ctx, client, imageID)
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, checksum)

	// The checksum does not change when it is saved once again.
	checksum, err = testcommon.SaveImageChecksum(t, ctx, client, imageID)
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, checksum)
	requireSavedImageChecksum(t, ctx, imageID, expectedChecksum)

	checksum, err = testcommon.ValidateImageChecksum(t, ctx, client, imageID)
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, checksum)

	deleteChecksumTestImage(t, ctx, client, imageID)

	// The image that is deleted has no checksum to deal with.
	expectedError := fmt.Sprintf("image %v is absent or not ready", imageID)
	_, err = testcommon.SaveImageChecksum(t, ctx, client, imageID)
	require.ErrorContains(t, err, expectedError)
	_, err = testcommon.ValidateImageChecksum(t, ctx, client, imageID)
	require.ErrorContains(t, err, expectedError)

	testcommon.CheckConsistency(t, ctx)
}

// The image that has no data has zero checksum. It is as good as any other
// checksum: zero that is saved is not the same as nothing saved.
func TestImageServiceChecksumOfEmptyImage(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	imageID := t.Name()

	createEmptyChecksumTestDisk(t, ctx, client, diskID)
	createChecksumTestImage(t, ctx, client, &disk_manager.CreateImageRequest{
		Src: &disk_manager.CreateImageRequest_SrcDiskId{
			SrcDiskId: &disk_manager.DiskId{
				ZoneId: defaultZoneID,
				DiskId: diskID,
			},
		},
		DstImageId: imageID,
		FolderId:   "folder",
	})

	_, err = testcommon.ValidateImageChecksum(t, ctx, client, imageID)
	require.ErrorContains(
		t,
		err,
		fmt.Sprintf("checksum of image %v is not saved", imageID),
	)

	checksum, err := testcommon.SaveImageChecksum(t, ctx, client, imageID)
	require.NoError(t, err)
	require.Zero(t, checksum)
	requireSavedImageChecksum(t, ctx, imageID, 0)

	checksum, err = testcommon.ValidateImageChecksum(t, ctx, client, imageID)
	require.NoError(t, err)
	require.Zero(t, checksum)

	deleteChecksumTestImage(t, ctx, client, imageID)
	testcommon.CheckConsistency(t, ctx)
}

func TestImageServiceChecksumOfAbsentImage(t *testing.T) {
	ctx := testcommon.NewContext()

	imageID := t.Name()
	// There is nothing wrong with Disk Manager, so the error is silent.
	expectedError := fmt.Sprintf("image %v is absent or not ready", imageID)

	taskID := testcommon.ScheduleSaveImageChecksum(t, ctx, imageID)
	testcommon.RequireChecksumTaskError(
		t,
		ctx,
		taskID,
		expectedError,
		true, // expectedSilent
	)

	taskID = testcommon.ScheduleValidateImageChecksum(t, ctx, imageID)
	testcommon.RequireChecksumTaskError(
		t,
		ctx,
		taskID,
		expectedError,
		true, // expectedSilent
	)

	storage, closeFunc := testcommon.NewResourceStorage(t, ctx)
	defer closeFunc()

	// The tasks do not create anything.
	meta, err := storage.GetImageMeta(ctx, imageID)
	require.NoError(t, err)
	require.Nil(t, meta)
}

func TestImageServiceChecksumDetectsCorruption(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	imageID := t.Name()

	chunkChecksums, randomDataChunkIndices := createChecksumTestDiskAndImage(
		t,
		ctx,
		client,
		diskID,
		imageID,
	)

	savedChecksum, err := testcommon.SaveImageChecksum(
		t,
		ctx,
		client,
		imageID,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		testcommon.GetExpectedSnapshotChecksum(chunkChecksums),
		savedChecksum,
	)

	// One of the chunks gets other data. The data is as valid as the data of
	// any other chunk, so the image is still readable and only its checksum
	// shows that it has changed.
	overwrittenChunkIndex := randomDataChunkIndices[0]
	data := testcommon.NewRandomSnapshotChunkData(checksumTestChunkCount)
	require.NotEqual(
		t,
		chunkChecksums[overwrittenChunkIndex],
		crc32.ChecksumIEEE(data),
	)

	testcommon.WriteSnapshotChunk(
		t,
		ctx,
		imageID,
		dataplane_common.Chunk{Index: overwrittenChunkIndex, Data: data},
	)

	chunkChecksums[overwrittenChunkIndex] = crc32.ChecksumIEEE(data)
	actualChecksum := testcommon.GetExpectedSnapshotChecksum(chunkChecksums)
	require.NotEqual(t, savedChecksum, actualChecksum)

	_, err = testcommon.ValidateImageChecksum(t, ctx, client, imageID)
	require.ErrorContains(
		t,
		err,
		fmt.Sprintf(
			"checksum mismatch for image %v: saved %v, actual %v",
			imageID,
			savedChecksum,
			actualChecksum,
		),
	)
	// Validation changes nothing.
	requireSavedImageChecksum(t, ctx, imageID, savedChecksum)

	// The checksum that is saved anew is the checksum of the new contents.
	savedChecksum, err = testcommon.SaveImageChecksum(t, ctx, client, imageID)
	require.NoError(t, err)
	require.Equal(t, actualChecksum, savedChecksum)
	requireSavedImageChecksum(t, ctx, imageID, savedChecksum)

	checksum, err := testcommon.ValidateImageChecksum(t, ctx, client, imageID)
	require.NoError(t, err)
	require.Equal(t, savedChecksum, checksum)

	// Stored data of another chunk gets damaged. Unlike the chunk that is
	// overwritten, this one is not readable anymore: its data does not match
	// the checksum that is stored with it.
	damagedChunkIndex := randomDataChunkIndices[1]
	testcommon.DamageSnapshotChunkData(t, ctx, imageID, damagedChunkIndex)

	// The checksum is of the data that the chunk has had.
	expectedError := fmt.Sprintf(
		"chunk checksum mismatch: expected %v, actual",
		chunkChecksums[damagedChunkIndex],
	)

	_, err = testcommon.ValidateImageChecksum(t, ctx, client, imageID)
	require.ErrorContains(t, err, expectedError)

	// There is no checksum to save, the one that is saved is kept.
	_, err = testcommon.SaveImageChecksum(t, ctx, client, imageID)
	require.ErrorContains(t, err, expectedError)
	requireSavedImageChecksum(t, ctx, imageID, savedChecksum)

	deleteChecksumTestImage(t, ctx, client, imageID)
	testcommon.CheckConsistency(t, ctx)
}

// The image is still ready for Disk Manager, but nothing is left of it in the
// storage of snapshots. Unlike the image that is deleted, a ready one should
// not lose its data, so the error is not silent.
func TestImageServiceChecksumDetectsLostData(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	imageID := t.Name()

	chunkChecksums, _ := createChecksumTestDiskAndImage(
		t,
		ctx,
		client,
		diskID,
		imageID,
	)

	savedChecksum, err := testcommon.SaveImageChecksum(t, ctx, client, imageID)
	require.NoError(t, err)
	require.Equal(
		t,
		testcommon.GetExpectedSnapshotChecksum(chunkChecksums),
		savedChecksum,
	)

	// Data of the image is stored as a snapshot with the same id.
	testcommon.DeleteSnapshotData(t, ctx, imageID)

	expectedError := fmt.Sprintf(
		"snapshot with id %v is not found",
		imageID,
	)

	taskID := testcommon.ScheduleValidateImageChecksum(t, ctx, imageID)
	testcommon.RequireChecksumTaskError(
		t,
		ctx,
		taskID,
		expectedError,
		false, // expectedSilent
	)

	// There is no checksum to save, the one that is saved is kept.
	taskID = testcommon.ScheduleSaveImageChecksum(t, ctx, imageID)
	testcommon.RequireChecksumTaskError(
		t,
		ctx,
		taskID,
		expectedError,
		false, // expectedSilent
	)
	requireSavedImageChecksum(t, ctx, imageID, savedChecksum)

	deleteChecksumTestImage(t, ctx, client, imageID)
	testcommon.CheckConsistency(t, ctx)
}

// An image that is deleted and created again with the same id is another
// image. The task that has started with the former has nothing to do with
// the latter. Ids are not expected to be used again, so the error is not
// silent.
func TestImageServiceChecksumOfRecreatedImage(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	imageID := t.Name()

	chunkChecksums, _ := createChecksumTestDiskAndImage(
		t,
		ctx,
		client,
		diskID,
		imageID,
	)

	savedChecksum, err := testcommon.SaveImageChecksum(t, ctx, client, imageID)
	require.NoError(t, err)
	require.Equal(
		t,
		testcommon.GetExpectedSnapshotChecksum(chunkChecksums),
		savedChecksum,
	)

	recreationCount := 0
	recreate := func() {
		recreationCount++
		testcommon.SetCreateTaskID(
			t,
			ctx,
			"images",
			imageID,
			fmt.Sprintf("recreate_%v", recreationCount),
		)
	}
	// Both the task and the storage that saves the checksum report that.
	expectedError := "was deleted and created again with the same id"

	testcommon.RequireChecksumTaskRejectsRecreatedResource(
		t,
		ctx,
		func() string {
			return testcommon.ScheduleValidateImageChecksum(t, ctx, imageID)
		},
		recreate,
		expectedError,
	)

	testcommon.RequireChecksumTaskRejectsRecreatedResource(
		t,
		ctx,
		func() string {
			return testcommon.ScheduleSaveImageChecksum(t, ctx, imageID)
		},
		recreate,
		expectedError,
	)
	requireSavedImageChecksum(t, ctx, imageID, savedChecksum)

	deleteChecksumTestImage(t, ctx, client, imageID)
	testcommon.CheckConsistency(t, ctx)
}

func TestImageServiceChecksumOfImageCreatedFromSnapshot(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	snapshotID := t.Name() + "_snapshot"
	imageID := t.Name() + "_image"

	chunkChecksums, _ := createChecksumTestDisk(t, ctx, client, diskID)
	expectedChecksum := testcommon.GetExpectedSnapshotChecksum(chunkChecksums)

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateSnapshot(
		reqCtx,
		&disk_manager.CreateSnapshotRequest{
			Src: &disk_manager.DiskId{
				ZoneId: defaultZoneID,
				DiskId: diskID,
			},
			SnapshotId: snapshotID,
			FolderId:   "folder",
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	createChecksumTestImage(t, ctx, client, &disk_manager.CreateImageRequest{
		Src: &disk_manager.CreateImageRequest_SrcSnapshotId{
			SrcSnapshotId: snapshotID,
		},
		DstImageId: imageID,
		FolderId:   "folder",
	})

	checksum, err := testcommon.SaveSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, checksum)

	// The image has the contents of the snapshot, so it has its checksum.
	requireImageChecksumIsNotSaved(t, ctx, imageID)

	checksum, err = testcommon.SaveImageChecksum(t, ctx, client, imageID)
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, checksum)
	requireSavedImageChecksum(t, ctx, imageID, expectedChecksum)

	// Checksums of images and checksums of snapshots are dealt with
	// separately, though their data is stored the same way.
	_, err = testcommon.SaveImageChecksum(t, ctx, client, snapshotID)
	require.ErrorContains(
		t,
		err,
		fmt.Sprintf("image %v is absent or not ready", snapshotID),
	)
	_, err = testcommon.SaveSnapshotChecksum(t, ctx, client, imageID)
	require.ErrorContains(
		t,
		err,
		fmt.Sprintf("snapshot %v is absent or not ready", imageID),
	)

	// The image keeps its contents when the snapshot is deleted together
	// with its data.
	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.DeleteSnapshot(
		reqCtx,
		&disk_manager.DeleteSnapshotRequest{
			SnapshotId: snapshotID,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)
	testcommon.WaitForSnapshotDataDeletion(t, ctx, snapshotID)

	checksum, err = testcommon.ValidateImageChecksum(t, ctx, client, imageID)
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, checksum)

	deleteChecksumTestImage(t, ctx, client, imageID)
	testcommon.CheckConsistency(t, ctx)
}
