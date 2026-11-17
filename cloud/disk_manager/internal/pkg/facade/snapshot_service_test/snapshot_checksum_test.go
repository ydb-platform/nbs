package tests

import (
	"context"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	snapshot_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
)

////////////////////////////////////////////////////////////////////////////////

const (
	checksumTestChunkCount = 40
	// It takes Disk Manager about a second to compute the checksum of the
	// snapshot that has this many chunks of data.
	checksumTestBigChunkCount = 256
)

////////////////////////////////////////////////////////////////////////////////

func createChecksumTestSnapshot(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	diskID string,
	snapshotID string,
) {

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
}

func deleteChecksumTestSnapshot(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	snapshotID string,
) {

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.DeleteSnapshot(
		reqCtx,
		&disk_manager.DeleteSnapshotRequest{
			SnapshotId: snapshotID,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)
}

func createChecksumTestDisk(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	diskID string,
	chunkCount uint32,
) {

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: int64(chunkCount) * testcommon.SnapshotChunkSize,
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

// Creates the disk that has both data and zeroes, and the snapshot of the
// disk.
// Returns CRC32 of the chunks of the disk by their indices, chunks that have
// nothing but zeroes are absent there. The snapshot should have the same
// chunks, so that is what its checksum is expected to be made of.
// Returns indices of the chunks that are filled with random data as well.
func createChecksumTestDiskAndSnapshot(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	diskID string,
	snapshotID string,
) (map[uint32]uint32, []uint32) {

	createChecksumTestDisk(t, ctx, client, diskID, checksumTestChunkCount)

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

	createChecksumTestSnapshot(t, ctx, client, diskID, snapshotID)

	return chunkChecksums, randomDataChunkIndices
}

// Checks the checksum that is saved in the database of Disk Manager.
func requireSavedSnapshotChecksum(
	t *testing.T,
	ctx context.Context,
	snapshotID string,
	expectedChecksum uint32,
) {

	storage, closeFunc := testcommon.NewResourceStorage(t, ctx)
	defer closeFunc()

	meta, err := storage.GetSnapshotMeta(ctx, snapshotID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.ChecksumCalculated)
	require.Equal(t, expectedChecksum, meta.Checksum)
}

func requireSnapshotChecksumIsNotSaved(
	t *testing.T,
	ctx context.Context,
	snapshotID string,
) {

	storage, closeFunc := testcommon.NewResourceStorage(t, ctx)
	defer closeFunc()

	meta, err := storage.GetSnapshotMeta(ctx, snapshotID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.Ready)
	require.False(t, meta.ChecksumCalculated)
}

////////////////////////////////////////////////////////////////////////////////

func TestSnapshotServiceSaveAndValidateSnapshotChecksum(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	snapshotID := t.Name()

	chunkChecksums, _ := createChecksumTestDiskAndSnapshot(
		t,
		ctx,
		client,
		diskID,
		snapshotID,
	)
	expectedChecksum := testcommon.GetExpectedSnapshotChecksum(chunkChecksums)

	requireSnapshotChecksumIsNotSaved(t, ctx, snapshotID)

	// There is nothing to compare the checksum of the snapshot with.
	_, err = testcommon.ValidateSnapshotChecksum(t, ctx, client, snapshotID)
	require.ErrorContains(
		t,
		err,
		fmt.Sprintf("checksum of snapshot %v is not saved", snapshotID),
	)
	requireSnapshotChecksumIsNotSaved(t, ctx, snapshotID)

	checksum, err := testcommon.SaveSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, checksum)
	requireSavedSnapshotChecksum(t, ctx, snapshotID, expectedChecksum)

	checksum, err = testcommon.ValidateSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, checksum)

	// The checksum does not change when it is saved once again.
	checksum, err = testcommon.SaveSnapshotChecksum(t, ctx, client, snapshotID)
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, checksum)
	requireSavedSnapshotChecksum(t, ctx, snapshotID, expectedChecksum)

	checksum, err = testcommon.ValidateSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, checksum)

	deleteChecksumTestSnapshot(t, ctx, client, snapshotID)

	// The snapshot that is deleted has no checksum to deal with.
	expectedError := fmt.Sprintf(
		"snapshot %v is absent or not ready",
		snapshotID,
	)
	_, err = testcommon.SaveSnapshotChecksum(t, ctx, client, snapshotID)
	require.ErrorContains(t, err, expectedError)
	_, err = testcommon.ValidateSnapshotChecksum(t, ctx, client, snapshotID)
	require.ErrorContains(t, err, expectedError)

	testcommon.CheckConsistency(t, ctx)
}

// The snapshot that has no data has zero checksum. It is as good as any other
// checksum: zero that is saved is not the same as nothing saved.
func TestSnapshotServiceChecksumOfEmptySnapshot(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	snapshotID := t.Name()

	// Nothing is written to the disk.
	createChecksumTestDisk(t, ctx, client, diskID, checksumTestChunkCount)
	createChecksumTestSnapshot(t, ctx, client, diskID, snapshotID)

	_, err = testcommon.ValidateSnapshotChecksum(t, ctx, client, snapshotID)
	require.ErrorContains(
		t,
		err,
		fmt.Sprintf("checksum of snapshot %v is not saved", snapshotID),
	)

	checksum, err := testcommon.SaveSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Zero(t, checksum)
	requireSavedSnapshotChecksum(t, ctx, snapshotID, 0)

	checksum, err = testcommon.ValidateSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Zero(t, checksum)

	deleteChecksumTestSnapshot(t, ctx, client, snapshotID)
	testcommon.CheckConsistency(t, ctx)
}

func TestSnapshotServiceChecksumOfAbsentSnapshot(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	snapshotID := t.Name()
	expectedError := fmt.Sprintf(
		"snapshot %v is absent or not ready",
		snapshotID,
	)

	_, err = testcommon.SaveSnapshotChecksum(t, ctx, client, snapshotID)
	require.ErrorContains(t, err, expectedError)

	_, err = testcommon.ValidateSnapshotChecksum(t, ctx, client, snapshotID)
	require.ErrorContains(t, err, expectedError)

	storage, closeFunc := testcommon.NewResourceStorage(t, ctx)
	defer closeFunc()

	// The tasks do not create anything.
	meta, err := storage.GetSnapshotMeta(ctx, snapshotID)
	require.NoError(t, err)
	require.Nil(t, meta)
}

func TestSnapshotServiceChecksumDetectsOverwrittenChunk(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	snapshotID := t.Name()

	chunkChecksums, randomDataChunkIndices := createChecksumTestDiskAndSnapshot(
		t,
		ctx,
		client,
		diskID,
		snapshotID,
	)

	savedChecksum, err := testcommon.SaveSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		testcommon.GetExpectedSnapshotChecksum(chunkChecksums),
		savedChecksum,
	)

	// One of the chunks gets other data. The data is as valid as the data of
	// any other chunk, so the snapshot is still readable and only its
	// checksum shows that it has changed.
	chunkIndex := randomDataChunkIndices[len(randomDataChunkIndices)/2]
	data := testcommon.NewRandomSnapshotChunkData(checksumTestChunkCount)
	require.NotEqual(t, chunkChecksums[chunkIndex], crc32.ChecksumIEEE(data))

	testcommon.WriteSnapshotChunk(
		t,
		ctx,
		snapshotID,
		dataplane_common.Chunk{Index: chunkIndex, Data: data},
	)

	chunkChecksums[chunkIndex] = crc32.ChecksumIEEE(data)
	actualChecksum := testcommon.GetExpectedSnapshotChecksum(chunkChecksums)
	require.NotEqual(t, savedChecksum, actualChecksum)

	_, err = testcommon.ValidateSnapshotChecksum(t, ctx, client, snapshotID)
	require.ErrorContains(
		t,
		err,
		fmt.Sprintf(
			"checksum mismatch for snapshot %v: saved %v, actual %v",
			snapshotID,
			savedChecksum,
			actualChecksum,
		),
	)
	// Validation changes nothing.
	requireSavedSnapshotChecksum(t, ctx, snapshotID, savedChecksum)

	// The checksum that is saved anew is the checksum of the new contents.
	checksum, err := testcommon.SaveSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Equal(t, actualChecksum, checksum)
	requireSavedSnapshotChecksum(t, ctx, snapshotID, actualChecksum)

	checksum, err = testcommon.ValidateSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Equal(t, actualChecksum, checksum)

	deleteChecksumTestSnapshot(t, ctx, client, snapshotID)
	testcommon.CheckConsistency(t, ctx)
}

func TestSnapshotServiceChecksumDetectsLostChunk(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	snapshotID := t.Name()

	chunkChecksums, randomDataChunkIndices := createChecksumTestDiskAndSnapshot(
		t,
		ctx,
		client,
		diskID,
		snapshotID,
	)

	savedChecksum, err := testcommon.SaveSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		testcommon.GetExpectedSnapshotChecksum(chunkChecksums),
		savedChecksum,
	)

	requireChecksumMismatch := func() uint32 {
		actualChecksum := testcommon.GetExpectedSnapshotChecksum(
			chunkChecksums,
		)
		require.NotEqual(t, savedChecksum, actualChecksum)

		_, err := testcommon.ValidateSnapshotChecksum(
			t,
			ctx,
			client,
			snapshotID,
		)
		require.ErrorContains(
			t,
			err,
			fmt.Sprintf(
				"checksum mismatch for snapshot %v: saved %v, actual %v",
				snapshotID,
				savedChecksum,
				actualChecksum,
			),
		)
		// Validation changes nothing.
		requireSavedSnapshotChecksum(t, ctx, snapshotID, savedChecksum)

		return actualChecksum
	}

	// The first chunk disappears from the snapshot. The snapshot is still
	// readable: it has zeroes instead of the chunk, as it has instead of any
	// other chunk that is absent.
	deletedChunkIndex := randomDataChunkIndices[0]
	testcommon.DeleteSnapshotChunk(t, ctx, snapshotID, deletedChunkIndex)

	delete(chunkChecksums, deletedChunkIndex)
	checksumWithoutDeletedChunk := requireChecksumMismatch()

	// The last chunk of random data is replaced with the chunk of zeroes.
	zeroChunkIndex := randomDataChunkIndices[len(randomDataChunkIndices)-1]
	testcommon.WriteSnapshotChunk(
		t,
		ctx,
		snapshotID,
		dataplane_common.Chunk{Index: zeroChunkIndex, Zero: true},
	)

	delete(chunkChecksums, zeroChunkIndex)
	actualChecksum := requireChecksumMismatch()
	require.NotEqual(t, checksumWithoutDeletedChunk, actualChecksum)

	// The checksum that is saved anew is the checksum of what is left.
	checksum, err := testcommon.SaveSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Equal(t, actualChecksum, checksum)
	requireSavedSnapshotChecksum(t, ctx, snapshotID, actualChecksum)

	checksum, err = testcommon.ValidateSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Equal(t, actualChecksum, checksum)

	deleteChecksumTestSnapshot(t, ctx, client, snapshotID)
	testcommon.CheckConsistency(t, ctx)
}

func TestSnapshotServiceChecksumDetectsDamagedChunk(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	snapshotID := t.Name()

	chunkChecksums, randomDataChunkIndices := createChecksumTestDiskAndSnapshot(
		t,
		ctx,
		client,
		diskID,
		snapshotID,
	)

	savedChecksum, err := testcommon.SaveSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		testcommon.GetExpectedSnapshotChecksum(chunkChecksums),
		savedChecksum,
	)

	// Stored data of one of the chunks gets damaged. Unlike the chunk that is
	// overwritten, this one is not readable anymore: its data does not match
	// the checksum that is stored with it.
	chunkIndex := randomDataChunkIndices[len(randomDataChunkIndices)/2]
	testcommon.DamageSnapshotChunkData(t, ctx, snapshotID, chunkIndex)

	// The checksum is of the data that the chunk has had.
	expectedError := fmt.Sprintf(
		"chunk checksum mismatch: expected %v, actual",
		chunkChecksums[chunkIndex],
	)

	_, err = testcommon.ValidateSnapshotChecksum(t, ctx, client, snapshotID)
	require.ErrorContains(t, err, expectedError)

	// There is no checksum to save, the one that is saved is kept.
	_, err = testcommon.SaveSnapshotChecksum(t, ctx, client, snapshotID)
	require.ErrorContains(t, err, expectedError)
	requireSavedSnapshotChecksum(t, ctx, snapshotID, savedChecksum)

	deleteChecksumTestSnapshot(t, ctx, client, snapshotID)
	testcommon.CheckConsistency(t, ctx)
}

func TestSnapshotServiceChecksumOfIncrementalSnapshot(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	baseSnapshotID := t.Name() + "_base"
	snapshotID := t.Name()

	baseChunkChecksums, randomDataChunkIndices :=
		createChecksumTestDiskAndSnapshot(
			t,
			ctx,
			client,
			diskID,
			baseSnapshotID,
		)

	baseChecksum, err := testcommon.SaveSnapshotChecksum(
		t,
		ctx,
		client,
		baseSnapshotID,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		testcommon.GetExpectedSnapshotChecksum(baseChunkChecksums),
		baseChecksum,
	)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, defaultZoneID)

	writeDiskChunk := func(chunkIndex uint32, data []byte) {
		err := nbsClient.Write(
			diskID,
			int(chunkIndex)*testcommon.SnapshotChunkSize,
			data,
		)
		require.NoError(t, err)
	}

	// One chunk of data gets other data, another one gets zeroes and the
	// last chunk gets data instead of zeroes. The rest of the disk is not
	// touched, so the next snapshot shares its chunks with the base one.
	changedChunkIndex := randomDataChunkIndices[0]
	zeroedChunkIndex := randomDataChunkIndices[1]
	filledChunkIndex := uint32(checksumTestChunkCount - 1)

	writeDiskChunk(
		changedChunkIndex,
		testcommon.NewRandomSnapshotChunkData(checksumTestChunkCount),
	)
	writeDiskChunk(
		zeroedChunkIndex,
		make([]byte, testcommon.SnapshotChunkSize),
	)
	writeDiskChunk(
		filledChunkIndex,
		testcommon.NewRandomSnapshotChunkData(checksumTestChunkCount+1),
	)

	chunkChecksums := testcommon.GetDiskChunkChecksums(
		t,
		ctx,
		nbsClient,
		diskID,
	)
	require.Contains(t, chunkChecksums, changedChunkIndex)
	require.NotEqual(
		t,
		baseChunkChecksums[changedChunkIndex],
		chunkChecksums[changedChunkIndex],
	)
	require.NotContains(t, chunkChecksums, zeroedChunkIndex)
	require.NotContains(t, baseChunkChecksums, filledChunkIndex)
	require.Contains(t, chunkChecksums, filledChunkIndex)

	createChecksumTestSnapshot(t, ctx, client, diskID, snapshotID)
	testcommon.CheckBaseSnapshot(t, ctx, snapshotID, baseSnapshotID)

	checksum, err := testcommon.SaveSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		testcommon.GetExpectedSnapshotChecksum(chunkChecksums),
		checksum,
	)
	require.NotEqual(t, baseChecksum, checksum)
	requireSavedSnapshotChecksum(t, ctx, snapshotID, checksum)

	// The base snapshot has not changed.
	requireSavedSnapshotChecksum(t, ctx, baseSnapshotID, baseChecksum)
	validatedChecksum, err := testcommon.ValidateSnapshotChecksum(
		t,
		ctx,
		client,
		baseSnapshotID,
	)
	require.NoError(t, err)
	require.Equal(t, baseChecksum, validatedChecksum)

	// The snapshot keeps its contents when the base one is deleted together
	// with its data.
	deleteChecksumTestSnapshot(t, ctx, client, baseSnapshotID)
	testcommon.WaitForSnapshotDataDeletion(t, ctx, baseSnapshotID)

	validatedChecksum, err = testcommon.ValidateSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Equal(t, checksum, validatedChecksum)

	deleteChecksumTestSnapshot(t, ctx, client, snapshotID)
	testcommon.CheckConsistency(t, ctx)
}

func TestSnapshotServiceChecksumOfBigSnapshot(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	snapshotID := t.Name()

	createChecksumTestDisk(t, ctx, client, diskID, checksumTestBigChunkCount)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, defaultZoneID)
	testcommon.FillDiskWithRandomData(t, ctx, nbsClient, diskID)

	chunkChecksums := testcommon.GetDiskChunkChecksums(
		t,
		ctx,
		nbsClient,
		diskID,
	)
	require.Len(t, chunkChecksums, checksumTestBigChunkCount)
	expectedChecksum := testcommon.GetExpectedSnapshotChecksum(chunkChecksums)

	createChecksumTestSnapshot(t, ctx, client, diskID, snapshotID)

	checksum, err := testcommon.SaveSnapshotChecksum(
		t,
		ctx,
		client,
		snapshotID,
	)
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, checksum)

	// Schedules the task, returns the function that waits for the task and
	// checks its result.
	scheduleTask := func(save bool) func() {
		var taskID string
		var response interface {
			proto.Message
			GetChecksum() uint32
		}

		if save {
			taskID = testcommon.ScheduleSaveSnapshotChecksum(
				t,
				ctx,
				snapshotID,
			)
			response = &snapshot_protos.SaveSnapshotChecksumResponse{}
		} else {
			taskID = testcommon.ScheduleValidateSnapshotChecksum(
				t,
				ctx,
				snapshotID,
			)
			response = &snapshot_protos.ValidateSnapshotChecksumResponse{}
		}

		return func() {
			err := internal_client.WaitResponse(ctx, client, taskID, response)
			require.NoError(t, err)
			require.Equal(t, expectedChecksum, response.GetChecksum())
		}
	}

	// Several tasks are in progress all the time: a new one is scheduled
	// when the oldest one is finished. Those of them that read the snapshot
	// share the throughput of Disk Manager, so it takes them seconds, and
	// there is hardly a moment when no task is in the middle of reading.
	//
	// With nemesis, each Disk Manager is restarted at least once in 30
	// seconds. The tasks go on for longer than that there, so some of them
	// are most likely interrupted in the middle of reading. Such a task is
	// executed again and continues from the state that it has saved, if it
	// has managed to save any. Whatever happens to the tasks, the checksum
	// should be the same.
	//
	// The task that reads the snapshot keeps up to hundreds of megabytes of
	// its chunks in memory, so there should not be too many tasks.
	inflightTaskCount := 8
	minTaskCount := inflightTaskCount
	minDuration := time.Duration(0)
	if testcommon.IsNemesisEnabled() {
		minDuration = 45 * time.Second
	}

	var waitTasks []func()
	startedAt := time.Now()

	for i := 0; i < minTaskCount || time.Since(startedAt) < minDuration; i++ {
		if i < inflightTaskCount {
			// The first tasks should not be scheduled at once: otherwise
			// they are finished at once as well, and so are the tasks that
			// are scheduled after them.
			time.Sleep(500 * time.Millisecond)
		} else {
			waitTasks[i-inflightTaskCount]()
		}

		// One task of four saves the checksum, the others validate it.
		waitTasks = append(waitTasks, scheduleTask(i%4 == 0))
	}

	for _, waitTask := range waitTasks[len(waitTasks)-inflightTaskCount:] {
		waitTask()
	}

	requireSavedSnapshotChecksum(t, ctx, snapshotID, expectedChecksum)

	deleteChecksumTestSnapshot(t, ctx, client, snapshotID)
	testcommon.DeleteDisk(t, ctx, client, diskID)
	testcommon.CheckConsistency(t, ctx)
}
