package testcommon

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images"
	images_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots"
	snapshot_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
	"github.com/ydb-platform/nbs/cloud/tasks"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
	"github.com/ydb-platform/nbs/contrib/go/cityhash"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

const (
	// Data of snapshots and images is split into chunks of this size.
	SnapshotChunkSize = 4 * 1024 * 1024

	// Is not expected to be reached: the data is removed in seconds.
	snapshotDataDeletionTimeout = 5 * time.Minute

	// Is not expected to be reached: the task ends in seconds.
	checksumTaskTimeout = 5 * time.Minute

	// See RequireChecksumTaskRejectsRecreatedResource.
	recreatedResourceAttemptCount = 10
)

////////////////////////////////////////////////////////////////////////////////

func newRandomData(seed int64, size uint64) []byte {
	data := make([]byte, size)
	rand.New(rand.NewSource(seed)).Read(data)
	return data
}

// Returns data of the whole chunk. The data is the same for the same seed
// and is not compressible.
func NewRandomSnapshotChunkData(seed int64) []byte {
	return newRandomData(seed, SnapshotChunkSize)
}

// Fills the disk so that its snapshot has chunks of all kinds. Depending on
// its index, the chunk of the disk gets:
//   - random data;
//   - a single block of random data at its very end, the rest is not written;
//   - zeroes that are written as data;
//   - zeroed blocks;
//   - nothing.
//
// The last chunk gets nothing whatever its index is, so the disk ends with
// zeroes. The contents are the same from run to run.
// Returns indices of the chunks that are filled with random data, in
// ascending order.
func FillDiskWithDataAndZeroes(
	t *testing.T,
	ctx context.Context,
	nbsClient nbs.TestingClient,
	diskID string,
) []uint32 {

	session, err := nbsClient.MountRW(
		ctx,
		diskID,
		0,   // fillGeneration
		0,   // fillSeqNumber
		nil, // encryption
	)
	require.NoError(t, err)
	defer session.Close(ctx)

	blockSize := uint64(session.BlockSize())
	blocksInChunk := SnapshotChunkSize / blockSize
	chunkCount := uint32(session.BlockCount() / blocksInChunk)
	// A chunk of each kind and the last one.
	require.Greater(t, chunkCount, uint32(5))

	var randomDataChunkIndices []uint32

	for chunkIndex := uint32(0); chunkIndex < chunkCount-1; chunkIndex++ {
		startIndex := uint64(chunkIndex) * blocksInChunk
		seed := int64(chunkIndex)

		switch chunkIndex % 5 {
		case 0:
			err = session.Write(
				ctx,
				startIndex,
				NewRandomSnapshotChunkData(seed),
			)
			randomDataChunkIndices = append(randomDataChunkIndices, chunkIndex)
		case 1:
			err = session.Write(
				ctx,
				startIndex+blocksInChunk-1,
				newRandomData(seed, blockSize),
			)
		case 2:
			err = session.Write(
				ctx,
				startIndex,
				make([]byte, SnapshotChunkSize),
			)
		case 3:
			err = session.Zero(ctx, startIndex, uint32(blocksInChunk))
		}

		require.NoError(t, err)
	}

	return randomDataChunkIndices
}

// Fills the whole disk with random data, so its snapshot has as much data as
// a snapshot of such a disk can have. The contents are the same from run to
// run.
func FillDiskWithRandomData(
	t *testing.T,
	ctx context.Context,
	nbsClient nbs.TestingClient,
	diskID string,
) {

	session, err := nbsClient.MountRW(
		ctx,
		diskID,
		0,   // fillGeneration
		0,   // fillSeqNumber
		nil, // encryption
	)
	require.NoError(t, err)
	defer session.Close(ctx)

	blocksInChunk := SnapshotChunkSize / uint64(session.BlockSize())
	chunkCount := uint32(session.BlockCount() / blocksInChunk)

	// Chunks are written concurrently: it is several times faster.
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)

	for i := uint32(0); i < chunkCount; i++ {
		chunkIndex := i
		group.Go(func() error {
			return session.Write(
				groupCtx,
				uint64(chunkIndex)*blocksInChunk,
				NewRandomSnapshotChunkData(int64(chunkIndex)),
			)
		})
	}

	require.NoError(t, group.Wait())
}

// Reads the disk chunk by chunk, the way a snapshot of the disk is split into
// chunks. Returns CRC32 of the chunks by their indices. Chunks that have
// nothing but zeroes are absent in the result.
func GetDiskChunkChecksums(
	t *testing.T,
	ctx context.Context,
	nbsClient nbs.TestingClient,
	diskID string,
) map[uint32]uint32 {

	session, err := nbsClient.MountRO(ctx, diskID, nil /* encryption */)
	require.NoError(t, err)
	defer session.Close(ctx)

	blockSize := uint64(session.BlockSize())
	blocksInChunk := SnapshotChunkSize / blockSize
	require.Zero(t, session.BlockCount()%blocksInChunk)
	chunkCount := uint32(session.BlockCount() / blocksInChunk)

	chunkChecksums := make(map[uint32]uint32)
	data := make([]byte, SnapshotChunkSize)
	zeroes := make([]byte, SnapshotChunkSize)

	for chunkIndex := uint32(0); chunkIndex < chunkCount; chunkIndex++ {
		// Read does not touch data if there is nothing but zeroes.
		zero := false
		err := session.Read(
			ctx,
			uint64(chunkIndex)*blocksInChunk,
			uint32(blocksInChunk),
			"", // checkpointID
			data,
			&zero,
		)
		require.NoError(t, err)

		if zero || bytes.Equal(data, zeroes) {
			continue
		}

		chunkChecksums[chunkIndex] = crc32.ChecksumIEEE(data)
	}

	return chunkChecksums
}

// Returns the checksum that the snapshot (or the image) should have. Takes
// CRC32 of its chunks by their indices, chunks that have nothing but zeroes
// should be absent.
//
// Computes the checksum by its definition and shares no code with Disk
// Manager. The checksum is CRC32 of the records of the chunks, the records go
// in the order of chunk indices. The record is the index of the chunk
// followed by CRC32 of its data, both are little-endian.
func GetExpectedSnapshotChecksum(chunkChecksums map[uint32]uint32) uint32 {
	chunkIndices := make([]uint32, 0, len(chunkChecksums))
	for chunkIndex := range chunkChecksums {
		chunkIndices = append(chunkIndices, chunkIndex)
	}

	slices.Sort(chunkIndices)

	var records []byte
	for _, chunkIndex := range chunkIndices {
		records = binary.LittleEndian.AppendUint32(records, chunkIndex)
		records = binary.LittleEndian.AppendUint32(
			records,
			chunkChecksums[chunkIndex],
		)
	}

	return crc32.ChecksumIEEE(records)
}

////////////////////////////////////////////////////////////////////////////////

// Tasks that deal with checksums are not available via API of Disk Manager,
// so the task is scheduled right into the database of tasks. It is executed
// by Disk Manager that is started by the recipe, the same way the tasks of
// API requests are. Returns id of the task.
func scheduleChecksumTask(
	t *testing.T,
	ctx context.Context,
	schedule func(context.Context, tasks.Scheduler, string) (string, error),
	resourceID string,
) string {

	db := newYDB(t, ctx)
	defer db.Close(ctx)

	taskStorage, err := tasks_storage.NewStorage(
		&tasks_config.TasksConfig{},
		metrics.NewEmptyRegistry(),
		db,
	)
	require.NoError(t, err)

	// The scheduler is needed to schedule the task and nothing else.
	regularSystemTasksEnabled := false
	scheduler, err := tasks.NewScheduler(
		ctx,
		tasks.NewRegistry(),
		taskStorage,
		&tasks_config.TasksConfig{
			RegularSystemTasksEnabled: &regularSystemTasksEnabled,
		},
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	// The context of a test has no incoming idempotency key, so a new key is
	// generated and each call schedules a new task.
	taskID, err := schedule(ctx, scheduler, resourceID)
	require.NoError(t, err)

	return taskID
}

// Schedules the task that computes the checksum of the snapshot and saves it
// to the database of Disk Manager. Returns id of the task, its response is
// SaveSnapshotChecksumResponse.
func ScheduleSaveSnapshotChecksum(
	t *testing.T,
	ctx context.Context,
	snapshotID string,
) string {

	return scheduleChecksumTask(
		t,
		ctx,
		snapshots.ScheduleSaveSnapshotChecksum,
		snapshotID,
	)
}

// Schedules the task that computes the checksum of the snapshot and compares
// it with the saved one, the task fails if they differ. Returns id of the
// task, its response is ValidateSnapshotChecksumResponse.
func ScheduleValidateSnapshotChecksum(
	t *testing.T,
	ctx context.Context,
	snapshotID string,
) string {

	return scheduleChecksumTask(
		t,
		ctx,
		snapshots.ScheduleValidateSnapshotChecksum,
		snapshotID,
	)
}

// Saves the checksum of the snapshot, see ScheduleSaveSnapshotChecksum.
// Returns the checksum.
func SaveSnapshotChecksum(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	snapshotID string,
) (uint32, error) {

	taskID := ScheduleSaveSnapshotChecksum(t, ctx, snapshotID)

	response := snapshot_protos.SaveSnapshotChecksumResponse{}
	err := internal_client.WaitResponse(ctx, client, taskID, &response)
	return response.Checksum, err
}

// Validates the checksum of the snapshot, see
// ScheduleValidateSnapshotChecksum. Returns the computed checksum.
func ValidateSnapshotChecksum(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	snapshotID string,
) (uint32, error) {

	taskID := ScheduleValidateSnapshotChecksum(t, ctx, snapshotID)

	response := snapshot_protos.ValidateSnapshotChecksumResponse{}
	err := internal_client.WaitResponse(ctx, client, taskID, &response)
	return response.Checksum, err
}

// Schedules the task that computes the checksum of the image and saves it to
// the database of Disk Manager. Returns id of the task, its response is
// SaveImageChecksumResponse.
func ScheduleSaveImageChecksum(
	t *testing.T,
	ctx context.Context,
	imageID string,
) string {

	return scheduleChecksumTask(
		t,
		ctx,
		images.ScheduleSaveImageChecksum,
		imageID,
	)
}

// Schedules the task that computes the checksum of the image and compares it
// with the saved one, the task fails if they differ. Returns id of the task,
// its response is ValidateImageChecksumResponse.
func ScheduleValidateImageChecksum(
	t *testing.T,
	ctx context.Context,
	imageID string,
) string {

	return scheduleChecksumTask(
		t,
		ctx,
		images.ScheduleValidateImageChecksum,
		imageID,
	)
}

// Saves the checksum of the image, see ScheduleSaveImageChecksum. Returns the
// checksum.
func SaveImageChecksum(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	imageID string,
) (uint32, error) {

	taskID := ScheduleSaveImageChecksum(t, ctx, imageID)

	response := images_protos.SaveImageChecksumResponse{}
	err := internal_client.WaitResponse(ctx, client, taskID, &response)
	return response.Checksum, err
}

// Validates the checksum of the image, see ScheduleValidateImageChecksum.
// Returns the computed checksum.
func ValidateImageChecksum(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	imageID string,
) (uint32, error) {

	taskID := ScheduleValidateImageChecksum(t, ctx, imageID)

	response := images_protos.ValidateImageChecksumResponse{}
	err := internal_client.WaitResponse(ctx, client, taskID, &response)
	return response.Checksum, err
}

// Waits until the task ends. Returns the error that the task has failed
// with, nil if it has not failed.
func waitChecksumTask(t *testing.T, ctx context.Context, taskID string) error {
	scheduler := newScheduler(t, ctx)

	err := scheduler.WaitTaskEndedWithTimeout(ctx, taskID, checksumTaskTimeout)
	require.NoError(t, err)

	return scheduler.GetTaskError(ctx, taskID)
}

// Waits until the task fails and checks its error. A silent error is the one
// that is expected to happen, Disk Manager does not report it as its own
// failure.
func RequireChecksumTaskError(
	t *testing.T,
	ctx context.Context,
	taskID string,
	expectedError string,
	expectedSilent bool,
) {

	err := waitChecksumTask(t, ctx, taskID)
	require.ErrorContains(t, err, expectedError)
	require.Equal(t, expectedSilent, errors.IsSilent(err))
}

// Makes the snapshot or the image look like another one that has the same id,
// as if it has been deleted and created again by another task. It takes
// minutes to do that for real, so id of the task that has created it is
// changed right in |table| of Disk Manager: "snapshots" or "images".
func SetCreateTaskID(
	t *testing.T,
	ctx context.Context,
	table string,
	resourceID string,
	createTaskID string,
) {

	db := newYDB(t, ctx)
	defer db.Close(ctx)

	// The table is in the folder of the same name, see NewResourceStorage.
	_, err := db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;
		declare $create_task_id as Utf8;

		update %v
		set create_task_id = $create_task_id
		where id = $id;
	`, db.AbsolutePath(table), table),
		persistence.ValueParam("$id", persistence.UTF8Value(resourceID)),
		persistence.ValueParam(
			"$create_task_id",
			persistence.UTF8Value(createTaskID),
		),
	)
	require.NoError(t, err)
}

// Checks that the checksum task fails if its snapshot or image is recreated
// while the task is in progress. Ids are not expected to be used again, so
// the error is not silent. |schedule| schedules the task and returns its id,
// |recreate| makes the resource look like a recreated one, see
// SetCreateTaskID.
//
// The resource is recreated when the task waits for its checksum to be
// computed: the task has remembered the resource by then and has not checked
// it for the last time yet. The checksum might be computed too fast to get
// there in time, so there are several attempts.
func RequireChecksumTaskRejectsRecreatedResource(
	t *testing.T,
	ctx context.Context,
	schedule func() string,
	recreate func(),
	expectedError string,
) {

	taskStorage := NewTaskStorage(t, ctx)

	for i := 0; i < recreatedResourceAttemptCount; i++ {
		taskID := schedule()

		for {
			state, err := taskStorage.GetTask(ctx, taskID)
			require.NoError(t, err)

			if state.Status == tasks_storage.TaskStatusWaitingToRun {
				recreate()
				break
			}

			if tasks_storage.IsEnded(state.Status) {
				break
			}

			time.Sleep(10 * time.Millisecond)
		}

		err := waitChecksumTask(t, ctx, taskID)
		if err == nil {
			// The task has not noticed anything: it was too late.
			continue
		}

		require.ErrorContains(t, err, expectedError)
		require.False(t, errors.IsSilent(err))
		return
	}

	require.Fail(t, "resource is never recreated in time")
}

////////////////////////////////////////////////////////////////////////////////

// Storage that holds data of snapshots and images of the recipe, together
// with its database. They are used to change the data behind the back of
// Disk Manager: via the storage and right in its tables.
type snapshotDataStorage struct {
	config  *snapshot_config.SnapshotConfig
	db      *persistence.YDBClient
	storage snapshot_storage.Storage
}

func newSnapshotDataStorage(
	t *testing.T,
	ctx context.Context,
) (*snapshotDataStorage, func()) {

	config := newSnapshotConfig()

	db, err := persistence.NewYDBClient(
		ctx,
		config.GetPersistenceConfig(),
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	closeFunc := func() {
		_ = db.Close(ctx)
	}

	storage, err := snapshot_storage.NewStorage(
		config,
		metrics.NewEmptyRegistry(),
		db,
		nil,   // do not need s3 here
		false, // backupEnabled
	)
	require.NoError(t, err)

	return &snapshotDataStorage{
		config:  config,
		db:      db,
		storage: storage,
	}, closeFunc
}

func (s *snapshotDataStorage) tablesPath() string {
	return s.db.AbsolutePath(s.config.GetStorageFolder())
}

// Returns nil if the snapshot has no such chunk.
func (s *snapshotDataStorage) findChunkMapEntry(
	t *testing.T,
	ctx context.Context,
	snapshotID string,
	chunkIndex uint32,
) *snapshot_storage.ChunkMapEntry {

	// Entries go in the order of chunk indices, starting from the given one.
	entries, entriesErrors := s.storage.ReadChunkMap(
		ctx,
		snapshotID,
		chunkIndex,
	)

	var result *snapshot_storage.ChunkMapEntry
	for entry := range entries {
		if entry.ChunkIndex == chunkIndex {
			found := entry
			result = &found
		}
	}

	require.NoError(t, <-entriesErrors)
	return result
}

func (s *snapshotDataStorage) readChunk(
	ctx context.Context,
	entry snapshot_storage.ChunkMapEntry,
) ([]byte, error) {

	chunk := dataplane_common.Chunk{
		ID:         entry.ChunkID,
		Index:      entry.ChunkIndex,
		StoredInS3: entry.StoredInS3,
		Data:       make([]byte, SnapshotChunkSize),
	}

	err := s.storage.ReadChunk(ctx, &chunk)
	return chunk.Data, err
}

////////////////////////////////////////////////////////////////////////////////

// Data of the snapshot (or of the image) that is deleted is removed some time
// after the snapshot is reported to be deleted. Waits until it is removed.
func WaitForSnapshotDataDeletion(
	t *testing.T,
	ctx context.Context,
	snapshotID string,
) {

	deadline := time.Now().Add(snapshotDataDeletionTimeout)

	for {
		// The snapshot is forgotten right after its data is removed.
		snapshotMeta, err := GetSnapshotMeta(t, ctx, snapshotID)
		require.NoError(t, err)

		if snapshotMeta == nil {
			return
		}

		require.True(
			t,
			time.Now().Before(deadline),
			"data of snapshot %v is not deleted",
			snapshotID,
		)

		time.Sleep(time.Second)
	}
}

// Deletes the snapshot (or the image) from the storage of snapshots, the same
// way the task that deletes a snapshot does, and waits until its data is
// removed. Disk Manager is not aware of that: the snapshot is still ready for
// it, but there is nothing to read.
func DeleteSnapshotData(
	t *testing.T,
	ctx context.Context,
	snapshotID string,
) {

	dataStorage, closeFunc := newSnapshotDataStorage(t, ctx)
	defer closeFunc()

	_, err := dataStorage.storage.DeletingSnapshot(
		ctx,
		snapshotID,
		"testcommon", // taskID
	)
	require.NoError(t, err)

	WaitForSnapshotDataDeletion(t, ctx, snapshotID)
}

// Writes the chunk to the snapshot (or to the image) via the storage of
// snapshots, the same way the tasks that create snapshots do. The snapshot is
// expected to be ready, so its contents change, and the new contents are as
// valid as the old ones. Data that Disk Manager has written for this chunk is
// left in the storage, but the snapshot does not refer to it anymore.
// The chunk should either be a zero one or have data of SnapshotChunkSize.
func WriteSnapshotChunk(
	t *testing.T,
	ctx context.Context,
	snapshotID string,
	chunk dataplane_common.Chunk,
) {

	dataStorage, closeFunc := newSnapshotDataStorage(t, ctx)
	defer closeFunc()

	if !chunk.Zero {
		require.Equal(t, SnapshotChunkSize, len(chunk.Data))
	}

	_, err := dataStorage.storage.WriteChunk(
		ctx,
		"testcommon", // uniqueID
		snapshotID,
		chunk,
		false, // useS3
	)
	require.NoError(t, err)

	entry := dataStorage.findChunkMapEntry(t, ctx, snapshotID, chunk.Index)
	require.NotNil(t, entry)

	if chunk.Zero {
		require.Empty(t, entry.ChunkID)
		return
	}

	data, err := dataStorage.readChunk(ctx, *entry)
	require.NoError(t, err)
	// Data is not compared with require.Equal: it would print megabytes.
	require.True(t, bytes.Equal(chunk.Data, data))
}

// Removes the chunk from the chunk map of the snapshot (or of the image), so
// the snapshot gets a hole instead of the chunk. The storage of snapshots has
// no method for it, so the row is deleted from the table. Data of the chunk
// is left in the storage, but the snapshot does not refer to it anymore.
func DeleteSnapshotChunk(
	t *testing.T,
	ctx context.Context,
	snapshotID string,
	chunkIndex uint32,
) {

	dataStorage, closeFunc := newSnapshotDataStorage(t, ctx)
	defer closeFunc()

	entry := dataStorage.findChunkMapEntry(t, ctx, snapshotID, chunkIndex)
	require.NotNil(t, entry)

	_, err := dataStorage.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $snapshot_id as Utf8;
		declare $chunk_index as Uint32;

		delete from chunk_map
		where shard_id = $shard_id and
			snapshot_id = $snapshot_id and
			chunk_index = $chunk_index;
	`, dataStorage.tablesPath()),
		persistence.ValueParam(
			"$shard_id",
			persistence.Uint64Value(cityhash.Hash64([]byte(snapshotID))),
		),
		persistence.ValueParam(
			"$snapshot_id",
			persistence.UTF8Value(snapshotID),
		),
		persistence.ValueParam(
			"$chunk_index",
			persistence.Uint32Value(chunkIndex),
		),
	)
	require.NoError(t, err)

	entry = dataStorage.findChunkMapEntry(t, ctx, snapshotID, chunkIndex)
	require.Nil(t, entry)
}

// Flips several bytes of the data of the chunk right where the data is
// stored. The checksum that is stored with the data is not touched, as if the
// data has been damaged by what it is stored in.
// The chunk should have data that is not compressible: otherwise the damage
// might break the compression, not the data.
func DamageSnapshotChunkData(
	t *testing.T,
	ctx context.Context,
	snapshotID string,
	chunkIndex uint32,
) {

	dataStorage, closeFunc := newSnapshotDataStorage(t, ctx)
	defer closeFunc()

	entry := dataStorage.findChunkMapEntry(t, ctx, snapshotID, chunkIndex)
	require.NotNil(t, entry)
	require.NotEmpty(t, entry.ChunkID)
	// The recipe does not store chunks in s3.
	require.False(t, entry.StoredInS3)

	_, err := dataStorage.readChunk(ctx, *entry)
	require.NoError(t, err)

	chunkBlob, err := dataStorage.storage.ReadChunkBlob(
		ctx,
		entry.ChunkID,
		entry.StoredInS3,
	)
	require.NoError(t, err)
	// Data that is not compressible is stored as is, so the middle of what is
	// stored is the middle of the data.
	require.GreaterOrEqual(t, len(chunkBlob.Data), SnapshotChunkSize)

	damagedData := bytes.Clone(chunkBlob.Data)
	for i := 0; i < 8; i++ {
		damagedData[len(damagedData)/2+i] ^= 0xff
	}

	_, err = dataStorage.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $chunk_id as Utf8;
		declare $data as String;

		update %v
		set data = $data
		where shard_id = $shard_id and
			chunk_id = $chunk_id and
			referer = "";
	`,
		dataStorage.tablesPath(),
		dataStorage.config.GetChunkBlobsTableName(),
	),
		persistence.ValueParam(
			"$shard_id",
			persistence.Uint64Value(cityhash.Hash64([]byte(entry.ChunkID))),
		),
		persistence.ValueParam(
			"$chunk_id",
			persistence.UTF8Value(entry.ChunkID),
		),
		persistence.ValueParam(
			"$data",
			persistence.StringValue(damagedData),
		),
	)
	require.NoError(t, err)

	damagedChunkBlob, err := dataStorage.storage.ReadChunkBlob(
		ctx,
		entry.ChunkID,
		entry.StoredInS3,
	)
	require.NoError(t, err)
	// Data is not compared with require.Equal: it would print megabytes.
	require.True(t, bytes.Equal(damagedData, damagedChunkBlob.Data))
	require.Equal(t, chunkBlob.Checksum, damagedChunkBlob.Checksum)
	require.Equal(t, chunkBlob.Compression, damagedChunkBlob.Compression)

	// The chunk is not readable anymore.
	_, err = dataStorage.readChunk(ctx, *entry)
	require.Error(t, err)
	require.ErrorContains(t, err, "chunk checksum mismatch")
}
