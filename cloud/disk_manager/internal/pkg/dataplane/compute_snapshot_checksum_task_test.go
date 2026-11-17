package dataplane

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/util"
	"github.com/ydb-platform/nbs/cloud/tasks"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

const (
	checksumTestTaskType = "dataplane.ComputeSnapshotChecksum"
	// Limits every wait of the tests, is not expected to be reached: tasks
	// of the tests take seconds.
	checksumTestTimeout    = 2 * time.Minute
	checksumTestPollPeriod = 50 * time.Millisecond
)

////////////////////////////////////////////////////////////////////////////////

// Data should have the size of the chunk: the task reads chunks into buffers
// of this size. The same seed gives the same data, so the checksums do not
// change from run to run.
func checksumTestDataChunk(index uint32, seed int64) dataplane_common.Chunk {
	data := make([]byte, chunkSize)
	rand.New(rand.NewSource(seed)).Read(data)
	return dataplane_common.Chunk{Index: index, Data: data}
}

func checksumTestFilledChunk(index uint32, value byte) dataplane_common.Chunk {
	return dataplane_common.Chunk{
		Index: index,
		Data:  bytes.Repeat([]byte{value}, chunkSize),
	}
}

func checksumTestZeroChunk(index uint32) dataplane_common.Chunk {
	return dataplane_common.Chunk{Index: index, Zero: true}
}

// Is stored as any other data chunk, although it has nothing but zeroes.
func checksumTestZeroDataChunk(index uint32) dataplane_common.Chunk {
	return checksumTestFilledChunk(index, 0)
}

// Computes the checksum that the snapshot with the given chunks should have.
// Does not share the code with the task: it uses only the chunks that the test
// has written and the standard library.
func checksumTestReference(writtenChunks []dataplane_common.Chunk) uint32 {
	sortedChunks := append([]dataplane_common.Chunk(nil), writtenChunks...)
	sort.Slice(sortedChunks, func(i, j int) bool {
		return sortedChunks[i].Index < sortedChunks[j].Index
	})

	result := uint32(0)
	for _, chunk := range sortedChunks {
		if chunk.Zero || len(bytes.Trim(chunk.Data, "\x00")) == 0 {
			continue
		}

		var record [8]byte
		binary.LittleEndian.PutUint32(record[0:4], chunk.Index)
		binary.LittleEndian.PutUint32(
			record[4:8],
			crc32.ChecksumIEEE(chunk.Data),
		)
		result = crc32.Update(result, crc32.IEEETable, record[:])
	}

	return result
}

func checksumTestChunksBefore(
	writtenChunks []dataplane_common.Chunk,
	chunkIndex uint32,
) []dataplane_common.Chunk {

	var result []dataplane_common.Chunk
	for _, chunk := range writtenChunks {
		if chunk.Index < chunkIndex {
			result = append(result, chunk)
		}
	}

	return result
}

type checksumTestCase struct {
	name  string
	useS3 bool
}

func checksumTestCases() []checksumTestCase {
	return []checksumTestCase{
		{name: "store chunks in ydb", useS3: false},
		{name: "store chunks in s3", useS3: true},
	}
}

////////////////////////////////////////////////////////////////////////////////

type checksumTestSnapshot struct {
	id         string
	chunkCount uint32
	// Indices that are absent here are holes.
	chunks []dataplane_common.Chunk
	// Zero chunks are not written if set, so they become holes too.
	ignoreZeroChunks bool
	useS3            bool
}

// Real storage of snapshots: tests create snapshots in it, change them and
// damage them.
type checksumTestFixture struct {
	ctx     context.Context
	db      *persistence.YDBClient
	s3      *persistence.S3Client
	folder  string
	config  *snapshot_config.SnapshotConfig
	storage snapshot_storage.Storage
	// Each set of runners has a storage of tasks of its own.
	tasksStorageCount int
}

func newChecksumTestFixture(t *testing.T) *checksumTestFixture {
	ctx, cancel := context.WithCancel(test.NewContext())
	t.Cleanup(cancel)

	// What the client of the database does on its own is logged without debug
	// messages. Otherwise the client prints a lot when it is closed, and the
	// message of the failed test is lost behind that.
	clientCtx := logging.SetLogger(
		ctx,
		logging.NewStderrLogger(logging.InfoLevel),
	)

	db, err := newYDB(clientCtx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close(clientCtx)
	})

	s3, err := test.NewS3Client()
	require.NoError(t, err)

	// Unique folder makes it possible to run the test many times with the same
	// database.
	folder := fmt.Sprintf(
		"dataplane_checksum_tests/%v/%v",
		t.Name(),
		time.Now().UnixNano(),
	)
	storageFolder := folder + "/snapshots"
	deleteWorkerCount := uint32(10)
	shallowCopyWorkerCount := uint32(11)
	shallowCopyInflightLimit := uint32(22)
	shardCount := uint64(2)
	s3Bucket := "test"

	snapshotConfig := &snapshot_config.SnapshotConfig{
		StorageFolder:             &storageFolder,
		DeleteWorkerCount:         &deleteWorkerCount,
		ShallowCopyWorkerCount:    &shallowCopyWorkerCount,
		ShallowCopyInflightLimit:  &shallowCopyInflightLimit,
		ChunkBlobsTableShardCount: &shardCount,
		ChunkMapTableShardCount:   &shardCount,
		S3Bucket:                  &s3Bucket,
		ChunkBlobsS3KeyPrefix:     &folder,
	}

	err = schema.Create(
		ctx,
		snapshotConfig,
		db,
		s3,
		false, // dropUnusedColumns
	)
	require.NoError(t, err)

	storage, err := snapshot_storage.NewStorage(
		snapshotConfig,
		metrics.NewEmptyRegistry(),
		db,
		s3,
	)
	require.NoError(t, err)

	return &checksumTestFixture{
		ctx:     ctx,
		db:      db,
		s3:      s3,
		folder:  folder,
		config:  snapshotConfig,
		storage: storage,
	}
}

func (f *checksumTestFixture) createSnapshot(
	t *testing.T,
	params checksumTestSnapshot,
) {

	_, err := f.storage.CreateSnapshot(
		f.ctx,
		snapshot_storage.SnapshotMeta{ID: params.id},
	)
	require.NoError(t, err)

	f.writeChunks(t, params, params.chunks)

	dataChunkCount := uint64(0)
	for _, chunk := range params.chunks {
		if !chunk.Zero {
			dataChunkCount++
		}
	}

	err = f.storage.SnapshotCreated(
		f.ctx,
		params.id,
		uint64(params.chunkCount)*chunkSize, // size
		dataChunkCount*chunkSize,            // storageSize
		params.chunkCount,
		nil, // encryption
	)
	require.NoError(t, err)
}

// Writes the chunks to the snapshot the same way the tasks that create
// snapshots do. Is also used to change the snapshot that is already created:
// the storage does not forbid it.
func (f *checksumTestFixture) writeChunks(
	t *testing.T,
	params checksumTestSnapshot,
	chunksToWrite []dataplane_common.Chunk,
) {

	target := snapshot.NewSnapshotTarget(
		"checksum_test", // uniqueID
		params.id,
		f.storage,
		params.ignoreZeroChunks,
		params.useS3,
		"", // storageClass
	)
	defer target.Close(f.ctx)

	for _, chunk := range chunksToWrite {
		err := target.Write(f.ctx, chunk)
		require.NoError(t, err)
	}
}

// Returns entries ordered by chunk index.
func (f *checksumTestFixture) readChunkMap(
	t *testing.T,
	snapshotID string,
) []snapshot_storage.ChunkMapEntry {

	entries, entriesErrors := f.storage.ReadChunkMap(f.ctx, snapshotID, 0)

	var result []snapshot_storage.ChunkMapEntry
	for entry := range entries {
		result = append(result, entry)
	}

	require.NoError(t, <-entriesErrors)
	return result
}

// Returns nil if the chunk is a hole.
func (f *checksumTestFixture) findChunkMapEntry(
	t *testing.T,
	snapshotID string,
	chunkIndex uint32,
) *snapshot_storage.ChunkMapEntry {

	for _, entry := range f.readChunkMap(t, snapshotID) {
		if entry.ChunkIndex == chunkIndex {
			return &entry
		}
	}

	return nil
}

// Removes the chunk from the chunk map table itself: the storage has no method
// for it. The chunk becomes a hole.
func (f *checksumTestFixture) deleteChunkMapEntry(
	t *testing.T,
	snapshotID string,
	chunkIndex uint32,
) {

	require.NotNil(t, f.findChunkMapEntry(t, snapshotID, chunkIndex))

	_, err := f.db.ExecuteRW(f.ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $chunk_index as Uint32;

		delete from chunk_map
		where snapshot_id = $snapshot_id and chunk_index = $chunk_index;
	`, f.db.AbsolutePath(f.config.GetStorageFolder())),
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

	require.Nil(t, f.findChunkMapEntry(t, snapshotID, chunkIndex))
}

// Flips bytes of the stored data of the chunk where it is stored: in the
// table of chunk blobs or in the s3 object. Its stored checksum is not
// touched, as if the data has been damaged by the storage.
func (f *checksumTestFixture) damageChunk(
	t *testing.T,
	snapshotID string,
	chunkIndex uint32,
) {

	entry := f.findChunkMapEntry(t, snapshotID, chunkIndex)
	require.NotNil(t, entry)
	require.NotEmpty(t, entry.ChunkID)

	chunkBlob, err := f.storage.ReadChunkBlob(
		f.ctx,
		entry.ChunkID,
		entry.StoredInS3,
	)
	require.NoError(t, err)
	// Random data is not compressible, so the blob holds it as is and the
	// middle of the blob is the middle of the data.
	require.GreaterOrEqual(t, len(chunkBlob.Data), chunkSize)

	damagedChunkBlob := chunks.ChunkBlob{
		Data:        append([]byte(nil), chunkBlob.Data...),
		Checksum:    chunkBlob.Checksum,
		Compression: chunkBlob.Compression,
	}

	for i := 0; i < 8; i++ {
		damagedChunkBlob.Data[len(damagedChunkBlob.Data)/2+i] ^= 0xff
	}

	if entry.StoredInS3 {
		err = f.s3.PutObject(
			f.ctx,
			f.config.GetS3Bucket(),
			test.NewS3Key(f.config, entry.ChunkID),
			chunks.NewS3Object(damagedChunkBlob),
		)
	} else {
		_, err = f.db.ExecuteRW(f.ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $chunk_id as Utf8;
			declare $data as String;

			update %v
			set data = $data
			where chunk_id = $chunk_id and referer = "";
		`,
			f.db.AbsolutePath(f.config.GetStorageFolder()),
			f.config.GetChunkBlobsTableName(),
		),
			persistence.ValueParam(
				"$chunk_id",
				persistence.UTF8Value(entry.ChunkID),
			),
			persistence.ValueParam(
				"$data",
				persistence.StringValue(damagedChunkBlob.Data),
			),
		)
	}

	require.NoError(t, err)

	storedChunkBlob, err := f.storage.ReadChunkBlob(
		f.ctx,
		entry.ChunkID,
		entry.StoredInS3,
	)
	require.NoError(t, err)
	// Data is not compared with require.Equal: it would print megabytes.
	require.True(t, bytes.Equal(damagedChunkBlob.Data, storedChunkBlob.Data))
	require.False(t, bytes.Equal(chunkBlob.Data, storedChunkBlob.Data))
	require.Equal(t, chunkBlob.Checksum, storedChunkBlob.Checksum)
	require.Equal(t, chunkBlob.Compression, storedChunkBlob.Compression)
}

////////////////////////////////////////////////////////////////////////////////

// Real storage of tasks, real scheduler and real runners that execute
// dataplane.ComputeSnapshotChecksum tasks.
type checksumTestRunners struct {
	ctx         context.Context
	taskStorage tasks_storage.Storage
	scheduler   tasks.Scheduler
	// Is used to make idempotency keys.
	scheduledTaskCount int
}

// Tasks read snapshots from |storage|.
func (f *checksumTestFixture) startRunners(
	t *testing.T,
	storage snapshot_storage.Storage,
) *checksumTestRunners {

	f.tasksStorageCount++

	pollForTaskUpdatesPeriod := "50ms"
	pollForTasksPeriodMin := "50ms"
	pollForTasksPeriodMax := "100ms"
	pollForStallingTasksPeriodMin := "100ms"
	pollForStallingTasksPeriodMax := "200ms"
	taskPingPeriod := "100ms"
	// The task that has failed with a retriable error is run again after this
	// timeout. It is the timeout of a ping as well, and the attempt that has
	// failed to ping is cancelled. So the tests should be given at least two
	// cores: the transfer keeps one core busy while it waits for the last
	// chunks, and with a single core pings time out again and again.
	taskStallingTimeout := "1s"
	taskWaitingTimeout := "10s"
	scheduleRegularTasksPeriodMin := "100ms"
	scheduleRegularTasksPeriodMax := "200ms"
	runnersCount := uint64(2)
	stalkingRunnersCount := uint64(1)
	storageFolder := fmt.Sprintf("%v/tasks_%v", f.folder, f.tasksStorageCount)
	maxRetriableErrorCount := uint64(100)
	maxPanicCount := uint64(0)
	regularSystemTasksEnabled := false
	tasksToListLimit := uint64(100)

	tasksConfig := &tasks_config.TasksConfig{
		PollForTaskUpdatesPeriod:      &pollForTaskUpdatesPeriod,
		PollForTasksPeriodMin:         &pollForTasksPeriodMin,
		PollForTasksPeriodMax:         &pollForTasksPeriodMax,
		PollForStallingTasksPeriodMin: &pollForStallingTasksPeriodMin,
		PollForStallingTasksPeriodMax: &pollForStallingTasksPeriodMax,
		TaskPingPeriod:                &taskPingPeriod,
		TaskStallingTimeout:           &taskStallingTimeout,
		TaskWaitingTimeout:            &taskWaitingTimeout,
		ScheduleRegularTasksPeriodMin: &scheduleRegularTasksPeriodMin,
		ScheduleRegularTasksPeriodMax: &scheduleRegularTasksPeriodMax,
		RunnersCount:                  &runnersCount,
		StalkingRunnersCount:          &stalkingRunnersCount,
		StorageFolder:                 &storageFolder,
		MaxRetriableErrorCount:        &maxRetriableErrorCount,
		MaxPanicCount:                 &maxPanicCount,
		RegularSystemTasksEnabled:     &regularSystemTasksEnabled,
		TasksToListLimit:              &tasksToListLimit,
	}

	// Is cancelled before the database client is closed: cleanups are called
	// in the reverse order.
	ctx, cancel := context.WithCancel(f.ctx)
	t.Cleanup(cancel)

	err := tasks_storage.CreateYDBTables(
		ctx,
		tasksConfig,
		f.db,
		false, // dropUnusedColumns
	)
	require.NoError(t, err)

	taskStorage, err := tasks_storage.NewStorage(
		tasksConfig,
		metrics.NewEmptyRegistry(),
		f.db,
	)
	require.NoError(t, err)

	registry := tasks.NewRegistry()

	scheduler, err := tasks.NewScheduler(
		ctx,
		registry,
		taskStorage,
		tasksConfig,
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	// Every chunk in flight holds a buffer of 4 MiB.
	readerCount := uint32(4)
	writerCount := uint32(4)
	chunksInflightLimit := uint32(8)

	err = registry.RegisterForExecution(
		checksumTestTaskType,
		func() tasks.Task {
			return &computeSnapshotChecksumTask{
				config: &config.DataplaneConfig{
					ReaderCount:         &readerCount,
					WriterCount:         &writerCount,
					ChunksInflightLimit: &chunksInflightLimit,
				},
				performanceConfig: &performance_config.PerformanceConfig{},
				storage:           storage,
			}
		},
	)
	require.NoError(t, err)

	err = tasks.StartRunners(
		ctx,
		taskStorage,
		registry,
		metrics.NewEmptyRegistry(),
		tasksConfig,
		"localhost",
	)
	require.NoError(t, err)

	return &checksumTestRunners{
		ctx:         ctx,
		taskStorage: taskStorage,
		scheduler:   scheduler,
	}
}

// Every call schedules a new task.
func (r *checksumTestRunners) scheduleTask(
	t *testing.T,
	snapshotID string,
) string {

	r.scheduledTaskCount++

	taskID, err := r.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			r.ctx,
			fmt.Sprintf("%v_%v", t.Name(), r.scheduledTaskCount),
		),
		checksumTestTaskType,
		"", // description
		&protos.ComputeSnapshotChecksumRequest{SnapshotId: snapshotID},
	)
	require.NoError(t, err)
	return taskID
}

// Returns the checksum from the response of the task.
func (r *checksumTestRunners) waitTask(
	t *testing.T,
	taskID string,
) (uint32, error) {

	response, err := r.scheduler.WaitTaskSync(
		r.ctx,
		taskID,
		checksumTestTimeout,
	)
	if err != nil {
		return 0, err
	}

	typedResponse, ok := response.(*protos.ComputeSnapshotChecksumResponse)
	require.True(t, ok, "unexpected response type %T", response)
	return typedResponse.Checksum, nil
}

func (r *checksumTestRunners) computeChecksum(
	t *testing.T,
	snapshotID string,
) uint32 {

	result, err := r.waitTask(t, r.scheduleTask(t, snapshotID))
	require.NoError(t, err)
	return result
}

// Returns what is saved in the storage of tasks.
func (r *checksumTestRunners) getTaskState(
	t *testing.T,
	taskID string,
) (tasks_storage.TaskState, *protos.ComputeSnapshotChecksumTaskState) {

	taskState, err := r.taskStorage.GetTask(r.ctx, taskID)
	require.NoError(t, err)

	state := &protos.ComputeSnapshotChecksumTaskState{}
	err = proto.Unmarshal(taskState.State, state)
	require.NoError(t, err)

	return taskState, state
}

func (r *checksumTestRunners) getTaskProgress(
	t *testing.T,
	taskID string,
) float64 {

	metadata, err := r.scheduler.GetTaskMetadata(r.ctx, taskID)
	require.NoError(t, err)

	typedMetadata, ok := metadata.(*protos.ComputeSnapshotChecksumMetadata)
	require.True(t, ok, "unexpected metadata type %T", metadata)
	return typedMetadata.Progress
}

// The task has failed for good: it is not going to be retried.
func (r *checksumTestRunners) requireTaskFailed(
	t *testing.T,
	taskID string,
	expectedErrorMessage string,
) {

	_, err := r.waitTask(t, taskID)
	require.Error(t, err)
	require.ErrorContains(t, err, expectedErrorMessage)

	err = r.scheduler.WaitTaskEndedWithTimeout(
		r.ctx,
		taskID,
		checksumTestTimeout,
	)
	require.NoError(t, err)

	taskState, _ := r.getTaskState(t, taskID)
	require.Equal(t, tasks_storage.TaskStatusCancelled, taskState.Status)
	require.Contains(t, taskState.ErrorMessage, "Non retriable error")
	require.Contains(t, taskState.ErrorMessage, expectedErrorMessage)
}

////////////////////////////////////////////////////////////////////////////////

type checksumStorageCall struct {
	// ReadChunkMap if set, ReadChunk otherwise.
	readChunkMap bool
	// Index to read the chunk map from or index of the chunk to read.
	chunkIndex uint32
}

type checksumHeldChunk struct {
	// Is closed when the reads should not wait anymore.
	done chan struct{}
	// The reads that have been waiting fail with a retriable error if set.
	interrupted bool
	// Number of the reads that are waiting.
	readCount int
}

// Real storage that remembers what is read from it and holds the reads of the
// given chunks: they wait until the test either interrupts them or releases
// them. After that the chunk is read as usual.
type checksumInterruptingStorage struct {
	snapshot_storage.Storage

	mutex      sync.Mutex
	calls      []checksumStorageCall
	heldChunks map[uint32]*checksumHeldChunk
}

func newChecksumInterruptingStorage(
	storage snapshot_storage.Storage,
	heldChunkIndices ...uint32,
) *checksumInterruptingStorage {

	heldChunks := make(map[uint32]*checksumHeldChunk)
	for _, chunkIndex := range heldChunkIndices {
		heldChunks[chunkIndex] = &checksumHeldChunk{
			done: make(chan struct{}),
		}
	}

	return &checksumInterruptingStorage{
		Storage:    storage,
		heldChunks: heldChunks,
	}
}

func (s *checksumInterruptingStorage) ReadChunkMap(
	ctx context.Context,
	snapshotID string,
	milestoneChunkIndex uint32,
) (<-chan snapshot_storage.ChunkMapEntry, <-chan error) {

	s.mutex.Lock()
	s.calls = append(s.calls, checksumStorageCall{
		readChunkMap: true,
		chunkIndex:   milestoneChunkIndex,
	})
	s.mutex.Unlock()

	return s.Storage.ReadChunkMap(ctx, snapshotID, milestoneChunkIndex)
}

func (s *checksumInterruptingStorage) ReadChunk(
	ctx context.Context,
	chunk *dataplane_common.Chunk,
) error {

	s.mutex.Lock()
	s.calls = append(s.calls, checksumStorageCall{chunkIndex: chunk.Index})
	heldChunk, held := s.heldChunks[chunk.Index]
	if held {
		heldChunk.readCount++
	}

	s.mutex.Unlock()

	if held {
		err := s.waitWhileHeld(ctx, chunk.Index, heldChunk)
		if err != nil {
			return err
		}
	}

	return s.Storage.ReadChunk(ctx, chunk)
}

// Returns when the test interrupts or releases the chunk.
func (s *checksumInterruptingStorage) waitWhileHeld(
	ctx context.Context,
	chunkIndex uint32,
	heldChunk *checksumHeldChunk,
) error {

	select {
	case <-heldChunk.done:
	case <-ctx.Done():
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	select {
	case <-heldChunk.done:
		// It does not matter if the context is cancelled at the same time:
		// the test has counted this read as a waiting one.
		if heldChunk.interrupted {
			return errors.NewRetriableErrorf(
				"emulated interruption of the read of chunk %v",
				chunkIndex,
			)
		}

		return nil
	default:
	}

	heldChunk.readCount--
	return ctx.Err()
}

// Fails the reads of the held chunk that are waiting, the chunk is not held
// after that. Does nothing and returns false if there are no such reads.
// Also returns the number of the calls made to the storage before that.
func (s *checksumInterruptingStorage) interrupt(
	chunkIndex uint32,
) (int, bool) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	heldChunk, held := s.heldChunks[chunkIndex]
	if !held || heldChunk.readCount == 0 {
		return 0, false
	}

	heldChunk.interrupted = true
	close(heldChunk.done)
	delete(s.heldChunks, chunkIndex)
	return len(s.calls), true
}

// Lets the reads of the held chunk go on, the chunk is not held after that.
func (s *checksumInterruptingStorage) release(chunkIndex uint32) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	heldChunk, held := s.heldChunks[chunkIndex]
	if held {
		close(heldChunk.done)
		delete(s.heldChunks, chunkIndex)
	}
}

func (s *checksumInterruptingStorage) hasWaitingRead(chunkIndex uint32) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	heldChunk, held := s.heldChunks[chunkIndex]
	return held && heldChunk.readCount != 0
}

func (s *checksumInterruptingStorage) waitForRead(
	t *testing.T,
	chunkIndex uint32,
) {

	deadline := time.Now().Add(checksumTestTimeout)

	for !s.hasWaitingRead(chunkIndex) {
		require.True(
			t,
			time.Now().Before(deadline),
			"chunk %v should be read",
			chunkIndex,
		)
		time.Sleep(checksumTestPollPeriod)
	}
}

func (s *checksumInterruptingStorage) getCalls() []checksumStorageCall {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return append([]checksumStorageCall(nil), s.calls...)
}

type checksumTestProgress struct {
	// What the task has saved.
	state *protos.ComputeSnapshotChecksumTaskState
	// Progress that the metadata of the task shows.
	progress float64
}

// Waits until the read of the held chunk is waiting and the task has saved the
// state that covers all the chunks before this chunk. The task is not able to
// get past the held chunk, so what it has saved does not change until the test
// interrupts or releases the chunk.
func (r *checksumTestRunners) waitForHeldChunk(
	t *testing.T,
	taskID string,
	storage *checksumInterruptingStorage,
	chunkIndex uint32,
) checksumTestProgress {

	deadline := time.Now().Add(checksumTestTimeout)

	for {
		taskState, state := r.getTaskState(t, taskID)
		require.False(
			t,
			tasks_storage.HasResult(taskState.Status),
			"task should not end before chunk %v is read, status %v, error %v",
			chunkIndex,
			tasks_storage.TaskStatusToString(taskState.Status),
			taskState.ErrorMessage,
		)

		if state.NextChunkIndex == chunkIndex &&
			storage.hasWaitingRead(chunkIndex) {

			return checksumTestProgress{
				state:    state,
				progress: r.getTaskProgress(t, taskID),
			}
		}

		require.True(
			t,
			time.Now().Before(deadline),
			"task should save the state with NextChunkIndex %v, state %+v",
			chunkIndex,
			state,
		)
		time.Sleep(checksumTestPollPeriod)
	}
}

// Fails the read of the held chunk after the task has saved its progress.
// Returns this progress and the number of the calls made to the storage before
// the failure.
func (r *checksumTestRunners) interruptTask(
	t *testing.T,
	taskID string,
	storage *checksumInterruptingStorage,
	chunkIndex uint32,
) (checksumTestProgress, int) {

	for {
		progress := r.waitForHeldChunk(t, taskID, storage, chunkIndex)

		// The read is not waiting anymore if its attempt has been cancelled
		// meanwhile, the next attempt is going to wait for the chunk again.
		callCount, interrupted := storage.interrupt(chunkIndex)
		if interrupted {
			return progress, callCount
		}
	}
}

// Data chunks, zero chunks and holes for the tests that hold the task in the
// middle of the snapshot.
func checksumTestSparseChunks() []dataplane_common.Chunk {
	return []dataplane_common.Chunk{
		checksumTestDataChunk(0, 1),
		checksumTestDataChunk(1, 2),
		checksumTestDataChunk(2, 3),
		checksumTestZeroChunk(3),
		// Chunk 4 is a hole.
		checksumTestDataChunk(5, 4),
		checksumTestDataChunk(6, 5),
		// Chunks 7 and 8 are holes.
		checksumTestZeroChunk(9),
		checksumTestDataChunk(10, 6),
		checksumTestDataChunk(11, 7),
		// Chunks 12 and 13 are holes.
	}
}

const checksumTestSparseChunkCount = uint32(14)

// What the task has saved while it waits for the chunk should cover exactly
// the chunks before this chunk.
func requireChecksumTestProgress(
	t *testing.T,
	writtenChunks []dataplane_common.Chunk,
	chunkIndex uint32,
	progress checksumTestProgress,
) {

	require.Equal(t, chunkIndex, progress.state.NextChunkIndex)
	require.Equal(
		t,
		checksumTestReference(
			checksumTestChunksBefore(writtenChunks, chunkIndex),
		),
		progress.state.Checksum,
	)
	require.Equal(
		t,
		checksumTestSparseChunkCount,
		progress.state.ChunkCount,
	)

	expectedProgress :=
		float64(chunkIndex) / float64(checksumTestSparseChunkCount)
	require.InDelta(t, expectedProgress, progress.state.Progress, 1e-9)
	require.InDelta(t, expectedProgress, progress.progress, 1e-9)
}

////////////////////////////////////////////////////////////////////////////////

func TestComputeSnapshotChecksumTask(t *testing.T) {
	for _, testCase := range checksumTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := newChecksumTestFixture(t)
			runners := f.startRunners(t, f.storage)

			writtenChunks := []dataplane_common.Chunk{
				checksumTestDataChunk(0, 1),
				checksumTestDataChunk(1, 2),
				checksumTestZeroChunk(2),
				// Chunk 3 is a hole.
				checksumTestDataChunk(4, 3),
				// Chunks 5 and 6 are holes.
				checksumTestZeroChunk(7),
				checksumTestDataChunk(8, 4),
				checksumTestZeroDataChunk(9),
				checksumTestDataChunk(10, 5),
				checksumTestZeroChunk(11),
				// Chunks from 12 to 15 are holes.
			}

			snapshotID := "snapshot"
			f.createSnapshot(t, checksumTestSnapshot{
				id:         snapshotID,
				chunkCount: 16,
				chunks:     writtenChunks,
				useS3:      testCase.useS3,
			})

			for _, entry := range f.readChunkMap(t, snapshotID) {
				if len(entry.ChunkID) != 0 {
					require.Equal(t, testCase.useS3, entry.StoredInS3)
				}
			}

			expected := checksumTestReference(writtenChunks)
			require.NotZero(t, expected)

			taskID := runners.scheduleTask(t, snapshotID)
			actual, err := runners.waitTask(t, taskID)
			require.NoError(t, err)
			require.Equal(t, expected, actual)

			taskState, state := runners.getTaskState(t, taskID)
			require.Equal(
				t,
				tasks_storage.TaskStatusFinished,
				taskState.Status,
			)
			require.Equal(t, expected, state.Checksum)
			// The last data chunk is chunk 10, so NextChunkIndex is after it.
			// Chunk 11, the last one, is a zero chunk. Zero chunks are not
			// remembered by the target, so NextChunkIndex gets past chunk 11
			// only together with the milestone of the transfer, and the last
			// milestone might be behind the last chunks.
			require.GreaterOrEqual(t, state.NextChunkIndex, uint32(11))
			require.LessOrEqual(t, state.NextChunkIndex, uint32(12))
			require.EqualValues(t, 16, state.ChunkCount)
			require.EqualValues(t, 1, state.Progress)
			// Each of 9 written chunks is counted once, zero chunks included.
			// It is so even if the task has been restarted: the counter is
			// saved together with NextChunkIndex.
			require.EqualValues(t, 9, state.TransferredChunkCount)

			require.EqualValues(t, 1, runners.getTaskProgress(t, taskID))

			// This is how disk-manager-admin shows the task.
			taskStateJSON := util.TaskStateToJSON(&taskState)
			require.True(t, proto.Equal(
				&protos.ComputeSnapshotChecksumRequest{SnapshotId: snapshotID},
				taskStateJSON.Request,
			))
			require.True(t, proto.Equal(state, taskStateJSON.State))
		})
	}
}

// The checksum is stored persistently, so it should never change for the same
// data. The expected value is computed by zlib.crc32 of Python from the
// definition of the checksum:
//
//	crc = lambda byte: zlib.crc32(bytes([byte]) * 4 * 1024 * 1024)
//	zlib.crc32(
//	    struct.pack("<II", 0, crc(1)) +
//	    struct.pack("<II", 5, crc(2)) +
//	    struct.pack("<II", 258, crc(3)))
func TestComputeSnapshotChecksumTaskReturnsKnownChecksum(t *testing.T) {
	f := newChecksumTestFixture(t)
	runners := f.startRunners(t, f.storage)

	writtenChunks := []dataplane_common.Chunk{
		checksumTestFilledChunk(0, 1),
		checksumTestZeroChunk(1),
		checksumTestFilledChunk(5, 2),
		checksumTestZeroDataChunk(6),
		// Both bytes of this index are significant.
		checksumTestFilledChunk(258, 3),
	}

	snapshotID := "snapshot"
	f.createSnapshot(t, checksumTestSnapshot{
		id:         snapshotID,
		chunkCount: 259,
		chunks:     writtenChunks,
	})

	expected := uint32(0xdc8de6cd)
	require.Equal(t, expected, checksumTestReference(writtenChunks))
	require.Equal(t, expected, runners.computeChecksum(t, snapshotID))
}

// Zero chunk that is stored, chunk that is not stored (hole) and stored data
// chunk without anything but zeroes are the same thing for the checksum.
func TestComputeSnapshotChecksumTaskIgnoresZeroChunks(t *testing.T) {
	f := newChecksumTestFixture(t)
	runners := f.startRunners(t, f.storage)

	writtenChunks := []dataplane_common.Chunk{
		checksumTestZeroChunk(0),
		checksumTestDataChunk(1, 1),
		checksumTestZeroChunk(2),
		checksumTestZeroChunk(3),
		checksumTestDataChunk(4, 2),
		checksumTestDataChunk(5, 3),
		checksumTestZeroChunk(6),
	}

	// Incremental snapshots store zero chunks.
	withZeroChunks := checksumTestSnapshot{
		id:         "with_zero_chunks",
		chunkCount: 8,
		chunks:     writtenChunks,
	}

	f.createSnapshot(t, withZeroChunks)

	// Full snapshots and migrated snapshots do not store them.
	withHoles := checksumTestSnapshot{
		id:               "with_holes",
		chunkCount:       8,
		chunks:           writtenChunks,
		ignoreZeroChunks: true,
	}

	f.createSnapshot(t, withHoles)

	withZeroDataChunks := checksumTestSnapshot{
		id:         "with_zero_data_chunks",
		chunkCount: 8,
	}

	for _, chunk := range writtenChunks {
		if chunk.Zero {
			chunk = checksumTestZeroDataChunk(chunk.Index)
		}

		withZeroDataChunks.chunks = append(withZeroDataChunks.chunks, chunk)
	}

	f.createSnapshot(t, withZeroDataChunks)

	// Snapshots do differ in the storage.
	var storedChunks [][]string
	for _, params := range []checksumTestSnapshot{
		withZeroChunks,
		withHoles,
		withZeroDataChunks,
	} {
		kinds := make([]string, 7)
		for i := range kinds {
			kinds[i] = "hole"
		}

		for _, entry := range f.readChunkMap(t, params.id) {
			kinds[entry.ChunkIndex] = "data"
			if len(entry.ChunkID) == 0 {
				kinds[entry.ChunkIndex] = "zero"
			}
		}

		storedChunks = append(storedChunks, kinds)
	}

	require.Equal(
		t,
		[][]string{
			{"zero", "data", "zero", "zero", "data", "data", "zero"},
			{"hole", "data", "hole", "hole", "data", "data", "hole"},
			{"data", "data", "data", "data", "data", "data", "data"},
		},
		storedChunks,
	)

	expected := checksumTestReference(writtenChunks)
	require.NotZero(t, expected)

	require.Equal(t, expected, runners.computeChecksum(t, withZeroChunks.id))
	require.Equal(t, expected, runners.computeChecksum(t, withHoles.id))
	require.Equal(
		t,
		expected,
		runners.computeChecksum(t, withZeroDataChunks.id),
	)
}

func TestComputeSnapshotChecksumTaskIsDeterministic(t *testing.T) {
	f := newChecksumTestFixture(t)
	runners := f.startRunners(t, f.storage)

	writtenChunks := []dataplane_common.Chunk{
		checksumTestDataChunk(0, 1),
		checksumTestZeroChunk(1),
		checksumTestDataChunk(3, 2),
		checksumTestDataChunk(4, 3),
	}

	snapshotID := "snapshot"
	f.createSnapshot(t, checksumTestSnapshot{
		id:         snapshotID,
		chunkCount: 6,
		chunks:     writtenChunks,
	})

	expected := checksumTestReference(writtenChunks)
	require.NotZero(t, expected)

	// Each computation is a new task.
	for i := 0; i < 3; i++ {
		require.Equal(t, expected, runners.computeChecksum(t, snapshotID))
	}

	// Another snapshot with the same data has the same checksum.
	sameSnapshotID := "same_snapshot"
	f.createSnapshot(t, checksumTestSnapshot{
		id:         sameSnapshotID,
		chunkCount: 6,
		chunks:     writtenChunks,
	})
	require.Equal(t, expected, runners.computeChecksum(t, sameSnapshotID))

	// And so does the shallow copy of the snapshot: this is how an image is
	// created from a snapshot.
	copiedSnapshotID := "copied_snapshot"
	_, err := f.storage.CreateSnapshot(
		f.ctx,
		snapshot_storage.SnapshotMeta{ID: copiedSnapshotID},
	)
	require.NoError(t, err)

	err = f.storage.ShallowCopySnapshot(
		f.ctx,
		snapshotID,
		copiedSnapshotID,
		0,   // milestoneChunkIndex
		nil, // saveProgress
	)
	require.NoError(t, err)

	err = f.storage.SnapshotCreated(
		f.ctx,
		copiedSnapshotID,
		6*chunkSize, // size
		3*chunkSize, // storageSize
		6,           // chunkCount
		nil,         // encryption
	)
	require.NoError(t, err)

	require.Equal(
		t,
		f.readChunkMap(t, snapshotID),
		f.readChunkMap(t, copiedSnapshotID),
	)
	require.Equal(t, expected, runners.computeChecksum(t, copiedSnapshotID))
}

func TestComputeSnapshotChecksumTaskReturnsZeroWithoutDataChunks(
	t *testing.T,
) {

	f := newChecksumTestFixture(t)
	runners := f.startRunners(t, f.storage)

	testCases := []struct {
		name       string
		chunkCount uint32
		chunks     []dataplane_common.Chunk
		// NextChunkIndex of the finished task is not greater than this.
		// Nothing but the milestone of the transfer moves it when there are
		// no data chunks, and the last milestone might be behind the last
		// chunks.
		maxNextChunkIndex uint32
	}{
		{
			name: "empty snapshot",
		},
		{
			name:       "snapshot of holes",
			chunkCount: 8,
		},
		{
			name:       "snapshot of zero chunks",
			chunkCount: 8,
			chunks: []dataplane_common.Chunk{
				checksumTestZeroChunk(0),
				checksumTestZeroChunk(1),
				checksumTestZeroDataChunk(3),
				checksumTestZeroChunk(6),
			},
			maxNextChunkIndex: 7,
		},
	}

	for i, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			snapshotID := fmt.Sprintf("snapshot_%v", i)
			f.createSnapshot(t, checksumTestSnapshot{
				id:         snapshotID,
				chunkCount: testCase.chunkCount,
				chunks:     testCase.chunks,
			})

			taskID := runners.scheduleTask(t, snapshotID)
			actual, err := runners.waitTask(t, taskID)
			require.NoError(t, err)
			require.Zero(t, actual)

			_, state := runners.getTaskState(t, taskID)
			require.Zero(t, state.Checksum)
			require.LessOrEqual(
				t,
				state.NextChunkIndex,
				testCase.maxNextChunkIndex,
			)
			require.Equal(t, testCase.chunkCount, state.ChunkCount)
			require.EqualValues(t, 1, state.Progress)
		})
	}
}

// Chunk count of the snapshot is not trusted: chunks after it are taken into
// account too.
func TestComputeSnapshotChecksumTaskCoversChunksAfterChunkCount(
	t *testing.T,
) {

	f := newChecksumTestFixture(t)
	runners := f.startRunners(t, f.storage)

	writtenChunks := []dataplane_common.Chunk{
		checksumTestDataChunk(0, 1),
		checksumTestDataChunk(1, 2),
		checksumTestDataChunk(5, 3),
	}

	for _, chunkCount := range []uint32{0, 1, 2} {
		t.Run(fmt.Sprintf("chunk count %v", chunkCount), func(t *testing.T) {
			snapshotID := fmt.Sprintf("snapshot_%v", chunkCount)
			f.createSnapshot(t, checksumTestSnapshot{
				id:         snapshotID,
				chunkCount: chunkCount,
				chunks:     writtenChunks,
			})

			taskID := runners.scheduleTask(t, snapshotID)
			actual, err := runners.waitTask(t, taskID)
			require.NoError(t, err)
			require.Equal(t, checksumTestReference(writtenChunks), actual)
			require.NotEqual(
				t,
				checksumTestReference(writtenChunks[:2]),
				actual,
			)

			_, state := runners.getTaskState(t, taskID)
			require.EqualValues(t, 6, state.NextChunkIndex)
			require.Equal(t, chunkCount, state.ChunkCount)
			require.EqualValues(t, 1, state.Progress)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

// The task saves its progress several times while it reads the snapshot. Each
// time it should save the checksum of the chunks that are before the saved
// index: this is what the task continues from if it is restarted.
func TestComputeSnapshotChecksumTaskSavesProgress(t *testing.T) {
	f := newChecksumTestFixture(t)
	// The task waits for each of these chunks until the test looks at what
	// the task has saved.
	heldChunkIndices := []uint32{2, 6, 11}
	storage := newChecksumInterruptingStorage(f.storage, heldChunkIndices...)
	runners := f.startRunners(t, storage)

	writtenChunks := checksumTestSparseChunks()
	snapshotID := "snapshot"
	f.createSnapshot(t, checksumTestSnapshot{
		id:         snapshotID,
		chunkCount: checksumTestSparseChunkCount,
		chunks:     writtenChunks,
	})

	taskID := runners.scheduleTask(t, snapshotID)

	for _, chunkIndex := range heldChunkIndices {
		progress := runners.waitForHeldChunk(t, taskID, storage, chunkIndex)
		requireChecksumTestProgress(t, writtenChunks, chunkIndex, progress)

		storage.release(chunkIndex)
	}

	actual, err := runners.waitTask(t, taskID)
	require.NoError(t, err)
	require.Equal(t, checksumTestReference(writtenChunks), actual)
}

// The task is interrupted in the middle of the transfer after it has saved its
// progress. Real runners run it again, it should continue from the saved state
// and return the same checksum as the task that has not been interrupted.
func TestComputeSnapshotChecksumTaskContinuesAfterInterruption(
	t *testing.T,
) {

	f := newChecksumTestFixture(t)

	writtenChunks := checksumTestSparseChunks()
	snapshotID := "snapshot"
	f.createSnapshot(t, checksumTestSnapshot{
		id:         snapshotID,
		chunkCount: checksumTestSparseChunkCount,
		chunks:     writtenChunks,
	})

	expected := checksumTestReference(writtenChunks)
	require.Equal(
		t,
		expected,
		f.startRunners(t, f.storage).computeChecksum(t, snapshotID),
	)

	testCases := []struct {
		name string
		// The task is interrupted when it reads these chunks.
		chunkIndices []uint32
	}{
		{
			name:         "after a zero chunk and a hole",
			chunkIndices: []uint32{5},
		},
		{
			name:         "several times",
			chunkIndices: []uint32{2, 6, 11},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			storage := newChecksumInterruptingStorage(
				f.storage,
				testCase.chunkIndices...,
			)
			runners := f.startRunners(t, storage)

			taskID := runners.scheduleTask(t, snapshotID)

			// Number of the calls made to the storage before each
			// interruption.
			var callCounts []int

			for _, chunkIndex := range testCase.chunkIndices {
				progress, callCount := runners.interruptTask(
					t,
					taskID,
					storage,
					chunkIndex,
				)
				callCounts = append(callCounts, callCount)

				// The progress is saved before the interruption.
				requireChecksumTestProgress(
					t,
					writtenChunks,
					chunkIndex,
					progress,
				)
				require.NotZero(t, progress.state.NextChunkIndex)
				require.NotZero(t, progress.state.Checksum)
				require.NotEqual(t, expected, progress.state.Checksum)
			}

			actual, err := runners.waitTask(t, taskID)
			require.NoError(t, err)
			require.Equal(t, expected, actual)

			_, state := runners.getTaskState(t, taskID)
			require.Equal(t, expected, state.Checksum)
			require.EqualValues(t, 12, state.NextChunkIndex)
			require.EqualValues(t, 1, state.Progress)
			// The chunks that were written before an interruption are not
			// counted once again.
			require.EqualValues(
				t,
				len(writtenChunks),
				state.TransferredChunkCount,
			)

			// The task has continued from the saved state each time: the
			// attempt that follows the interruption reads the chunk map from
			// the saved index, and the chunks before this index are not read
			// again.
			calls := storage.getCalls()
			chunkMapReadCount := 0
			for _, call := range calls {
				if call.readChunkMap {
					chunkMapReadCount++
				}
			}
			// The first attempt and one attempt after each interruption.
			require.GreaterOrEqual(
				t,
				chunkMapReadCount,
				len(testCase.chunkIndices)+1,
			)

			for i, chunkIndex := range testCase.chunkIndices {
				chunkMapIsRead := false

				for _, call := range calls[callCounts[i]:] {
					require.GreaterOrEqual(
						t,
						call.chunkIndex,
						chunkIndex,
						"call %+v after the interruption on chunk %v",
						call,
						chunkIndex,
					)

					if call.readChunkMap && !chunkMapIsRead {
						chunkMapIsRead = true
						require.Equal(t, chunkIndex, call.chunkIndex)
					}
				}

				require.True(t, chunkMapIsRead)
			}
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

// The contents of the snapshot change in a consistent way: chunks can be read
// and their stored checksums match their data. The checksum of the snapshot
// should change.
func TestComputeSnapshotChecksumTaskDetectsChangedContents(t *testing.T) {
	f := newChecksumTestFixture(t)
	runners := f.startRunners(t, f.storage)

	writtenChunks := []dataplane_common.Chunk{
		checksumTestDataChunk(0, 1),
		checksumTestDataChunk(1, 2),
		checksumTestZeroChunk(2),
		// Chunk 3 is a hole.
		checksumTestDataChunk(4, 3),
		// Chunk 5 is a hole.
	}

	testCases := []struct {
		name string
		// Changes the snapshot and returns the chunks that it has after that.
		change func(
			t *testing.T,
			params checksumTestSnapshot,
		) []dataplane_common.Chunk
	}{
		{
			name: "chunk is overwritten",
			change: func(
				t *testing.T,
				params checksumTestSnapshot,
			) []dataplane_common.Chunk {

				chunk := checksumTestDataChunk(1, 4)
				f.writeChunks(t, params, []dataplane_common.Chunk{chunk})

				return []dataplane_common.Chunk{
					writtenChunks[0],
					chunk,
					writtenChunks[2],
					writtenChunks[3],
				}
			},
		},
		{
			name: "one byte of chunk is overwritten",
			change: func(
				t *testing.T,
				params checksumTestSnapshot,
			) []dataplane_common.Chunk {

				chunk := checksumTestDataChunk(4, 3)
				chunk.Data[chunkSize-1] ^= 1
				f.writeChunks(t, params, []dataplane_common.Chunk{chunk})

				return []dataplane_common.Chunk{
					writtenChunks[0],
					writtenChunks[1],
					writtenChunks[2],
					chunk,
				}
			},
		},
		{
			name: "chunk becomes a hole",
			change: func(
				t *testing.T,
				params checksumTestSnapshot,
			) []dataplane_common.Chunk {

				f.deleteChunkMapEntry(t, params.id, 1)

				return []dataplane_common.Chunk{
					writtenChunks[0],
					writtenChunks[2],
					writtenChunks[3],
				}
			},
		},
		{
			name: "chunk becomes zero",
			change: func(
				t *testing.T,
				params checksumTestSnapshot,
			) []dataplane_common.Chunk {

				chunk := checksumTestZeroChunk(0)
				f.writeChunks(t, params, []dataplane_common.Chunk{chunk})

				return []dataplane_common.Chunk{
					chunk,
					writtenChunks[1],
					writtenChunks[2],
					writtenChunks[3],
				}
			},
		},
		{
			name: "hole becomes a chunk",
			change: func(
				t *testing.T,
				params checksumTestSnapshot,
			) []dataplane_common.Chunk {

				chunk := checksumTestDataChunk(5, 4)
				f.writeChunks(t, params, []dataplane_common.Chunk{chunk})

				return append(
					[]dataplane_common.Chunk{chunk},
					writtenChunks...,
				)
			},
		},
		{
			name: "two chunks are swapped",
			change: func(
				t *testing.T,
				params checksumTestSnapshot,
			) []dataplane_common.Chunk {

				// The data is the same, but it is in other chunks now.
				changedChunks := []dataplane_common.Chunk{
					{Index: 0, Data: writtenChunks[1].Data},
					{Index: 1, Data: writtenChunks[0].Data},
				}

				f.writeChunks(t, params, changedChunks)

				return append(
					changedChunks,
					writtenChunks[2],
					writtenChunks[3],
				)
			},
		},
	}

	for i, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			params := checksumTestSnapshot{
				id:         fmt.Sprintf("snapshot_%v", i),
				chunkCount: 6,
				chunks:     writtenChunks,
			}

			f.createSnapshot(t, params)

			checksumBefore := runners.computeChecksum(t, params.id)
			require.Equal(
				t,
				checksumTestReference(writtenChunks),
				checksumBefore,
			)

			changedChunks := testCase.change(t, params)

			checksumAfter := runners.computeChecksum(t, params.id)
			require.NotEqual(t, checksumBefore, checksumAfter)
			require.Equal(
				t,
				checksumTestReference(changedChunks),
				checksumAfter,
			)
		})
	}
}

// The stored data of a chunk is damaged and does not match its stored
// checksum anymore. There is no checksum to return for such a snapshot: the
// task should fail.
func TestComputeSnapshotChecksumTaskFailsOnDamagedChunk(t *testing.T) {
	for _, testCase := range checksumTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := newChecksumTestFixture(t)
			runners := f.startRunners(t, f.storage)

			writtenChunks := []dataplane_common.Chunk{
				checksumTestDataChunk(0, 1),
				checksumTestZeroChunk(1),
				checksumTestDataChunk(2, 2),
				checksumTestDataChunk(4, 3),
			}

			snapshotID := "snapshot"
			f.createSnapshot(t, checksumTestSnapshot{
				id:         snapshotID,
				chunkCount: 6,
				chunks:     writtenChunks,
				useS3:      testCase.useS3,
			})

			// The snapshot is fine so far.
			require.Equal(
				t,
				checksumTestReference(writtenChunks),
				runners.computeChecksum(t, snapshotID),
			)

			f.damageChunk(t, snapshotID, 2)

			expectedErrorMessage := "ydb chunk checksum mismatch"
			if testCase.useS3 {
				expectedErrorMessage = "s3 chunk checksum mismatch"
			}

			runners.requireTaskFailed(
				t,
				runners.scheduleTask(t, snapshotID),
				expectedErrorMessage,
			)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestComputeSnapshotChecksumTaskFailsIfSnapshotIsNotReady(t *testing.T) {
	f := newChecksumTestFixture(t)
	runners := f.startRunners(t, f.storage)

	writtenChunks := []dataplane_common.Chunk{checksumTestFilledChunk(0, 1)}

	t.Run("snapshot does not exist", func(t *testing.T) {
		runners.requireTaskFailed(
			t,
			runners.scheduleTask(t, "nonexistent"),
			"snapshot with id nonexistent is not found",
		)
	})

	t.Run("snapshot is being created", func(t *testing.T) {
		params := checksumTestSnapshot{id: "creating"}

		_, err := f.storage.CreateSnapshot(
			f.ctx,
			snapshot_storage.SnapshotMeta{ID: params.id},
		)
		require.NoError(t, err)

		f.writeChunks(t, params, writtenChunks)

		runners.requireTaskFailed(
			t,
			runners.scheduleTask(t, params.id),
			"snapshot with id creating is not ready",
		)
	})

	t.Run("snapshot is being deleted", func(t *testing.T) {
		params := checksumTestSnapshot{
			id:         "deleting",
			chunkCount: 1,
			chunks:     writtenChunks,
		}

		f.createSnapshot(t, params)

		_, err := f.storage.DeletingSnapshot(f.ctx, params.id, "delete")
		require.NoError(t, err)

		runners.requireTaskFailed(
			t,
			runners.scheduleTask(t, params.id),
			"snapshot with id deleting is not ready",
		)
	})
}

// Deletion of the snapshot starts when the task is reading its chunks. The
// task is able to read all of them, their data is not deleted yet, but it
// should not return the checksum of the snapshot that is going away.
func TestComputeSnapshotChecksumTaskFailsIfSnapshotIsDeletedMeanwhile(
	t *testing.T,
) {

	f := newChecksumTestFixture(t)
	// The read of chunk 1 waits, so the task is not able to finish before
	// the deletion starts.
	storage := newChecksumInterruptingStorage(f.storage, 1)
	runners := f.startRunners(t, storage)

	snapshotID := "snapshot"
	f.createSnapshot(t, checksumTestSnapshot{
		id:         snapshotID,
		chunkCount: 3,
		chunks: []dataplane_common.Chunk{
			checksumTestFilledChunk(0, 1),
			checksumTestFilledChunk(1, 2),
			checksumTestFilledChunk(2, 3),
		},
	})

	taskID := runners.scheduleTask(t, snapshotID)
	storage.waitForRead(t, 1)

	_, err := f.storage.DeletingSnapshot(f.ctx, snapshotID, "delete")
	require.NoError(t, err)

	storage.release(1)

	runners.requireTaskFailed(
		t,
		taskID,
		"snapshot with id snapshot is not ready",
	)
}

////////////////////////////////////////////////////////////////////////////////

// Control plane does not execute the task, but it waits for its response and
// shows its progress. So its nodes should know the task, and the task that has
// nothing but the saved state should be able to return them.
func TestComputeSnapshotChecksumTaskIsRegistered(t *testing.T) {
	ctx := test.NewContext()

	registry := tasks.NewRegistry()
	err := Register(ctx, registry)
	require.NoError(t, err)
	require.Contains(t, registry.TaskTypes(), checksumTestTaskType)

	task, err := registry.NewTask(checksumTestTaskType)
	require.NoError(t, err)

	request, err := proto.Marshal(&protos.ComputeSnapshotChecksumRequest{
		SnapshotId: "snapshot",
	})
	require.NoError(t, err)

	state, err := proto.Marshal(&protos.ComputeSnapshotChecksumTaskState{
		NextChunkIndex: 5,
		Checksum:       12345,
		ChunkCount:     10,
		Progress:       0.5,
	})
	require.NoError(t, err)

	err = task.Load(request, state)
	require.NoError(t, err)

	require.True(t, proto.Equal(
		&protos.ComputeSnapshotChecksumResponse{Checksum: 12345},
		task.GetResponse(),
	))

	metadata, err := task.GetMetadata(ctx)
	require.NoError(t, err)
	require.True(t, proto.Equal(
		&protos.ComputeSnapshotChecksumMetadata{Progress: 0.5},
		metadata,
	))
}
