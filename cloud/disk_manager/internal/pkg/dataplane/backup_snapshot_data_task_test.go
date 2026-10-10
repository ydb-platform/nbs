package dataplane

import (
	"context"
	"math"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

const backupTestBucket = "chunks-backup"

type testFollower struct {
	s3           *persistence.S3Client
	backupS3     *backup.S3
	encryptedDEK []byte
}

func newTestFollower(t *testing.T, ctx context.Context) testFollower {
	s3, err := test.NewS3Client()
	require.NoError(t, err)

	exists, err := s3.BucketExists(ctx, backupTestBucket)
	require.NoError(t, err)
	if !exists {
		err = s3.CreateBucket(ctx, backupTestBucket)
		require.NoError(t, err)
	}

	backupS3, err := backup.NewS3(
		s3,
		backupTestBucket,
		t.Name(),
		"kek1",
		make([]byte, 32),
		0, // uploadBytesPerSecond
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	encryptedDEK, err := backupS3.NewEncryptedDEK()
	require.NoError(t, err)

	return testFollower{
		s3:           s3,
		backupS3:     backupS3,
		encryptedDEK: encryptedDEK,
	}
}

func (f testFollower) getObject(
	ctx context.Context,
	key string,
) (persistence.S3Object, error) {

	return f.backupS3.GetObject(ctx, key)
}

func (f testFollower) getRawObject(
	ctx context.Context,
	key string,
) (persistence.S3Object, error) {

	return f.s3.GetObject(ctx, backupTestBucket, f.backupS3.Key(key))
}

func newBackupSnapshotDataTask(
	storage snapshot_storage.Storage,
	follower testFollower,
	snapshotID string,
) *backupSnapshotDataTask {

	return &backupSnapshotDataTask{
		storage:   storage,
		backupS3:  follower.backupS3,
		batchSize: 1000,
		request: &protos.BackupSnapshotDataRequest{
			SnapshotId:   snapshotID,
			EncryptedDek: follower.encryptedDEK,
			MetaKey:      backupTestMetaKey(snapshotID),
			Meta:         backupTestMeta(snapshotID),
		},
		state: &protos.BackupSnapshotDataTaskState{},
	}
}

func backupTestMetaKey(snapshotID string) string {
	return backup.SnapshotMetaKey("disk", snapshotID)
}

func backupTestMeta(snapshotID string) []byte {
	return []byte(`{"id":"` + snapshotID + `"}`)
}

func requireBackupMeta(
	t *testing.T,
	ctx context.Context,
	follower testFollower,
	snapshotID string,
) {

	key := backupTestMetaKey(snapshotID)
	object, err := follower.getObject(ctx, key)
	require.NoError(t, err)
	require.Equal(t, backupTestMeta(snapshotID), object.Data)

	raw, err := follower.getRawObject(ctx, key)
	require.NoError(t, err)
	require.NotEqual(t, object.Data, raw.Data)
}

func requireNoBackupMeta(
	t *testing.T,
	ctx context.Context,
	follower testFollower,
	snapshotID string,
) {

	_, err := follower.getObject(ctx, backupTestMetaKey(snapshotID))
	require.Error(t, err)
}

func newBackupExecutionContext(
	ctx context.Context,
) *mocks.ExecutionContextMock {

	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("SaveState", ctx).Return(nil)
	execCtx.On("GetTaskID").Return("backup")
	return execCtx
}

func enqueueBackupChunks(
	ctx context.Context,
	storage snapshot_storage.Storage,
	entries []snapshot_storage.BackupChunkQueueEntry,
) error {

	for _, entry := range entries {
		err := storage.EnqueueBackupChunks(
			ctx,
			entry.SnapshotID,
			[]snapshot_storage.BackupChunkQueueEntry{entry},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// Reads the whole backup chunk queue.
func queuedBackupChunks(
	ctx context.Context,
	storage snapshot_storage.Storage,
) ([]snapshot_storage.BackupChunkQueueEntry, error) {

	return storage.GetQueuedChunksToBackup(
		ctx,
		0,
		math.MaxUint64,
		nil, // after
		10,  // limit
	)
}

func createSnapshotWithChunk(
	t *testing.T,
	ctx context.Context,
	storage snapshot_storage.Storage,
	snapshotID string,
) string {

	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: snapshotID},
	)
	require.NoError(t, err)

	chunkID, err := storage.WriteChunk(
		ctx,
		"task",
		snapshotID,
		dataplane_common.Chunk{Index: 0, Data: []byte("abc")},
		true, // useS3
	)
	require.NoError(t, err)

	err = storage.SnapshotCreated(
		ctx,
		snapshotID,
		4096, // size
		4096, // storageSize
		1,    // chunkCount
		nil,  // encryption
	)
	require.NoError(t, err)

	return chunkID
}

// Deletes the snapshot the way dataplane.DeleteSnapshot does: its own chunk
// references go away, chunks referenced by other snapshots stay.
func finishSnapshotDeletion(
	t *testing.T,
	ctx context.Context,
	storage snapshot_storage.Storage,
	snapshotID string,
) {

	_, err := storage.DeletingSnapshot(ctx, snapshotID, "delete")
	require.NoError(t, err)

	err = storage.DeleteSnapshotData(ctx, snapshotID)
	require.NoError(t, err)
}

func readBackupChunkMap(
	t *testing.T,
	ctx context.Context,
	follower testFollower,
	snapshotID string,
) *protos.BackupChunkMap {

	object, err := follower.getObject(ctx, backup.ChunkMapKey(snapshotID))
	require.NoError(t, err)

	chunkMap := &protos.BackupChunkMap{}
	err = proto.Unmarshal(object.Data, chunkMap)
	require.NoError(t, err)

	raw, err := follower.getRawObject(ctx, backup.ChunkMapKey(snapshotID))
	require.NoError(t, err)
	require.NotEqual(t, object.Data, raw.Data)
	require.Equal(t, "kek1", *raw.Metadata["Key-Id"])

	return chunkMap
}

////////////////////////////////////////////////////////////////////////////////

func TestBackupSnapshotDataTask(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)

	disk := &types.Disk{ZoneId: "zone", DiskId: "disk1"}
	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "snap1", Disk: disk},
	)
	require.NoError(t, err)

	chunk0, err := storage.WriteChunk(
		ctx,
		"task",
		"snap1",
		dataplane_common.Chunk{Index: 0, Data: []byte("abc")},
		true, // useS3
	)
	require.NoError(t, err)

	_, err = storage.WriteChunk(
		ctx,
		"task",
		"snap1",
		dataplane_common.Chunk{Index: 1, Zero: true},
		true, // useS3
	)
	require.NoError(t, err)

	err = storage.SnapshotCreated(
		ctx,
		"snap1",
		8192, // size
		4096, // storageSize
		2,    // chunkCount
		nil,  // encryption
	)
	require.NoError(t, err)

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "snap1")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	// The copy holds the snapshot.
	_, err = storage.DeletingSnapshot(ctx, "snap1", "delete")
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	_, err = follower.getObject(ctx, backup.ChunkMapKey("snap1"))
	require.Error(t, err)

	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Equal(
		t,
		[]snapshot_storage.BackupChunkQueueEntry{
			{
				SnapshotID:   "snap1",
				ChunkID:      chunk0,
				StoredInS3:   true,
				EncryptedDEK: follower.encryptedDEK,
			},
		},
		queue,
	)

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	// The meta is written only after the chunks and the chunk map.
	requireNoBackupMeta(t, ctx, follower, "snap1")

	err = storage.ChunksBackupCompleted(ctx, queue)
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	chunkMap := readBackupChunkMap(t, ctx, follower, "snap1")
	require.Equal(t, []string{chunk0, ""}, chunkMap.ChunkIds)
	requireBackupMeta(t, ctx, follower, "snap1")

	cleared, err := storage.ClearBackupChunks(ctx, "snap1", 10)
	require.NoError(t, err)
	require.Zero(t, cleared)

	// A finished copy restarted after its state was saved only cleans up.
	state, err := task.Save()
	require.NoError(t, err)
	request, err := proto.Marshal(task.request)
	require.NoError(t, err)
	resumed := newBackupSnapshotDataTask(storage, follower, "snap1")
	require.NoError(t, resumed.Load(request, state))

	err = resumed.Run(ctx, execCtx)
	require.NoError(t, err)

	_, err = storage.DeletingSnapshot(ctx, "snap1", "delete")
	require.NoError(t, err)
}

func TestBackupSnapshotDataTaskEnqueuesInBatches(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)

	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "snap1"},
	)
	require.NoError(t, err)

	chunk0, err := storage.WriteChunk(
		ctx,
		"task",
		"snap1",
		dataplane_common.Chunk{Index: 0, Data: []byte("abc")},
		true, // useS3
	)
	require.NoError(t, err)

	chunk1, err := storage.WriteChunk(
		ctx,
		"task",
		"snap1",
		dataplane_common.Chunk{Index: 1, Data: []byte("def")},
		true, // useS3
	)
	require.NoError(t, err)

	err = storage.SnapshotCreated(
		ctx,
		"snap1",
		8192, // size
		8192, // storageSize
		2,    // chunkCount
		nil,  // encryption
	)
	require.NoError(t, err)

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "snap1")
	task.batchSize = 1

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.EqualValues(t, 2, task.state.MilestoneChunkIndex)

	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]snapshot_storage.BackupChunkQueueEntry{
			{
				SnapshotID:   "snap1",
				ChunkID:      chunk0,
				StoredInS3:   true,
				EncryptedDEK: follower.encryptedDEK,
			},
			{
				SnapshotID:   "snap1",
				ChunkID:      chunk1,
				StoredInS3:   true,
				EncryptedDEK: follower.encryptedDEK,
			},
		},
		queue,
	)

	err = storage.ChunksBackupCompleted(ctx, queue[:1])
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	err = storage.ChunksBackupCompleted(ctx, queue[1:])
	require.NoError(t, err)

	resumed := newBackupSnapshotDataTask(storage, follower, "snap1")
	resumed.batchSize = 1
	resumed.state.MilestoneChunkIndex = 1

	err = resumed.Run(ctx, execCtx)
	require.NoError(t, err)

	queue, err = queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Empty(t, queue)

	cleared, err := storage.ClearBackupChunks(ctx, "snap1", 10)
	require.NoError(t, err)
	require.Zero(t, cleared)
}

func TestBackupSnapshotDataTaskBacksUpChunkStoredInYDB(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)

	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "snap1"},
	)
	require.NoError(t, err)

	chunkID, err := storage.WriteChunk(
		ctx,
		"task",
		"snap1",
		dataplane_common.Chunk{Index: 0, Data: []byte("abc")},
		false, // useS3
	)
	require.NoError(t, err)

	err = storage.SnapshotCreated(
		ctx,
		"snap1",
		4096, // size
		4096, // storageSize
		1,    // chunkCount
		nil,  // encryption
	)
	require.NoError(t, err)

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "snap1")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Equal(
		t,
		[]snapshot_storage.BackupChunkQueueEntry{
			{
				SnapshotID:   "snap1",
				ChunkID:      chunkID,
				EncryptedDEK: follower.encryptedDEK,
			},
		},
		queue,
	)

	chunksTask := newBackupChunksTask(storage, follower)
	err = chunksTask.Run(ctx, newBackupExecutionContext(ctx))
	require.NoError(t, err)

	chunkBlob, err := storage.ReadChunkBlob(
		ctx,
		chunkID,
		false, // storedInS3
	)
	require.NoError(t, err)

	object, err := follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)
	require.Equal(t, chunkBlob.Data, object.Data)
	require.Equal(
		t,
		*chunks.NewS3Object(chunkBlob).Metadata["Checksum"],
		*object.Metadata["Checksum"],
	)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	chunkMap := readBackupChunkMap(t, ctx, follower, "snap1")
	require.Equal(t, []string{chunkID}, chunkMap.ChunkIds)
}

func TestBackupSnapshotDataTaskFailsOnChunkIndexOutOfRange(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)

	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "snap1"},
	)
	require.NoError(t, err)

	for index := uint32(0); index < 2; index++ {
		_, err = storage.WriteChunk(
			ctx,
			"task",
			"snap1",
			dataplane_common.Chunk{Index: index, Data: []byte("abc")},
			true, // useS3
		)
		require.NoError(t, err)
	}

	err = storage.SnapshotCreated(
		ctx,
		"snap1",
		4096, // size
		4096, // storageSize
		1,    // chunkCount
		nil,  // encryption
	)
	require.NoError(t, err)

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "snap1")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	_, err = follower.getObject(ctx, backup.ChunkMapKey("snap1"))
	require.Error(t, err)

	_, err = storage.DeletingSnapshot(ctx, "snap1", "delete")
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.NoError(t, task.Cancel(ctx, execCtx))
	_, err = storage.DeletingSnapshot(ctx, "snap1", "delete")
	require.NoError(t, err)
}

func TestBackupSnapshotDataTaskBacksUpShallowCopyWithoutCreatorBackup(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunk0 := createSnapshotWithChunk(t, ctx, storage, "snap1")

	// The source has never been backed up. The image must copy its data itself.
	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "image1"},
	)
	require.NoError(t, err)

	err = storage.ShallowCopyChunk(
		ctx,
		snapshot_storage.ChunkMapEntry{
			ChunkIndex: 0,
			ChunkID:    chunk0,
			StoredInS3: true,
		},
		"image1",
	)
	require.NoError(t, err)

	err = storage.SnapshotCreated(
		ctx,
		"image1",
		4096, // size
		4096, // storageSize
		1,    // chunkCount
		nil,  // encryption
	)
	require.NoError(t, err)

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "image1")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	workerCtx := mocks.NewExecutionContextMock()
	err = newBackupChunksTask(storage, follower).Run(ctx, workerCtx)
	require.NoError(t, err)
	require.NoError(t, task.Run(ctx, execCtx))
	_, err = follower.getObject(ctx, backup.ChunkKey(chunk0))
	require.NoError(t, err)

	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Empty(t, queue)

	chunkMap := readBackupChunkMap(t, ctx, follower, "image1")
	require.Equal(t, []string{chunk0}, chunkMap.ChunkIds)
}

func TestBackupSnapshotDataTaskSkipsDeletedSnapshot(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	createSnapshotWithChunk(t, ctx, storage, "snap1")

	_, err := storage.DeletingSnapshot(ctx, "snap1", "deleteTaskID")
	require.NoError(t, err)

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "snap1")

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Empty(t, queue)

	// Nothing is written: the deletion would not wait for the copy.
	_, err = follower.getObject(ctx, backup.ChunkMapKey("snap1"))
	require.Error(t, err)
	requireNoBackupMeta(t, ctx, follower, "snap1")
}

func TestBackupSnapshotDataTaskRejectsRequestWithoutMeta(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	createSnapshotWithChunk(t, ctx, storage, "snap1")

	task := newBackupSnapshotDataTask(storage, follower, "snap1")
	task.request.MetaKey = ""

	err := task.Run(ctx, newBackupExecutionContext(ctx))
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	// The request is rejected before the hold.
	_, err = storage.DeletingSnapshot(ctx, "snap1", "delete")
	require.NoError(t, err)
}

func TestBackupSnapshotDataTaskEnqueuesNothingOnBadDEK(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	createSnapshotWithChunk(t, ctx, storage, "snap1")

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "snap1")
	task.request.EncryptedDek = []byte("bad")

	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.Zero(t, task.state.MilestoneChunkIndex)

	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Empty(t, queue)

	// The failed copy keeps its reference until its Cancel runs.
	_, err = storage.DeletingSnapshot(ctx, "snap1", "delete")
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.NoError(t, task.Cancel(ctx, execCtx))
	_, err = storage.DeletingSnapshot(ctx, "snap1", "delete")
	require.NoError(t, err)
}

func TestBackupSnapshotDataTaskCancellationClearsQueue(t *testing.T) {
	ctx := test.NewContext()
	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()
	follower := newTestFollower(t, ctx)
	createSnapshotWithChunk(t, ctx, storage, "snap1")
	task := newBackupSnapshotDataTask(storage, follower, "snap1")
	execCtx := newBackupExecutionContext(ctx)

	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Len(t, queue, 1)

	require.NoError(t, task.Cancel(ctx, execCtx))
	queue, err = queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Empty(t, queue)
	cleared, err := storage.ClearBackupChunks(ctx, "snap1", 10)
	require.NoError(t, err)
	require.Zero(t, cleared)

	// The next attempt enqueues the chunk again.
	next := newBackupSnapshotDataTask(storage, follower, "snap1")
	err = next.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	queue, err = queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Len(t, queue, 1)
}

func TestBackupSnapshotDataTaskNextAttemptWaitsForCancelledCopy(t *testing.T) {
	ctx := test.NewContext()
	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()
	follower := newTestFollower(t, ctx)
	createSnapshotWithChunk(t, ctx, storage, "snap1")

	cancelled := newBackupSnapshotDataTask(storage, follower, "snap1")
	cancelledCtx := newBackupExecutionContext(ctx)
	err := cancelled.Run(ctx, cancelledCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	// The next attempt cannot start while the cancelled copy holds the
	// snapshot, so it never sees the chunks being cleared.
	next := newBackupSnapshotDataTask(storage, follower, "snap1")
	nextCtx := mocks.NewExecutionContextMock()
	nextCtx.On("SaveState", ctx).Return(nil)
	nextCtx.On("GetTaskID").Return("next")
	err = next.Run(ctx, nextCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.Zero(t, next.state.MilestoneChunkIndex)

	require.NoError(t, cancelled.Cancel(ctx, cancelledCtx))
	err = next.Run(ctx, nextCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Len(t, queue, 1)
}

type failingBackupChunkStorage struct {
	snapshot_storage.Storage
}

func (s failingBackupChunkStorage) ReadChunkBlob(
	ctx context.Context,
	chunkID string,
	storedInS3 bool,
) (chunks.ChunkBlob, error) {

	return chunks.ChunkBlob{}, errors.NewNonRetriableErrorf("source chunk is corrupt")
}

func TestBackupSnapshotDataTaskWaitsForChunkRetriedByAnotherWorker(t *testing.T) {
	ctx := test.NewContext()
	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()
	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snapshot")
	task := newBackupSnapshotDataTask(storage, follower, "snapshot")
	execCtx := newBackupExecutionContext(ctx)
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewInterruptExecutionError()))

	workerCtx := mocks.NewExecutionContextMock()
	worker := newBackupChunksTask(failingBackupChunkStorage{Storage: storage}, follower)
	require.True(t, errors.Is(worker.Run(ctx, workerCtx), errors.NewEmptyNonRetriableError()))

	// The chunk stays queued; the copy waits instead of failing.
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewInterruptExecutionError()))
	_, err := follower.getObject(ctx, backup.ChunkMapKey("snapshot"))
	require.Error(t, err)

	err = newBackupChunksTask(storage, follower).Run(ctx, workerCtx)
	require.NoError(t, err)
	require.NoError(t, task.Run(ctx, execCtx))
	require.Equal(t, []string{chunkID}, readBackupChunkMap(t, ctx, follower, "snapshot").ChunkIds)
}

// Fails reading a chunk, so the test sees which chunks a copy reads.
type unreadableChunkStorage struct {
	snapshot_storage.Storage
	chunkID string
}

func (s unreadableChunkStorage) ReadChunkBlob(
	ctx context.Context,
	chunkID string,
	storedInS3 bool,
) (chunks.ChunkBlob, error) {

	if chunkID == s.chunkID {
		return chunks.ChunkBlob{}, errors.NewNonRetriableErrorf(
			"chunk already in the follower must not be read",
		)
	}
	return s.Storage.ReadChunkBlob(ctx, chunkID, storedInS3)
}

func backUpSnapshot(
	t *testing.T,
	ctx context.Context,
	storage snapshot_storage.Storage,
	follower testFollower,
	snapshotID string,
) []snapshot_storage.BackupChunkQueueEntry {

	task := newBackupSnapshotDataTask(storage, follower, snapshotID)
	execCtx := newBackupExecutionContext(ctx)
	workerCtx := mocks.NewExecutionContextMock()

	err := task.Run(ctx, execCtx)
	if err == nil {
		return nil
	}
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)

	err = newBackupChunksTask(storage, follower).Run(ctx, workerCtx)
	require.NoError(t, err)
	require.NoError(t, task.Run(ctx, execCtx))
	return queue
}

func createSnapshotFromChunks(
	t *testing.T,
	ctx context.Context,
	storage snapshot_storage.Storage,
	snapshotID string,
	ownData string,
	chunkIDs ...string,
) string {

	_, err := storage.CreateSnapshot(ctx, snapshot_storage.SnapshotMeta{ID: snapshotID})
	require.NoError(t, err)

	for index, chunkID := range chunkIDs {
		err = storage.ShallowCopyChunk(
			ctx,
			snapshot_storage.ChunkMapEntry{
				ChunkIndex: uint32(index),
				ChunkID:    chunkID,
				StoredInS3: true,
			},
			snapshotID,
		)
		require.NoError(t, err)
	}

	own, err := storage.WriteChunk(
		ctx,
		"task",
		snapshotID,
		dataplane_common.Chunk{Index: uint32(len(chunkIDs)), Data: []byte(ownData)},
		true, // useS3
	)
	require.NoError(t, err)

	count := uint32(len(chunkIDs) + 1)
	size := uint64(count) * 4096
	err = storage.SnapshotCreated(
		ctx,
		snapshotID,
		size,
		size, // storageSize
		count,
		nil, // encryption
	)
	require.NoError(t, err)
	return own
}

func TestBackupSnapshotDataTaskSkipsChunksCopiedByCreator(t *testing.T) {
	ctx := test.NewContext()
	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()
	follower := newTestFollower(t, ctx)

	a := createSnapshotWithChunk(t, ctx, storage, "A")
	backUpSnapshot(t, ctx, storage, follower, "A")

	own := createSnapshotFromChunks(t, ctx, storage, "C", "own", a)
	guarded := unreadableChunkStorage{Storage: storage, chunkID: a}
	queue := backUpSnapshot(t, ctx, guarded, follower, "C")

	require.Len(t, queue, 1)
	require.Equal(t, own, queue[0].ChunkID)
	require.Equal(t, []string{a, own}, readBackupChunkMap(t, ctx, follower, "C").ChunkIds)
}

func TestBackupSnapshotDataTaskCopiesChunkOfDeletedCreatorOnce(t *testing.T) {
	ctx := test.NewContext()
	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()
	follower := newTestFollower(t, ctx)

	// X created chunk a; S1 and S2 both use it, and X is gone.
	a := createSnapshotWithChunk(t, ctx, storage, "X")
	createSnapshotFromChunks(t, ctx, storage, "S1", "b", a)
	own := createSnapshotFromChunks(t, ctx, storage, "S2", "d", a)
	finishSnapshotDeletion(t, ctx, storage, "X")

	queue := backUpSnapshot(t, ctx, storage, follower, "S1")
	require.Len(t, queue, 2)

	guarded := unreadableChunkStorage{Storage: storage, chunkID: a}
	queue = backUpSnapshot(t, ctx, guarded, follower, "S2")
	require.Len(t, queue, 1)
	require.Equal(t, own, queue[0].ChunkID)
	require.Equal(t, []string{a, own}, readBackupChunkMap(t, ctx, follower, "S2").ChunkIds)
}
