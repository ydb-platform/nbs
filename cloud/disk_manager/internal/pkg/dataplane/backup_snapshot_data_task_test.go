package dataplane

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
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
		},
		state: &protos.BackupSnapshotDataTaskState{},
	}
}

func newBackupExecutionContext(
	ctx context.Context,
) *mocks.ExecutionContextMock {

	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("SaveState", ctx).Return(nil)
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

	_, err = follower.getObject(ctx, backup.ChunkMapKey("snap1"))
	require.Error(t, err)

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
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
	require.EqualValues(t, 1, task.state.EnqueuedChunkCount)

	err = storage.ChunksBackupCompleted(ctx, queue)
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	chunkMap := readBackupChunkMap(t, ctx, follower, "snap1")
	require.Equal(t, []string{chunk0, ""}, chunkMap.ChunkIds)

	cleared, err := storage.ClearCompletedBackupChunks(ctx, "snap1", 10)
	require.NoError(t, err)
	require.Zero(t, cleared)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
}

func TestBackupSnapshotDataTaskEnqueuesOnlyOwnChunks(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)

	disk := &types.Disk{ZoneId: "zone", DiskId: "disk1"}
	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{
			ID:           "snap1",
			Disk:         disk,
			CheckpointID: "checkpoint1",
		},
	)
	require.NoError(t, err)

	chunk0, err := storage.WriteChunk(
		ctx,
		"task1",
		"snap1",
		dataplane_common.Chunk{Index: 0, Data: []byte("abc")},
		true, // useS3
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

	snap2, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{
			ID:           "snap2",
			Disk:         disk,
			CheckpointID: "checkpoint2",
		},
	)
	require.NoError(t, err)
	require.Equal(t, "snap1", snap2.BaseSnapshotID)

	err = storage.ShallowCopyChunk(
		ctx,
		snapshot_storage.ChunkMapEntry{
			ChunkIndex: 0,
			ChunkID:    chunk0,
			StoredInS3: true,
		},
		"snap2",
	)
	require.NoError(t, err)

	chunk1, err := storage.WriteChunk(
		ctx,
		"task2",
		"snap2",
		dataplane_common.Chunk{Index: 1, Data: []byte("def")},
		true, // useS3
	)
	require.NoError(t, err)

	err = storage.SnapshotCreated(
		ctx,
		"snap2",
		8192, // size
		8192, // storageSize
		2,    // chunkCount
		nil,  // encryption
	)
	require.NoError(t, err)

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "snap2")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(
		t,
		[]snapshot_storage.BackupChunkQueueEntry{
			{
				SnapshotID:   "snap2",
				ChunkID:      chunk1,
				StoredInS3:   true,
				EncryptedDEK: follower.encryptedDEK,
			},
		},
		queue,
	)

	err = storage.ChunksBackupCompleted(ctx, queue)
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	chunkMap := readBackupChunkMap(t, ctx, follower, "snap2")
	require.Equal(t, []string{chunk0, chunk1}, chunkMap.ChunkIds)
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
	require.EqualValues(t, 2, task.state.EnqueuedChunkCount)
	require.EqualValues(t, 2, task.state.MilestoneChunkIndex)

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
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
	resumed.state.EnqueuedChunkCount = 1

	err = resumed.Run(ctx, execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 2, resumed.state.EnqueuedChunkCount)

	queue, err = storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)

	completed, err := storage.GetBackedUpChunkCount(ctx, "snap1")
	require.NoError(t, err)
	require.Zero(t, completed)
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

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
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
	err = chunksTask.Run(ctx, mocks.NewExecutionContextMock())
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

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
}

func TestBackupSnapshotDataTaskEnqueuesNothingForShallowCopy(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunk0 := createSnapshotWithChunk(t, ctx, storage, "snap1")

	// An image made from a snapshot owns no chunks, all of them are copied by
	// the backup of that snapshot.
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
	require.NoError(t, err)

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)

	chunkMap := readBackupChunkMap(t, ctx, follower, "image1")
	require.Equal(t, []string{chunk0}, chunkMap.ChunkIds)
}

func TestBackupSnapshotDataTaskFailsOnDeletedSnapshot(t *testing.T) {
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
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}

func TestBackupSnapshotDataTaskRejectsBadDEK(t *testing.T) {
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
	require.Zero(t, task.state.EnqueuedChunkCount)

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}
