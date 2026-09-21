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
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

const backupTestBucket = "chunks-backup"

type testFollower struct {
	s3         *persistence.S3Client
	followerS3 *backup.FollowerS3
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

	return testFollower{
		s3:         s3,
		followerS3: backup.NewFollowerS3(s3, backupTestBucket, t.Name()),
	}
}

func (f testFollower) getObject(
	ctx context.Context,
	key string,
) (persistence.S3Object, error) {

	return f.s3.GetObject(ctx, backupTestBucket, f.followerS3.Key(key))
}

func newBackupSnapshotChunksTask(
	storage snapshot_storage.Storage,
	follower testFollower,
	snapshotID string,
) *backupSnapshotChunksTask {

	return &backupSnapshotChunksTask{
		storage:    storage,
		followerS3: follower.followerS3,
		batchSize:  1000,
		request: &protos.BackupSnapshotChunksRequest{
			SnapshotId: snapshotID,
		},
		state: &protos.BackupSnapshotChunksTaskState{},
	}
}

func newBackupExecutionContext(
	ctx context.Context,
) *mocks.ExecutionContextMock {

	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("SaveState", ctx).Return(nil)
	return execCtx
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

	return chunkMap
}

////////////////////////////////////////////////////////////////////////////////

func TestBackupSnapshotChunksTask(t *testing.T) {
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
	task := newBackupSnapshotChunksTask(storage, follower, "snap1")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	_, err = follower.getObject(ctx, backup.ChunkMapKey("snap1"))
	require.Error(t, err)

	queue, err := storage.GetBackupChunkQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(
		t,
		[]snapshot_storage.BackupChunkQueueEntry{
			{SnapshotID: "snap1", ChunkID: chunk0},
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
}

func TestBackupSnapshotChunksTaskEnqueuesOnlyOwnChunks(t *testing.T) {
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
	task := newBackupSnapshotChunksTask(storage, follower, "snap2")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	queue, err := storage.GetBackupChunkQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(
		t,
		[]snapshot_storage.BackupChunkQueueEntry{
			{SnapshotID: "snap2", ChunkID: chunk1},
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

func TestBackupSnapshotChunksTaskEnqueuesInBatches(t *testing.T) {
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
	task := newBackupSnapshotChunksTask(storage, follower, "snap1")
	task.batchSize = 1

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.EqualValues(t, 2, task.state.EnqueuedChunkCount)
	require.EqualValues(t, 2, task.state.MilestoneChunkIndex)

	queue, err := storage.GetBackupChunkQueue(ctx, 10)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]snapshot_storage.BackupChunkQueueEntry{
			{SnapshotID: "snap1", ChunkID: chunk0},
			{SnapshotID: "snap1", ChunkID: chunk1},
		},
		queue,
	)

	err = storage.ChunksBackupCompleted(ctx, queue)
	require.NoError(t, err)

	resumed := newBackupSnapshotChunksTask(storage, follower, "snap1")
	resumed.batchSize = 1
	resumed.state.MilestoneChunkIndex = 1
	resumed.state.EnqueuedChunkCount = 1

	err = resumed.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.EqualValues(t, 2, resumed.state.EnqueuedChunkCount)

	queue, err = storage.GetBackupChunkQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(
		t,
		[]snapshot_storage.BackupChunkQueueEntry{
			{SnapshotID: "snap1", ChunkID: chunk1},
		},
		queue,
	)
}

func TestBackupSnapshotChunksTaskFailsOnChunkStoredInYDB(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)

	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "snap1"},
	)
	require.NoError(t, err)

	_, err = storage.WriteChunk(
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
	task := newBackupSnapshotChunksTask(storage, follower, "snap1")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	queue, err := storage.GetBackupChunkQueue(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}

func TestBackupSnapshotChunksTaskFailsOnChunkIndexOutOfRange(t *testing.T) {
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
	task := newBackupSnapshotChunksTask(storage, follower, "snap1")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	_, err = follower.getObject(ctx, backup.ChunkMapKey("snap1"))
	require.Error(t, err)
}

func TestBackupSnapshotChunksTaskEnqueuesNothingForShallowCopy(t *testing.T) {
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
	task := newBackupSnapshotChunksTask(storage, follower, "image1")

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	queue, err := storage.GetBackupChunkQueue(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)

	chunkMap := readBackupChunkMap(t, ctx, follower, "image1")
	require.Equal(t, []string{chunk0}, chunkMap.ChunkIds)
}

func TestBackupSnapshotChunksTaskFailsOnDeletedSnapshot(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	createSnapshotWithChunk(t, ctx, storage, "snap1")

	_, err := storage.DeletingSnapshot(ctx, "snap1", "deleteTaskID")
	require.NoError(t, err)

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotChunksTask(storage, follower, "snap1")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	queue, err := storage.GetBackupChunkQueue(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}
