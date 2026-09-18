package dataplane

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
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

const backupTestBucket = "backup"

type testBackup struct {
	s3        *persistence.S3Client
	bucket    string
	keyPrefix string
}

func newTestBackup(t *testing.T, ctx context.Context) testBackup {
	s3, err := test.NewS3Client()
	require.NoError(t, err)

	exists, err := s3.BucketExists(ctx, backupTestBucket)
	require.NoError(t, err)

	if !exists {
		err = s3.CreateBucket(ctx, backupTestBucket)
		if err != nil {
			// Another test may have created the bucket in the meantime.
			exists, existsErr := s3.BucketExists(ctx, backupTestBucket)
			require.NoError(t, existsErr)
			require.True(t, exists, "failed to create bucket: %v", err)
		}
	}

	return testBackup{
		s3:        s3,
		bucket:    backupTestBucket,
		keyPrefix: t.Name(),
	}
}

func (b testBackup) getObject(
	ctx context.Context,
	object string,
) (persistence.S3Object, error) {

	return b.s3.GetObject(
		ctx,
		b.bucket,
		fmt.Sprintf("%v/%v", b.keyPrefix, object),
	)
}

func newBackupSnapshotTask(
	storage snapshot_storage.Storage,
	dst testBackup,
	snapshotID string,
) *backupSnapshotTask {

	return &backupSnapshotTask{
		storage:          storage,
		s3:               dst.s3,
		bucket:           dst.bucket,
		keyPrefix:        dst.keyPrefix,
		enqueueBatchSize: 1000,
		request: &protos.BackupSnapshotRequest{
			SnapshotId: snapshotID,
		},
		state: &protos.BackupSnapshotTaskState{},
	}
}

func newBackupExecutionContext(
	ctx context.Context,
	taskID string,
) *mocks.ExecutionContextMock {

	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("SaveState", ctx).Return(nil)
	execCtx.On("GetTaskID").Return(taskID)
	return execCtx
}

func readBackupChunkMap(
	t *testing.T,
	ctx context.Context,
	dst testBackup,
	object string,
) *protos.BackupChunkMap {

	obj, err := dst.getObject(ctx, object)
	require.NoError(t, err)

	chunkMap := &protos.BackupChunkMap{}
	err = proto.Unmarshal(obj.Data, chunkMap)
	require.NoError(t, err)

	return chunkMap
}

////////////////////////////////////////////////////////////////////////////////

func TestBackupSnapshotTask(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	dst := newTestBackup(t, ctx)

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

	execCtx := newBackupExecutionContext(ctx, "backupTaskID")
	task := newBackupSnapshotTask(storage, dst, "snap1")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	_, err = dst.getObject(ctx, "chunk_maps/snap1")
	require.Error(t, err)

	queue, err := storage.GetBackupQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(
		t,
		[]snapshot_storage.BackupQueueEntry{
			{SnapshotID: "snap1", ChunkID: chunk0},
		},
		queue,
	)

	// Chunks are enqueued once, the task just waits for them to be copied.
	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.EqualValues(t, 1, task.state.EnqueuedChunkCount)

	err = storage.ClearBackupQueue(ctx, queue)
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 1, task.state.Progress)

	chunkMap := readBackupChunkMap(t, ctx, dst, "chunk_maps/snap1")
	require.Equal(t, []string{chunk0, ""}, chunkMap.ChunkIds)
}

func TestBackupSnapshotTaskEnqueuesOnlyOwnChunks(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	dst := newTestBackup(t, ctx)

	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "snap1"},
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

	_, err = storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "snap2"},
	)
	require.NoError(t, err)

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

	execCtx := newBackupExecutionContext(ctx, "backupTaskID")
	task := newBackupSnapshotTask(storage, dst, "snap2")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	queue, err := storage.GetBackupQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(
		t,
		[]snapshot_storage.BackupQueueEntry{
			{SnapshotID: "snap2", ChunkID: chunk1},
		},
		queue,
	)

	err = storage.ClearBackupQueue(ctx, queue)
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	// The chunk inherited from snap1 is in the map, but it is copied by the
	// backup of snap1.
	chunkMap := readBackupChunkMap(t, ctx, dst, "chunk_maps/snap2")
	require.Equal(t, []string{chunk0, chunk1}, chunkMap.ChunkIds)
}

func TestBackupSnapshotTaskHoldsSnapshotLock(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	dst := newTestBackup(t, ctx)

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

	execCtx := newBackupExecutionContext(ctx, "backupTaskID")
	task := newBackupSnapshotTask(storage, dst, "snap1")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	_, err = storage.DeletingSnapshot(ctx, "snap1", "deleteTaskID")
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	err = task.Cancel(ctx, execCtx)
	require.NoError(t, err)

	_, err = storage.DeletingSnapshot(ctx, "snap1", "deleteTaskID")
	require.NoError(t, err)
}

func TestBackupSnapshotTaskSkipsDeletedSnapshot(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	dst := newTestBackup(t, ctx)

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

	_, err = storage.DeletingSnapshot(ctx, "snap1", "deleteTaskID")
	require.NoError(t, err)

	execCtx := newBackupExecutionContext(ctx, "backupTaskID")
	task := newBackupSnapshotTask(storage, dst, "snap1")

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	queue, err := storage.GetBackupQueue(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}
