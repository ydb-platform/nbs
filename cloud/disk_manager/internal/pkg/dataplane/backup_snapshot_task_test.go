package dataplane

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

const backupTestBucket = "backup"

func newBackupTestSlave(t *testing.T, ctx context.Context) *backup.Slave {
	s3, err := test.NewS3Client()
	require.NoError(t, err)

	exists, err := s3.BucketExists(ctx, backupTestBucket)
	require.NoError(t, err)

	if !exists {
		err = s3.CreateBucket(ctx, backupTestBucket)
		if err != nil {
			// Another test has just created it.
			exists, existsErr := s3.BucketExists(ctx, backupTestBucket)
			require.NoError(t, existsErr)
			require.True(t, exists, "failed to create bucket: %v", err)
		}
	}

	return &backup.Slave{
		ID:        "slave",
		S3:        s3,
		Bucket:    backupTestBucket,
		KeyPrefix: t.Name(),
	}
}

func newBackupTestConfig() *config.DataplaneConfig {
	return &config.DataplaneConfig{
		SnapshotConfig: &snapshot_config.SnapshotConfig{},
		BackupConfig:   &config.BackupConfig{},
	}
}

func newBackupSnapshotTask(
	storage snapshot_storage.Storage,
	slave *backup.Slave,
	snapshotID string,
) *backupSnapshotTask {

	return &backupSnapshotTask{
		config:  newBackupTestConfig(),
		storage: storage,
		slaves:  backup.Slaves{slave.ID: slave},
		request: &protos.BackupSnapshotRequest{
			SnapshotId: snapshotID,
			FolderId:   "folder",
			Slave:      slave.ID,
		},
		state: &protos.BackupSnapshotTaskState{},
	}
}

func readBackupChunkMap(
	t *testing.T,
	ctx context.Context,
	slave *backup.Slave,
	object string,
) *protos.BackupChunkMap {

	obj, err := slave.S3.GetObject(ctx, slave.Bucket, slave.Key(object))
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

	slave := newBackupTestSlave(t, ctx)

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

	err = storage.SnapshotCreated(ctx, "snap1", 8192, 4096, 2, nil)
	require.NoError(t, err)

	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("SaveState", ctx).Return(nil)

	task := newBackupSnapshotTask(storage, slave, "snap1")

	// The chunk is still in the queue: meta is written, the task yields.
	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	obj, err := slave.S3.GetObject(
		ctx,
		slave.Bucket,
		slave.Key("snapshots/disk1/snap1/meta.json"),
	)
	require.NoError(t, err)

	var meta backup.SnapshotMeta
	err = json.Unmarshal(obj.Data, &meta)
	require.NoError(t, err)
	require.Equal(t, "snap1", meta.ID)
	require.Equal(t, "folder", meta.FolderID)
	require.Equal(t, "disk1", meta.DiskID)
	require.Equal(t, "zone", meta.ZoneID)
	require.EqualValues(t, 2, meta.ChunkCount)
	require.EqualValues(t, chunkSize, meta.ChunkSize)

	_, err = slave.S3.GetObject(
		ctx,
		slave.Bucket,
		slave.Key("snapshots/disk1/snap1/map.bin"),
	)
	require.Error(t, err)

	queue, err := storage.GetBackupQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(
		t,
		[]snapshot_storage.BackupQueueEntry{
			{SnapshotID: "snap1", ChunkID: chunk0, Slave: slave.ID},
		},
		queue,
	)

	snapshotMeta, err := storage.GetSnapshotMeta(ctx, "snap1")
	require.NoError(t, err)
	require.Equal(t, slave.ID, snapshotMeta.BackupSlave)

	// Still waiting: nothing is enqueued twice.
	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.EqualValues(t, 1, task.state.EnqueuedChunkCount)

	// Chunks are copied: the map is written.
	err = storage.ClearBackupQueue(ctx, queue)
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 1, task.state.Progress)

	chunkMap := readBackupChunkMap(t, ctx, slave, "snapshots/disk1/snap1/map.bin")
	require.EqualValues(t, 2, chunkMap.ChunkCount)
	require.Equal(t, []string{chunk0, ""}, chunkMap.ChunkIds)
}

func TestBackupSnapshotTaskEnqueuesOnlyOwnChunks(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	slave := newBackupTestSlave(t, ctx)

	_, err := storage.CreateSnapshot(ctx, snapshot_storage.SnapshotMeta{ID: "snap1"})
	require.NoError(t, err)

	chunk0, err := storage.WriteChunk(
		ctx,
		"task1",
		"snap1",
		dataplane_common.Chunk{Index: 0, Data: []byte("abc")},
		true, // useS3
	)
	require.NoError(t, err)

	err = storage.SnapshotCreated(ctx, "snap1", 4096, 4096, 1, nil)
	require.NoError(t, err)

	// snap2 inherits chunk 0 from snap1 and writes chunk 1 itself.
	_, err = storage.CreateSnapshot(ctx, snapshot_storage.SnapshotMeta{ID: "snap2"})
	require.NoError(t, err)

	err = storage.ShallowCopyChunk(
		ctx,
		snapshot_storage.ChunkMapEntry{ChunkIndex: 0, ChunkID: chunk0, StoredInS3: true},
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

	err = storage.SnapshotCreated(ctx, "snap2", 8192, 8192, 2, nil)
	require.NoError(t, err)

	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("SaveState", ctx).Return(nil)

	task := newBackupSnapshotTask(storage, slave, "snap2")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	queue, err := storage.GetBackupQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(
		t,
		[]snapshot_storage.BackupQueueEntry{
			{SnapshotID: "snap2", ChunkID: chunk1, Slave: slave.ID},
		},
		queue,
	)

	err = storage.ClearBackupQueue(ctx, queue)
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	// The map covers the whole snapshot, inherited chunks included.
	chunkMap := readBackupChunkMap(t, ctx, slave, "snapshots/-/snap2/map.bin")
	require.EqualValues(t, 2, chunkMap.ChunkCount)
	require.Equal(t, []string{chunk0, chunk1}, chunkMap.ChunkIds)
}
