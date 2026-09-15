package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/protos"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

////////////////////////////////////////////////////////////////////////////////

const (
	testChunkSize    = 4096
	backupTestBucket = "backup"
)

func newYDB(ctx context.Context) (*persistence.YDBClient, error) {
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
	)
	database := "/Root"
	connectionTimeout := "10s"

	return persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint:          &endpoint,
			Database:          &database,
			ConnectionTimeout: &connectionTimeout,
		},
		metrics.NewEmptyRegistry(),
	)
}

// Keeps chunks in bucket "test" under prefix t.Name().
func newStorage(
	t *testing.T,
	ctx context.Context,
) (snapshot_storage.Storage, func()) {

	db, err := newYDB(ctx)
	require.NoError(t, err)

	closeFunc := func() {
		_ = db.Close(ctx)
	}

	storageFolder := fmt.Sprintf("backup_tasks_tests/%v", t.Name())
	deleteWorkerCount := uint32(10)
	shallowCopyWorkerCount := uint32(11)
	shallowCopyInflightLimit := uint32(22)
	shardCount := uint64(2)
	s3Bucket := "test"
	chunkBlobsS3KeyPrefix := t.Name()

	config := &snapshot_config.SnapshotConfig{
		StorageFolder:             &storageFolder,
		DeleteWorkerCount:         &deleteWorkerCount,
		ShallowCopyWorkerCount:    &shallowCopyWorkerCount,
		ShallowCopyInflightLimit:  &shallowCopyInflightLimit,
		ChunkBlobsTableShardCount: &shardCount,
		ChunkMapTableShardCount:   &shardCount,
		S3Bucket:                  &s3Bucket,
		ChunkBlobsS3KeyPrefix:     &chunkBlobsS3KeyPrefix,
	}

	s3, err := test.NewS3Client()
	require.NoError(t, err)

	err = schema.Create(ctx, config, db, s3, false /* dropUnusedColumns */)
	require.NoError(t, err)

	storage, err := snapshot_storage.NewStorage(
		config,
		metrics.NewEmptyRegistry(),
		db,
		s3,
	)
	require.NoError(t, err)

	return storage, closeFunc
}

// Backup bucket of the test: "backup" with prefix t.Name().
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
			// Another test has just created it.
			exists, existsErr := s3.BucketExists(ctx, backupTestBucket)
			require.NoError(t, existsErr)
			require.True(t, exists, "failed to create bucket: %v", err)
		}
	}

	return testBackup{s3: s3, bucket: backupTestBucket, keyPrefix: t.Name()}
}

func (b testBackup) getObject(
	ctx context.Context,
	object string,
) (persistence.S3Object, error) {

	return b.s3.GetObject(ctx, b.bucket, key(b.keyPrefix, object))
}

func newBackupSnapshotTask(
	storage snapshot_storage.Storage,
	backup testBackup,
	snapshotID string,
) *backupSnapshotTask {

	return &backupSnapshotTask{
		storage:          storage,
		s3:               backup.s3,
		bucket:           backup.bucket,
		keyPrefix:        backup.keyPrefix,
		chunkSize:        testChunkSize,
		chunkCompression: "lz4",
		enqueueBatchSize: 1000,
		request: &protos.BackupSnapshotRequest{
			SnapshotId: snapshotID,
			FolderId:   "folder",
		},
		state: &protos.BackupSnapshotTaskState{},
	}
}

func readBackupChunkMap(
	t *testing.T,
	ctx context.Context,
	backup testBackup,
	object string,
) *protos.BackupChunkMap {

	obj, err := backup.getObject(ctx, object)
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

	backup := newTestBackup(t, ctx)

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

	task := newBackupSnapshotTask(storage, backup, "snap1")

	// The chunk is still in the queue: meta is written, the task yields.
	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	obj, err := backup.getObject(ctx, "snapshots/disk1/snap1/meta.json")
	require.NoError(t, err)

	var meta SnapshotMeta
	err = json.Unmarshal(obj.Data, &meta)
	require.NoError(t, err)
	require.Equal(t, "snap1", meta.ID)
	require.Equal(t, "folder", meta.FolderID)
	require.Equal(t, "disk1", meta.DiskID)
	require.Equal(t, "zone", meta.ZoneID)
	require.EqualValues(t, 2, meta.ChunkCount)
	require.EqualValues(t, testChunkSize, meta.ChunkSize)
	require.Equal(t, "lz4", meta.Compression)

	_, err = backup.getObject(ctx, "snapshots/disk1/snap1/map.bin")
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

	chunkMap := readBackupChunkMap(t, ctx, backup, "snapshots/disk1/snap1/map.bin")
	require.Equal(t, []string{chunk0, ""}, chunkMap.ChunkIds)
}

func TestBackupSnapshotTaskEnqueuesOnlyOwnChunks(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	backup := newTestBackup(t, ctx)

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

	task := newBackupSnapshotTask(storage, backup, "snap2")

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

	// The map covers the whole snapshot, inherited chunks included.
	chunkMap := readBackupChunkMap(t, ctx, backup, "snapshots/-/snap2/map.bin")
	require.Equal(t, []string{chunk0, chunk1}, chunkMap.ChunkIds)
}
