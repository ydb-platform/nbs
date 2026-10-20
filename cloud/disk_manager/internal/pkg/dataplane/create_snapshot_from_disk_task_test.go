package dataplane

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/bucket"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

const snapshotTestBucket = "snapshot"

// The snapshot bucket of the tests, under the prefix of the test.
func newTestSnapshotS3(t *testing.T, ctx context.Context) *bucket.S3 {
	s3, err := test.NewS3Client()
	require.NoError(t, err)

	exists, err := s3.BucketExists(ctx, snapshotTestBucket)
	require.NoError(t, err)
	if !exists {
		err = s3.CreateBucket(ctx, snapshotTestBucket)
		require.NoError(t, err)
	}

	return bucket.NewS3(s3, snapshotTestBucket, t.Name())
}

func newCreateSnapshotFromDiskTask(
	storage snapshot_storage.Storage,
	snapshotS3 *bucket.S3,
	snapshotID string,
	chunkCount uint32,
) *createSnapshotFromDiskTask {

	return &createSnapshotFromDiskTask{
		storage:    storage,
		snapshotS3: snapshotS3,
		request: &protos.CreateSnapshotFromDiskRequest{
			DstSnapshotId: snapshotID,
			UseS3:         true,
		},
		state: &protos.CreateSnapshotFromDiskTaskState{
			ChunkCount: chunkCount,
		},
	}
}

func writeChunk(
	t *testing.T,
	ctx context.Context,
	storage snapshot_storage.Storage,
	snapshotID string,
	chunkIndex uint32,
	useS3 bool,
) string {

	chunkID, err := storage.WriteChunk(
		ctx,
		"task",
		snapshotID,
		dataplane_common.Chunk{Index: chunkIndex, Data: []byte("abc")},
		useS3,
	)
	require.NoError(t, err)

	return chunkID
}

func readMetaChunkMap(
	t *testing.T,
	ctx context.Context,
	snapshotS3 *bucket.S3,
	snapshotID string,
) *protos.BackupChunkMap {

	object, err := snapshotS3.GetObject(ctx, bucket.ChunkMapKey(snapshotID))
	require.NoError(t, err)

	chunkMap := &protos.BackupChunkMap{}
	err = proto.Unmarshal(object.Data, chunkMap)
	require.NoError(t, err)

	return chunkMap
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateSnapshotFromDiskTaskPutsChunkMap(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	snapshotS3 := newTestSnapshotS3(t, ctx)

	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "snap1"},
	)
	require.NoError(t, err)

	// Chunk 1 is a zero chunk and has no entry.
	chunk0 := writeChunk(t, ctx, storage, "snap1", 0, true /* useS3 */)
	chunk2 := writeChunk(t, ctx, storage, "snap1", 2, true /* useS3 */)

	task := newCreateSnapshotFromDiskTask(storage, snapshotS3, "snap1", 3)
	err = task.putChunkMap(ctx)
	require.NoError(t, err)

	chunkMap := readMetaChunkMap(t, ctx, snapshotS3, "snap1")
	require.Equal(t, []string{chunk0, "", chunk2}, chunkMap.ChunkIds)
}

func TestCreateSnapshotFromDiskTaskPutsNoChunkMapOfYDBSnapshot(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	snapshotS3 := newTestSnapshotS3(t, ctx)

	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "snap1"},
	)
	require.NoError(t, err)

	writeChunk(t, ctx, storage, "snap1", 0, false /* useS3 */)

	task := newCreateSnapshotFromDiskTask(storage, snapshotS3, "snap1", 1)
	task.request.UseS3 = false
	err = task.putChunkMap(ctx)
	require.NoError(t, err)

	_, err = snapshotS3.GetObject(ctx, bucket.ChunkMapKey("snap1"))
	require.Error(t, err)
}

func TestCreateSnapshotFromDiskTaskRejectsChunkOutOfRange(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	snapshotS3 := newTestSnapshotS3(t, ctx)

	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "snap1"},
	)
	require.NoError(t, err)

	writeChunk(t, ctx, storage, "snap1", 2, true /* useS3 */)

	task := newCreateSnapshotFromDiskTask(storage, snapshotS3, "snap1", 1)
	err = task.putChunkMap(ctx)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	_, err = snapshotS3.GetObject(ctx, bucket.ChunkMapKey("snap1"))
	require.Error(t, err)
}

func TestCreateSnapshotFromDiskTaskWithoutMetaS3(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "snap1"},
	)
	require.NoError(t, err)

	writeChunk(t, ctx, storage, "snap1", 0, true /* useS3 */)

	task := newCreateSnapshotFromDiskTask(storage, nil, "snap1", 1)
	err = task.putChunkMap(ctx)
	require.NoError(t, err)

	snapshotS3 := newTestSnapshotS3(t, ctx)
	_, err = snapshotS3.GetObject(ctx, bucket.ChunkMapKey("snap1"))
	require.Error(t, err)
}
