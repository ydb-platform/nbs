package storage

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

////////////////////////////////////////////////////////////////////////////////

type chunkBlob struct {
	chunkID string
	refcnt  uint32
	data    []byte
}

func sortChunks(chunkBlobs []chunkBlob) []chunkBlob {
	sort.SliceStable(chunkBlobs, func(i, j int) bool {
		return chunkBlobs[i].chunkID < chunkBlobs[j].chunkID
	})

	return chunkBlobs
}

func readChunkBlobs(
	f *fixture,
	useS3 bool,
	chunkIDs ...string,
) []chunkBlob {

	if useS3 {
		return readChunkBlobsFromS3(f, chunkIDs...)
	} else {
		return readChunkBlobsFromYDB(f, chunkIDs...)
	}
}

func readChunkBlobsFromS3(
	f *fixture,
	chunkIDs ...string,
) []chunkBlob {

	chunkBlobs := readChunkBlobsFromYDB(f, chunkIDs...)

	for i := range chunkBlobs {
		obj := getS3Object(f, chunkBlobs[i].chunkID)
		chunkBlobs[i].data = obj.Data
	}

	return chunkBlobs
}

func readChunkBlobsFromYDB(
	f *fixture,
	chunkIDs ...string,
) []chunkBlob {

	var result []chunkBlob

	values := make([]persistence.Value, 0)
	for _, chunkID := range chunkIDs {
		values = append(values, persistence.UTF8Value(chunkID))
	}

	err := f.db.Execute(
		f.ctx,
		func(ctx context.Context, session *persistence.Session) error {
			res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				declare $chunk_ids as List<Utf8>;

				select *
				from chunk_blobs
				where chunk_id in $chunk_ids and referer = "";
			`, f.db.AbsolutePath(f.config.GetStorageFolder())),
				persistence.ValueParam("$chunk_ids", persistence.ListValue(values...)),
			)
			if err != nil {
				return err
			}
			defer res.Close()

			for res.NextResultSet(ctx) {
				for res.NextRow() {
					var blob chunkBlob
					err = res.ScanNamed(
						persistence.OptionalWithDefault("chunk_id", &blob.chunkID),
						persistence.OptionalWithDefault("refcnt", &blob.refcnt),
						persistence.OptionalWithDefault("data", &blob.data),
					)
					require.NoError(f.t, err)
					result = append(result, blob)
				}
			}
			return nil
		},
	)
	require.NoError(f.t, err)

	return sortChunks(result)
}

////////////////////////////////////////////////////////////////////////////////

type chunkMapEntry struct {
	shardID    uint64
	snapshotID string
	chunkIndex uint32
	chunkID    string
	storedInS3 bool
}

func sortChunkMapEntries(entries []chunkMapEntry) []chunkMapEntry {
	sort.SliceStable(entries, func(i, j int) bool {
		l := entries[i]
		r := entries[j]
		if l.shardID == r.shardID {
			if l.snapshotID == r.snapshotID {
				return l.chunkIndex < r.chunkIndex
			} else {
				return l.snapshotID < r.snapshotID
			}
		} else {
			return l.shardID < r.shardID
		}
	})

	return entries
}

func readChunkMap(
	f *fixture,
	snapshotID string,
) []chunkMapEntry {

	var result []chunkMapEntry

	err := f.db.Execute(
		f.ctx,
		func(ctx context.Context, session *persistence.Session) error {
			res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				declare $shard_id as Uint64;
				declare $snapshot_id as Utf8;

				select *
				from chunk_map
				where shard_id = $shard_id and snapshot_id = $snapshot_id
			`, f.db.AbsolutePath(f.config.GetStorageFolder())),
				persistence.ValueParam("$shard_id", persistence.Uint64Value(makeShardID(snapshotID))),
				persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
			)
			if err != nil {
				return err
			}
			defer res.Close()

			for res.NextResultSet(ctx) {
				for res.NextRow() {
					var entry chunkMapEntry
					err = res.ScanNamed(
						persistence.OptionalWithDefault("shard_id", &entry.shardID),
						persistence.OptionalWithDefault("snapshot_id", &entry.snapshotID),
						persistence.OptionalWithDefault("chunk_index", &entry.chunkIndex),
						persistence.OptionalWithDefault("chunk_id", &entry.chunkID),
						persistence.OptionalWithDefault("stored_in_s3", &entry.storedInS3),
					)
					require.NoError(f.t, err)
					result = append(result, entry)
				}
			}
			return nil
		},
	)
	require.NoError(f.t, err)

	return sortChunkMapEntries(result)
}

func updateBlobChecksum(
	f *fixture,
	chunkID string,
	checksum uint32,
	useS3 bool,
) {

	if useS3 {
		updateS3BlobChecksum(f, chunkID, checksum)
	} else {
		updateYDBBlobChecksum(f, chunkID, checksum)
	}
}

func getS3Object(f *fixture, chunkID string) persistence.S3Object {
	obj, err := f.s3.GetObject(
		f.ctx,
		f.config.GetS3Bucket(),
		test.NewS3Key(f.config, chunkID),
	)
	require.NoError(f.t, err)

	return obj
}

func updateS3BlobChecksum(f *fixture, chunkID string, checksum uint32) {
	obj := getS3Object(f, chunkID)

	checksumStr := strconv.FormatUint(uint64(checksum), 10)
	obj.Metadata["Checksum"] = &checksumStr

	err := f.s3.PutObject(
		f.ctx,
		f.config.GetS3Bucket(),
		test.NewS3Key(f.config, chunkID),
		obj,
	)
	require.NoError(f.t, err)
}

func clearS3BlobMetadata(f *fixture, chunkID string) {
	obj := getS3Object(f, chunkID)

	obj.Metadata = nil

	err := f.s3.PutObject(
		f.ctx,
		f.config.GetS3Bucket(),
		test.NewS3Key(f.config, chunkID),
		obj,
	)
	require.NoError(f.t, err)
}

func updateYDBBlobChecksum(f *fixture, chunkID string, checksum uint32) {
	err := f.db.Execute(
		f.ctx,
		func(ctx context.Context, session *persistence.Session) error {
			_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
					--!syntax_v1
					pragma TablePathPrefix = "%v";
					declare $shard_id as Uint64;
					declare $chunk_id as Utf8;
					declare $checksum as Uint32;
					update chunk_blobs
					set checksum = $checksum
					where shard_id = $shard_id and chunk_id = $chunk_id
			`, f.db.AbsolutePath(f.config.GetStorageFolder())),
				persistence.ValueParam("$shard_id", persistence.Uint64Value(makeShardID(chunkID))),
				persistence.ValueParam("$checksum", persistence.Uint32Value(checksum)),
				persistence.ValueParam("$chunk_id", persistence.UTF8Value(chunkID)),
			)
			return err
		},
	)
	require.NoError(f.t, err)
}

////////////////////////////////////////////////////////////////////////////////

func newYDB(ctx context.Context) (*persistence.YDBClient, error) {
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
	)
	database := "/Root"
	rootPath := "disk_manager"
	connectionTimeout := "10s"

	return persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint:          &endpoint,
			Database:          &database,
			RootPath:          &rootPath,
			ConnectionTimeout: &connectionTimeout,
		},
		metrics.NewEmptyRegistry(),
	)
}

func newStorage(
	t *testing.T,
	ctx context.Context,
	config *snapshot_config.SnapshotConfig,
	db *persistence.YDBClient,
	s3 *persistence.S3Client,
	registry metrics.Registry,
) Storage {

	err := schema.Create(ctx, config, db, s3, false /* dropUnusedColumns */)
	require.NoError(t, err)

	storage, err := NewStorage(config, registry, db, s3)
	require.NoError(t, err)

	return storage
}

////////////////////////////////////////////////////////////////////////////////

type fixture struct {
	t       *testing.T
	ctx     context.Context
	cancel  context.CancelFunc
	db      *persistence.YDBClient
	s3      *persistence.S3Client
	config  *snapshot_config.SnapshotConfig
	storage Storage
}

func (f *fixture) teardown() {
	err := f.db.Close(f.ctx)
	require.NoError(f.t, err)
	f.cancel()
}

func createFixture(t *testing.T) *fixture {
	ctx, cancel := context.WithCancel(test.NewContext())

	db, err := newYDB(ctx)
	require.NoError(t, err)

	s3, err := test.NewS3Client()
	require.NoError(t, err)

	storageFolder := fmt.Sprintf("storage_ydb_test/%v", t.Name())
	deleteWorkerCount := uint32(10)
	shallowCopyWorkerCount := uint32(10)
	shallowCopyInflightLimit := uint32(100)
	shardCount := uint64(2)
	compression := ""
	s3Bucket := "test"
	chunkBlobsS3KeyPrefix := t.Name()

	config := &snapshot_config.SnapshotConfig{
		StorageFolder:             &storageFolder,
		DeleteWorkerCount:         &deleteWorkerCount,
		ShallowCopyWorkerCount:    &shallowCopyWorkerCount,
		ShallowCopyInflightLimit:  &shallowCopyInflightLimit,
		ChunkBlobsTableShardCount: &shardCount,
		ChunkMapTableShardCount:   &shardCount,
		ChunkCompression:          &compression,
		S3Bucket:                  &s3Bucket,
		ChunkBlobsS3KeyPrefix:     &chunkBlobsS3KeyPrefix,
	}

	storage := newStorage(t, ctx, config, db, s3, metrics.NewEmptyRegistry())

	return &fixture{
		t:       t,
		ctx:     ctx,
		cancel:  cancel,
		db:      db,
		s3:      s3,
		config:  config,
		storage: storage,
	}
}

////////////////////////////////////////////////////////////////////////////////

func makeChunk(chunkIndex uint32, chunkData string) dataplane_common.Chunk {
	return dataplane_common.Chunk{
		Index: chunkIndex,
		Data:  []byte(chunkData),
	}
}

func makeZeroChunk(chunkIndex uint32) dataplane_common.Chunk {
	return dataplane_common.Chunk{
		Index: chunkIndex,
		Zero:  true,
	}
}

////////////////////////////////////////////////////////////////////////////////

type differentChunkStorageTestCase struct {
	name  string
	useS3 bool
}

func testCases() []differentChunkStorageTestCase {
	return []differentChunkStorageTestCase{
		{"store chunks in ydb", false},
		{"store chunks in s3", true},
	}
}

////////////////////////////////////////////////////////////////////////////////

func checkIncremental(
	t *testing.T,
	f *fixture,
	disk *types.Disk,
	expectedSnapshotID string,
	expectedCheckpointID string,
) {

	snapshotID, checkpointID, err := f.storage.GetIncremental(f.ctx, disk)
	require.NoError(t, err)
	require.Equal(t, expectedSnapshotID, snapshotID)
	require.Equal(t, expectedCheckpointID, checkpointID)
}

func checkBaseSnapshot(
	t *testing.T,
	ctx context.Context,
	storage Storage,
	snapshotID string,
	expectedBaseSnapshotID string,
) {

	snapshotMeta, err := storage.GetSnapshotMeta(ctx, snapshotID)
	require.NoError(t, err)
	require.EqualValues(t, expectedBaseSnapshotID, snapshotMeta.BaseSnapshotID)
}

func checkLockTaskID(
	t *testing.T,
	ctx context.Context,
	storage Storage,
	snapshotID string,
	expectedLockTaskID string,
) {

	snapshotMeta, err := storage.GetSnapshotMeta(ctx, snapshotID)
	require.NoError(t, err)
	require.EqualValues(t, expectedLockTaskID, snapshotMeta.LockTaskID)
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateSnapshot(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			snapshotMeta, err := f.storage.CreateSnapshot(
				f.ctx,
				SnapshotMeta{
					ID: "snapshot",
				},
			)
			require.NoError(t, err)
			require.False(t, snapshotMeta.Ready)

			// Check idempotency.
			_, err = f.storage.CreateSnapshot(
				f.ctx,
				SnapshotMeta{
					ID: "snapshot",
				},
			)
			require.NoError(t, err)

			err = f.storage.SnapshotCreated(f.ctx, "snapshot", 0, 0, 0, nil)
			require.NoError(t, err)

			// Check idempotency.
			err = f.storage.SnapshotCreated(f.ctx, "snapshot", 0, 0, 0, nil)
			require.NoError(t, err)

			// Check idempotency.
			snapshotMeta, err = f.storage.CreateSnapshot(
				f.ctx,
				SnapshotMeta{
					ID: "snapshot",
				},
			)
			require.NoError(t, err)
			require.True(t, snapshotMeta.Ready)
		})
	}
}

func TestSnapshotsCreateIncrementalSnapshot(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			disk := types.Disk{
				ZoneId: "zone",
				DiskId: "disk",
			}

			snapshot1 := SnapshotMeta{
				ID:           "snapshot1",
				Disk:         &disk,
				CheckpointID: "checkpoint1",
				CreateTaskID: "create1",
			}

			created, err := f.storage.CreateSnapshot(f.ctx, snapshot1)
			require.NoError(t, err)
			require.NotNil(t, created)
			require.Empty(t, created.BaseSnapshotID)
			require.Empty(t, created.BaseCheckpointID)
			checkIncremental(t, f, &disk, "", "")

			created, err = f.storage.CreateSnapshot(f.ctx, SnapshotMeta{
				ID: "snapshot2",
				Disk: &types.Disk{
					ZoneId: "zone",
					DiskId: "disk",
				},
				CheckpointID: "checkpoint2",
				CreateTaskID: "create2",
			})
			require.NoError(t, err)
			require.NotNil(t, created)
			require.Empty(t, created.BaseSnapshotID)
			require.Empty(t, created.BaseCheckpointID)

			err = f.storage.SnapshotCreated(f.ctx, snapshot1.ID, 0, 0, 0, nil)
			require.NoError(t, err)
			checkIncremental(t, f, &disk, "snapshot1", "checkpoint1")

			snapshot3 := SnapshotMeta{
				ID: "snapshot3",
				Disk: &types.Disk{
					ZoneId: "zone",
					DiskId: "disk",
				},
				CheckpointID: "checkpoint3",
				CreateTaskID: "create3",
			}

			created, err = f.storage.CreateSnapshot(f.ctx, snapshot3)
			require.NoError(t, err)
			require.NotNil(t, created)
			require.Equal(t, snapshot1.ID, created.BaseSnapshotID)
			require.Equal(t, snapshot1.CheckpointID, created.BaseCheckpointID)
			checkBaseSnapshot(t, f.ctx, f.storage, snapshot3.ID, snapshot1.ID)

			err = f.storage.SnapshotCreated(f.ctx, snapshot3.ID, 0, 0, 0, nil)
			require.NoError(t, err)
			checkIncremental(t, f, &disk, "snapshot3", "checkpoint3")

			snapshot4 := SnapshotMeta{
				ID: "snapshot4",
				Disk: &types.Disk{
					ZoneId: "zone",
					DiskId: "disk",
				},
				CheckpointID: "checkpoint4",
				CreateTaskID: "create4",
			}

			created, err = f.storage.CreateSnapshot(f.ctx, snapshot4)
			require.NoError(t, err)
			require.NotNil(t, created)
			require.Equal(t, snapshot3.ID, created.BaseSnapshotID)
			require.Equal(t, snapshot3.CheckpointID, created.BaseCheckpointID)
			checkBaseSnapshot(t, f.ctx, f.storage, snapshot4.ID, snapshot3.ID)

			err = f.storage.SnapshotCreated(f.ctx, snapshot4.ID, 0, 0, 0, nil)
			require.NoError(t, err)
			checkIncremental(t, f, &disk, "snapshot4", "checkpoint4")

			_, err = f.storage.DeletingSnapshot(f.ctx, snapshot1.ID, "delete1")
			require.NoError(t, err)

			_, err = f.storage.DeletingSnapshot(f.ctx, snapshot3.ID, "delete3")
			require.NoError(t, err)

			_, err = f.storage.DeletingSnapshot(f.ctx, snapshot4.ID, "delete4")
			require.NoError(t, err)
		})
	}
}

func TestSnapshotsLocks(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			snapshot1 := SnapshotMeta{
				ID: "snapshot1",
				Disk: &types.Disk{
					ZoneId: "zone",
					DiskId: "disk",
				},
				CheckpointID: "checkpoint1",
				CreateTaskID: "create1",
			}

			created, err := f.storage.CreateSnapshot(f.ctx, snapshot1)
			require.NoError(t, err)
			require.NotNil(t, created)

			err = f.storage.SnapshotCreated(f.ctx, snapshot1.ID, 0, 0, 0, nil)
			require.NoError(t, err)

			snapshot2 := SnapshotMeta{
				ID: "snapshot2",
				Disk: &types.Disk{
					ZoneId: "zone",
					DiskId: "disk",
				},
				CheckpointID: "checkpoint2",
				CreateTaskID: "create2",
			}

			created, err = f.storage.CreateSnapshot(f.ctx, snapshot2)
			require.NoError(t, err)
			require.NotNil(t, created)

			locked, err := f.storage.LockSnapshot(f.ctx, snapshot1.ID, created.CreateTaskID)
			require.NoError(t, err)
			require.True(t, locked)
			checkLockTaskID(t, f.ctx, f.storage, snapshot1.ID, created.CreateTaskID)

			err = f.storage.SnapshotCreated(f.ctx, snapshot2.ID, 0, 0, 0, nil)
			require.NoError(t, err)

			err = f.storage.UnlockSnapshot(f.ctx, snapshot1.ID, created.CreateTaskID)
			require.NoError(t, err)
			checkLockTaskID(t, f.ctx, f.storage, snapshot1.ID, "")

			_, err = f.storage.DeletingSnapshot(f.ctx, snapshot2.ID, created.CreateTaskID)
			require.NoError(t, err)

			snapshot3 := SnapshotMeta{
				ID: "snapshot3",
				Disk: &types.Disk{
					ZoneId: "zone",
					DiskId: "disk",
				},
				CheckpointID: "checkpoint3",
				CreateTaskID: "create3",
			}

			created, err = f.storage.CreateSnapshot(f.ctx, snapshot3)
			require.NoError(t, err)
			require.NotNil(t, created)

			locked, err = f.storage.LockSnapshot(f.ctx, snapshot2.ID, created.CreateTaskID)
			require.NoError(t, err)
			require.False(t, locked)
			checkLockTaskID(t, f.ctx, f.storage, snapshot2.ID, "")

			err = f.storage.SnapshotCreated(f.ctx, snapshot3.ID, 0, 0, 0, nil)
			require.NoError(t, err)

			err = f.storage.UnlockSnapshot(f.ctx, snapshot2.ID, created.CreateTaskID)
			require.NoError(t, err)
			checkLockTaskID(t, f.ctx, f.storage, snapshot2.ID, "")

			_, err = f.storage.DeletingSnapshot(f.ctx, snapshot3.ID, "delete3")
			require.NoError(t, err)

			_, err = f.storage.DeletingSnapshot(f.ctx, snapshot1.ID, "delete1")
			require.NoError(t, err)
		})
	}
}

func TestDeletingSnapshot(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			_, err := f.storage.CreateSnapshot(
				f.ctx,
				SnapshotMeta{
					ID: "snapshot",
				},
			)
			require.NoError(t, err)

			_, err = f.storage.DeletingSnapshot(f.ctx, "snapshot", "task")
			require.NoError(t, err)

			err = f.storage.SnapshotCreated(f.ctx, "snapshot", 0, 0, 0, nil)
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

			// Check idempotency.
			_, err = f.storage.DeletingSnapshot(f.ctx, "snapshot", "task")
			require.NoError(t, err)

			_, err = f.storage.CreateSnapshot(
				f.ctx,
				SnapshotMeta{
					ID: "snapshot",
				},
			)
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

			// Check idempotency.
			_, err = f.storage.DeletingSnapshot(f.ctx, "snapshot", "task")
			require.NoError(t, err)

			_, err = f.storage.CreateSnapshot(
				f.ctx,
				SnapshotMeta{
					ID: "snapshot",
				},
			)
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

			err = f.storage.SnapshotCreated(f.ctx, "snapshot", 0, 0, 0, nil)
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
		})
	}
}

func TestDeleteNonexistentSnapshot(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			_, err := f.storage.DeletingSnapshot(f.ctx, "snapshot", "task")
			require.NoError(t, err)

			err = f.storage.SnapshotCreated(f.ctx, "snapshot", 0, 0, 0, nil)
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

			_, err = f.storage.DeletingSnapshot(f.ctx, "snapshot", "task")
			require.NoError(t, err)

			// Check idempotency.
			_, err = f.storage.DeletingSnapshot(f.ctx, "snapshot", "task")
			require.NoError(t, err)

			_, err = f.storage.CreateSnapshot(
				f.ctx,
				SnapshotMeta{
					ID: "snapshot",
				},
			)
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
		})
	}
}

func TestClearDeletingSnapshots(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			_, err := f.storage.CreateSnapshot(
				f.ctx,
				SnapshotMeta{
					ID: "snapshot",
				},
			)
			require.NoError(t, err)

			deletingAt := time.Now() // not exactly precise
			_, err = f.storage.DeletingSnapshot(f.ctx, "snapshot", "task")
			require.NoError(t, err)

			deletingBefore := deletingAt.Add(-time.Microsecond)

			toDelete, err := f.storage.GetSnapshotsToDelete(f.ctx, deletingBefore, 100500)
			require.NoError(t, err)
			require.Empty(t, toDelete)

			deletingBefore = time.Now()

			toDelete, err = f.storage.GetSnapshotsToDelete(f.ctx, deletingBefore, 100500)
			require.NoError(t, err)
			require.Equal(t, 1, len(toDelete))
			require.Equal(t, "snapshot", toDelete[0].SnapshotId)

			// Check idempotency.
			toDelete, err = f.storage.GetSnapshotsToDelete(f.ctx, deletingBefore, 100500)
			require.NoError(t, err)
			require.Equal(t, 1, len(toDelete))
			require.Equal(t, "snapshot", toDelete[0].SnapshotId)

			err = f.storage.ClearDeletingSnapshots(f.ctx, toDelete)
			require.NoError(t, err)

			// Check idempotency.
			err = f.storage.ClearDeletingSnapshots(f.ctx, toDelete)
			require.NoError(t, err)

			deletingBefore = time.Now()

			toDelete, err = f.storage.GetSnapshotsToDelete(f.ctx, deletingBefore, 100500)
			require.NoError(t, err)
			require.Empty(t, toDelete)

			_, err = f.storage.CreateSnapshot(
				f.ctx,
				SnapshotMeta{
					ID: "snapshot",
				},
			)
			require.NoError(t, err)
		})
	}
}

func TestDeleteSnapshotData(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			dataChunk := makeChunk(0, "abc")
			dataChunkID, err := f.storage.WriteChunk(f.ctx, "", "snapshot", dataChunk, testCase.useS3)
			require.NoError(t, err)

			zeroChunk := makeZeroChunk(1)
			zeroChunkID, err := f.storage.WriteChunk(f.ctx, "", "snapshot", zeroChunk, testCase.useS3)
			require.NoError(t, err)

			err = f.storage.DeleteSnapshotData(f.ctx, "snapshot")
			require.NoError(t, err)

			chunkBlobs := readChunkBlobs(f, testCase.useS3, dataChunkID, zeroChunkID)
			require.Equal(t, 0, len(chunkBlobs))

			chunkMapEntries := readChunkMap(f, "snapshot")
			require.Equal(t, 0, len(chunkMapEntries))
		})
	}
}

func TestShallowCopySnapshot(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			chunk1 := makeChunk(0, "abc")
			chunkID1, err := f.storage.WriteChunk(f.ctx, "", "src", chunk1, testCase.useS3)
			require.NoError(t, err)
			chunk2 := makeChunk(1, "qwe")
			chunkID2, err := f.storage.WriteChunk(f.ctx, "", "src", chunk2, testCase.useS3)
			require.NoError(t, err)
			chunk3 := makeChunk(10, "aaa")
			chunkID3, err := f.storage.WriteChunk(f.ctx, "", "src", chunk3, testCase.useS3)
			require.NoError(t, err)
			chunk4 := dataplane_common.Chunk{
				Index: 11,
				Zero:  true,
			}
			chunkID4, err := f.storage.WriteChunk(f.ctx, "", "src", chunk4, testCase.useS3)
			require.NoError(t, err)

			chunkBlobs := readChunkBlobs(f, testCase.useS3, chunkID1, chunkID2, chunkID3, chunkID4)
			require.Equal(t, sortChunks([]chunkBlob{
				{chunkID1, 1, []byte("abc")},
				{chunkID2, 1, []byte("qwe")},
				{chunkID3, 1, []byte("aaa")},
			}), chunkBlobs)

			chunkMapEntries := readChunkMap(f, "src")
			require.Equal(t, sortChunkMapEntries([]chunkMapEntry{
				{makeShardID("src"), "src", 0, chunkID1, testCase.useS3},
				{makeShardID("src"), "src", 1, chunkID2, testCase.useS3},
				{makeShardID("src"), "src", 10, chunkID3, testCase.useS3},
				{makeShardID("src"), "src", 11, chunkID4, false},
			}), chunkMapEntries)

			err = f.storage.ShallowCopySnapshot(f.ctx, "src", "dst", 0, nil)
			require.NoError(t, err)

			chunkBlobs = readChunkBlobs(f, testCase.useS3, chunkID1, chunkID2, chunkID3, chunkID4)
			require.Equal(t, sortChunks([]chunkBlob{
				{chunkID1, 2, []byte("abc")},
				{chunkID2, 2, []byte("qwe")},
				{chunkID3, 2, []byte("aaa")},
			}), chunkBlobs)

			chunkMapEntries = readChunkMap(f, "dst")
			require.Equal(t, sortChunkMapEntries([]chunkMapEntry{
				{makeShardID("dst"), "dst", 0, chunkID1, testCase.useS3},
				{makeShardID("dst"), "dst", 1, chunkID2, testCase.useS3},
				{makeShardID("dst"), "dst", 10, chunkID3, testCase.useS3},
				{makeShardID("dst"), "dst", 11, chunkID4, false},
			}), chunkMapEntries)

			// Check idempotency.
			err = f.storage.ShallowCopySnapshot(f.ctx, "src", "dst", 0, nil)
			require.NoError(t, err)

			chunkBlobs = readChunkBlobs(f, testCase.useS3, chunkID1, chunkID2, chunkID3, chunkID4)
			require.Equal(t, sortChunks([]chunkBlob{
				{chunkID1, 2, []byte("abc")},
				{chunkID2, 2, []byte("qwe")},
				{chunkID3, 2, []byte("aaa")},
			}), chunkBlobs)

			chunkMapEntries = readChunkMap(f, "dst")
			require.Equal(t, sortChunkMapEntries([]chunkMapEntry{
				{makeShardID("dst"), "dst", 0, chunkID1, testCase.useS3},
				{makeShardID("dst"), "dst", 1, chunkID2, testCase.useS3},
				{makeShardID("dst"), "dst", 10, chunkID3, testCase.useS3},
				{makeShardID("dst"), "dst", 11, chunkID4, false},
			}), chunkMapEntries)
		})
	}
}

func TestDeleteCopiedSnapshot(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			chunk := makeChunk(0, "abc")
			chunkID, err := f.storage.WriteChunk(f.ctx, "", "src", chunk, testCase.useS3)
			require.NoError(t, err)

			chunkBlobs := readChunkBlobs(f, testCase.useS3, chunkID)
			require.Equal(t, chunkBlobs, []chunkBlob{
				{chunkID, 1, []byte("abc")},
			})

			chunkMapEntries := readChunkMap(f, "src")
			require.Equal(t, sortChunkMapEntries([]chunkMapEntry{
				{makeShardID("src"), "src", 0, chunkID, testCase.useS3},
			}), chunkMapEntries)

			err = f.storage.ShallowCopySnapshot(f.ctx, "src", "dst", 0, nil)
			require.NoError(t, err)

			chunkBlobs = readChunkBlobs(f, testCase.useS3, chunkID)
			require.Equal(t, chunkBlobs, []chunkBlob{
				{chunkID, 2, []byte("abc")},
			})

			chunkMapEntries = readChunkMap(f, "dst")
			require.Equal(t, sortChunkMapEntries([]chunkMapEntry{
				{makeShardID("dst"), "dst", 0, chunkID, testCase.useS3},
			}), chunkMapEntries)

			err = f.storage.DeleteSnapshotData(f.ctx, "dst")
			require.NoError(t, err)

			chunkBlobs = readChunkBlobs(f, testCase.useS3, chunkID)
			require.Equal(t, chunkBlobs, []chunkBlob{
				{chunkID, 1, []byte("abc")},
			})

			chunkMapEntries = readChunkMap(f, "dst")
			require.Equal(t, 0, len(chunkMapEntries))
		})
	}
}

func TestDeleteCopiedSnapshotSource(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			chunk := makeChunk(0, "abc")
			chunkID, err := f.storage.WriteChunk(f.ctx, "", "src", chunk, testCase.useS3)
			require.NoError(t, err)

			chunkBlobs := readChunkBlobs(f, testCase.useS3, chunkID)
			require.Equal(t, chunkBlobs, []chunkBlob{
				{chunkID, 1, []byte("abc")},
			})

			chunkMapEntries := readChunkMap(f, "src")
			require.Equal(t, sortChunkMapEntries([]chunkMapEntry{
				{makeShardID("src"), "src", 0, chunkID, testCase.useS3},
			}), chunkMapEntries)

			err = f.storage.ShallowCopySnapshot(f.ctx, "src", "dst", 0, nil)
			require.NoError(t, err)

			chunkBlobs = readChunkBlobs(f, testCase.useS3, chunkID)
			require.Equal(t, chunkBlobs, []chunkBlob{
				{chunkID, 2, []byte("abc")},
			})

			chunkMapEntries = readChunkMap(f, "dst")
			require.Equal(t, sortChunkMapEntries([]chunkMapEntry{
				{makeShardID("dst"), "dst", 0, chunkID, testCase.useS3},
			}), chunkMapEntries)

			err = f.storage.DeleteSnapshotData(f.ctx, "src")
			require.NoError(t, err)

			chunkBlobs = readChunkBlobs(f, testCase.useS3, chunkID)
			require.Equal(t, []chunkBlob{
				{chunkID, 1, []byte("abc")},
			}, chunkBlobs)

			chunkMapEntries = readChunkMap(f, "src")
			require.Equal(t, 0, len(chunkMapEntries))
		})
	}
}

func TestShallowCopySnapshotWithRandomFailure(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			chunkCount := uint32(1000)

			rand.Seed(time.Now().UnixNano())

			chunks := make([]string, 0)
			expectedChunkBlobs := make([]chunkBlob, 0)
			expectedChunkMapEntries := make([]chunkMapEntry, 0)

			workerCount := uint32(10)
			chunksPerWorker := chunkCount / workerCount

			var workerMutex sync.Mutex
			var wg sync.WaitGroup

			for i := uint32(0); i < workerCount; i++ {
				wg.Add(1)
				i := i

				go func() {
					defer wg.Done()

					for j := i * chunksPerWorker; j < (i+1)*chunksPerWorker; j++ {
						var chunk dataplane_common.Chunk

						data := []byte(fmt.Sprintf("chunk-%v", j))
						chunk = dataplane_common.Chunk{Index: j, Data: data}

						chunkID, err := f.storage.WriteChunk(f.ctx, "", "src", chunk, testCase.useS3)
						require.NoError(t, err)

						workerMutex.Lock()

						chunks = append(chunks, chunkID)

						expectedChunkBlobs = append(expectedChunkBlobs, chunkBlob{
							chunkID: chunkID,
							refcnt:  2,
							data:    data,
						})

						expectedChunkMapEntries = append(expectedChunkMapEntries, chunkMapEntry{
							shardID:    makeShardID("dst"),
							snapshotID: "dst",
							chunkIndex: j,
							chunkID:    chunkID,
							storedInS3: testCase.useS3,
						})

						workerMutex.Unlock()
					}
				}()
			}

			wg.Wait()

			milestoneChunkIndex := uint32(0)
			attemptIndex := 0

			attempt := func() error {
				logging.Info(
					f.ctx,
					"Attempt #%v, milestoneChunkIndex=%v",
					attemptIndex,
					milestoneChunkIndex,
				)

				copyCtx, cancelCopyCtx := context.WithCancel(f.ctx)
				defer cancelCopyCtx()

				rand.Seed(time.Now().UnixNano())

				go func() {
					for {
						select {
						case <-copyCtx.Done():
							return
						case <-time.After(time.Second):
							if rand.Intn(5) == 0 {
								logging.Info(f.ctx, "Cancelling copy context...")
								cancelCopyCtx()
							}
						}
					}
				}()

				err := f.storage.ShallowCopySnapshot(
					copyCtx,
					"src",
					"dst",
					milestoneChunkIndex,
					func(ctx context.Context, milestoneIndex uint32) error {
						milestoneChunkIndex = milestoneIndex

						logging.Info(
							ctx,
							"Updating progress, milestoneChunkIndex=%v",
							milestoneChunkIndex,
						)

						if rand.Intn(2) == 0 {
							return errors.NewRetriableErrorf(
								"emulated saveProgress error",
							)
						}

						return nil
					},
				)

				return err
			}

			for {
				err := attempt()
				if err == nil {
					break
				}

				if errors.CanRetry(err) {
					logging.Warn(f.ctx, "Attempt #%v failed: %v", attemptIndex, err)
					attemptIndex++
					continue
				}

				require.NoError(t, err)
			}

			chunkBlobs := readChunkBlobs(f, testCase.useS3, chunks...)
			require.Equal(t, sortChunks(expectedChunkBlobs), chunkBlobs)

			chunkMapEntries := readChunkMap(f, "dst")
			require.Equal(t, sortChunkMapEntries(expectedChunkMapEntries), chunkMapEntries)
		})
	}
}

func TestWriteChunk(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			chunk := makeChunk(0, "data")
			chunkID, err := f.storage.WriteChunk(f.ctx, "", "snapshot", chunk, testCase.useS3)
			require.NoError(t, err)

			entries, errors := f.storage.ReadChunkMap(f.ctx, "snapshot", 0)

			actual := make([]ChunkMapEntry, 0)
			for a := range entries {
				actual = append(actual, a)
			}
			require.NoError(t, <-errors)

			require.Equal(t, []ChunkMapEntry{{0, chunkID, testCase.useS3}}, actual)
		})
	}
}

func TestWriteChunkIdempotency(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			chunk := makeChunk(0, "data")
			chunkID1, err := f.storage.WriteChunk(f.ctx, "", "snapshot", chunk, testCase.useS3)
			require.NoError(t, err)

			chunkID2, err := f.storage.WriteChunk(f.ctx, "", "snapshot", chunk, testCase.useS3)
			require.NoError(t, err)

			require.Equal(t, chunkID1, chunkID2)

			chunkBlobs := readChunkBlobs(f, testCase.useS3, chunkID1, chunkID2)
			require.Equal(t, sortChunks([]chunkBlob{
				{chunkID1, 1, []byte("data")},
			}), chunkBlobs)

			chunkMapEntries := readChunkMap(f, "snapshot")
			require.Equal(t, sortChunkMapEntries([]chunkMapEntry{
				{makeShardID("snapshot"), "snapshot", 0, chunkID1, testCase.useS3},
			}), chunkMapEntries)
		})
	}
}

func TestFlattenChunkData(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			chunk := dataplane_common.Chunk{
				Index: 0,
				Data:  []byte("abcdef"),
			}

			chunkID, err := f.storage.WriteChunk(f.ctx, "", "test", chunk, testCase.useS3)
			require.NoError(t, err)

			chunkBlobs := readChunkBlobs(f, testCase.useS3, chunkID)
			require.Equal(t, chunkBlobs, []chunkBlob{
				{chunkID, 1, []byte("abcdef")},
			})
		})
	}
}

func TestReadChunk(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			snapshotID := "test"
			chunk := makeChunk(0, "abc")
			chunkID, err := f.storage.WriteChunk(f.ctx, "", snapshotID, chunk, testCase.useS3)
			require.NoError(t, err)

			chunkMapEntries := readChunkMap(f, snapshotID)
			require.Len(t, chunkMapEntries, 1)

			readChunk := dataplane_common.Chunk{
				ID:         chunkID,
				Data:       make([]byte, len(chunk.Data)),
				StoredInS3: chunkMapEntries[0].storedInS3,
			}
			err = f.storage.ReadChunk(f.ctx, &readChunk)
			require.NoError(t, err)
			require.Equal(t, chunk.Data, readChunk.Data)
		})
	}
}

func TestReadNonExistentChunk(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			readChunk := dataplane_common.Chunk{
				ID:         "nonexistent",
				Data:       make([]byte, 10000),
				StoredInS3: testCase.useS3,
			}
			err := f.storage.ReadChunk(f.ctx, &readChunk)
			require.Error(t, err)
			require.ErrorContains(t, err, "not found")
		})
	}
}

func TestWriteZeroChunk(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			chunk := makeZeroChunk(0)
			chunkID, err := f.storage.WriteChunk(f.ctx, "", "snapshot", chunk, testCase.useS3)
			require.NoError(t, err)
			require.Equal(t, "", chunkID)

			chunkBlobs := readChunkBlobs(f, testCase.useS3, chunkID)
			require.Equal(t, 0, len(chunkBlobs))

			chunkMapEntries := readChunkMap(f, "snapshot")
			require.Equal(t, []chunkMapEntry{
				{makeShardID("snapshot"), "snapshot", 0, "", false},
			}, chunkMapEntries)
		})
	}
}

func TestShallowCopyZeroChunk(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			chunk := makeZeroChunk(0)
			chunkID, err := f.storage.WriteChunk(f.ctx, "", "src", chunk, testCase.useS3)
			require.NoError(t, err)

			err = f.storage.ShallowCopySnapshot(f.ctx, "src", "dst", 0, nil)
			require.NoError(t, err)

			chunkBlobs := readChunkBlobs(f, testCase.useS3, chunkID)
			require.Equal(t, 0, len(chunkBlobs))

			chunkMapEntries := readChunkMap(f, "dst")
			require.Equal(t, sortChunkMapEntries([]chunkMapEntry{
				{makeShardID("dst"), "dst", 0, "", false},
			}), chunkMapEntries)
		})
	}
}

func TestCheckSnapshotReady(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			_, err := f.storage.CreateSnapshot(
				f.ctx,
				SnapshotMeta{
					ID: "snapshot",
				},
			)
			require.NoError(t, err)

			diskSize := uint64(2 * 1024 * 1024)
			chunkCount := uint32(2)

			err = f.storage.SnapshotCreated(f.ctx, "snapshot", diskSize, diskSize, chunkCount, nil)
			require.NoError(t, err)

			resource, err := f.storage.CheckSnapshotReady(f.ctx, "snapshot")
			require.NoError(t, err)
			require.Equal(t, diskSize, resource.Size)
			require.Equal(t, diskSize, resource.StorageSize)
			require.Equal(t, chunkCount, resource.ChunkCount)
		})
	}
}

func TestChunkChecksumMismatch(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			chunk := makeChunk(0, "abc")
			chunkID, err := f.storage.WriteChunk(f.ctx, "", "test", chunk, testCase.useS3)
			require.NoError(t, err)

			updateBlobChecksum(f, chunkID, 1111111111, testCase.useS3)

			readChunk := dataplane_common.Chunk{
				ID:         chunkID,
				Data:       make([]byte, len(chunk.Data)),
				StoredInS3: testCase.useS3,
			}
			err = f.storage.ReadChunk(f.ctx, &readChunk)
			require.Error(t, err)
			require.Contains(t, err.Error(), "chunk checksum mismatch")
		})
	}
}

func TestS3BlobMetadataMissing(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	chunk := makeChunk(0, "abc")
	chunkID, err := f.storage.WriteChunk(f.ctx, "", "test", chunk, true /* useS3 */)
	require.NoError(t, err)

	clearS3BlobMetadata(f, chunkID)

	readChunk := dataplane_common.Chunk{
		ID:         chunkID,
		Data:       make([]byte, len(chunk.Data)),
		StoredInS3: true,
	}
	err = f.storage.ReadChunk(f.ctx, &readChunk)
	require.Error(t, err)
	require.Contains(t, err.Error(), "s3 object metadata is missing")
}

func TestChunkCompression(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			comp := "lz4"
			f.config.ChunkCompression = &comp
			f.storage = newStorage(
				t,
				f.ctx,
				f.config,
				f.db,
				f.s3,
				metrics.NewEmptyRegistry(),
			)

			chunk := makeChunk(0, "abc")
			chunkID, err := f.storage.WriteChunk(f.ctx, "", "test", chunk, testCase.useS3)
			require.NoError(t, err)

			readChunk := dataplane_common.Chunk{
				ID:         chunkID,
				Data:       make([]byte, len(chunk.Data)),
				StoredInS3: testCase.useS3,
			}
			err = f.storage.ReadChunk(f.ctx, &readChunk)
			require.NoError(t, err)
			require.Equal(t, chunk.Data, readChunk.Data)
		})
	}
}

func TestReadChunkMapReturnsErrorWhenContextIsAlreadyCancelled(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			ctx, cancel := context.WithCancel(f.ctx)
			cancel()

			entries, errors := f.storage.ReadChunkMap(ctx, "snapshot", 0)
			require.NotNil(t, entries, errors)

			err := <-errors
			require.Error(t, err)
			require.Contains(t, err.Error(), "context canceled")
		})
	}
}

func TestGetDataChunkCount(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()

			chunks := []string{
				"qwe", "", "abc", "aaa", "", "asd",
			}

			for i, data := range chunks {
				var chunk dataplane_common.Chunk

				if len(data) != 0 {
					chunk = makeChunk(uint32(i), data)
				} else {
					chunk = makeZeroChunk(uint32(i))
				}
				_, err := f.storage.WriteChunk(f.ctx, "", "snapshot", chunk, testCase.useS3)
				require.NoError(t, err)
			}

			chunkCount, err := f.storage.GetDataChunkCount(f.ctx, "snapshot")
			require.NoError(t, err)
			require.Equal(t, uint64(4), chunkCount)
		})
	}
}

func TestCompressionMetricsCollection(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(
			testCase.name, func(t *testing.T) {
				f := createFixture(t)
				defer f.teardown()

				comp := "lz4"
				f.config.ChunkCompression = &comp
				f.config.ProbeCompressionPercentage = map[string]uint32{
					"gzip":      100,
					"zstd":      100,
					"zstd_cgo":  100,
					"lz4_block": 100,
				}
				registry := mocks.NewIgnoreUnknownCallsRegistryMock()
				histogramName := "chunkCompressionRatio"
				for compression := range f.config.ProbeCompressionPercentage {
					expectHistogramCalledOnce(
						registry,
						histogramName,
						map[string]string{"compression": compression},
					)
				}

				f.storage = newStorage(t, f.ctx, f.config, f.db, f.s3, registry)
				chunk := makeChunk(0, "abc")
				chunkID, err := f.storage.WriteChunk(
					f.ctx,
					"",
					"test",
					chunk,
					testCase.useS3,
				)
				require.NoError(t, err)

				readChunk := dataplane_common.Chunk{
					ID:         chunkID,
					Data:       make([]byte, len(chunk.Data)),
					StoredInS3: testCase.useS3,
				}
				err = f.storage.ReadChunk(f.ctx, &readChunk)
				require.NoError(t, err)
				require.Equal(t, chunk.Data, readChunk.Data)
			},
		)
	}
}

func expectHistogramCalledOnce(registry *mocks.RegistryMock, name string, compressionTags map[string]string) *mock.Call {
	return registry.GetHistogram(
		name,
		compressionTags,
		metrics.NewExponentialBuckets(1, 1.5, 10),
	).On(
		"RecordValue",
		mock.MatchedBy(
			func(i float64) bool {
				return true
			},
		),
	).Return(nil).Once()
}

////////////////////////////////////////////////////////////////////////////////

type ydbTestFixture struct {
	t      *testing.T
	ctx    context.Context
	cancel context.CancelFunc
	db     *persistence.YDBClient
	folder string
}

func newYdbTestFixture(t *testing.T) *ydbTestFixture {
	ctx, cancel := context.WithCancel(test.NewContext())
	db, err := newYDB(ctx)
	require.NoError(t, err)
	folder := fmt.Sprintf("ydb_test/%v", t.Name())
	return &ydbTestFixture{
		t:      t,
		ctx:    ctx,
		cancel: cancel,
		db:     db,
		folder: folder,
	}
}

func tableDescription() persistence.CreateTableDescription {
	optional := persistence.Optional
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("shard_id", optional(persistence.TypeUint64)),
		persistence.WithColumn("chunk_id", optional(persistence.TypeUTF8)),
		persistence.WithColumn("data", optional(persistence.TypeString)),
		persistence.WithPrimaryKeyColumn("shard_id", "chunk_id"),
		persistence.WithUniformPartitions(5),
		persistence.WithExternalBlobs("rotencrypted"),
	)
}

func setupTestYdbRequestDoesNotHang(t *testing.T) {
	f := newYdbTestFixture(t)
	err := f.db.CreateOrAlterTable(
		f.ctx,
		f.folder,
		"table",
		tableDescription(),
		false, // dropUnusedColumns
	)
	require.NoError(f.t, err)
}

func (f *ydbTestFixture) teardown() {
	require.NoError(f.t, f.db.Close(f.ctx))
	f.cancel()
}

func (f *ydbTestFixture) writeChunkData(
	ctx context.Context,
	chunkIndex int,
) error {

	dataToWrite := make([]byte, 4096*1024)
	_, err := f.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $chunk_id as Utf8;
		declare $data as String;

		upsert into %v (shard_id, chunk_id, data)
		values ($shard_id, $chunk_id, $data)
	`, f.db.AbsolutePath(f.folder), "table"),
		persistence.ValueParam(
			"$shard_id", persistence.Uint64Value(uint64(chunkIndex))),
		persistence.ValueParam(
			"$chunk_id",
			persistence.UTF8Value(fmt.Sprintf("chunk_%d", chunkIndex)),
		),
		persistence.ValueParam(
			"$data",
			persistence.StringValue(dataToWrite),
		),
	)
	return err
}

////////////////////////////////////////////////////////////////////////////////

func TestYDBRequestDoesNotHang(t *testing.T) {
	// Test that reproduces the issue with hanging transactions in ydb after
	// parallel requests that write external blobs are cancelled.
	// See: https://github.com/ydb-platform/ydb-go-sdk/issues/1025
	// See: https://github.com/ydb-platform/nbs/issues/501
	// We need 50 iteration to guarantee for the bug to reproduce,
	// usually it takes less iterations.
	setupTestYdbRequestDoesNotHang(t)
	for i := 0; i < 50; i++ {
		func() {
			f := newYdbTestFixture(t)
			defer f.teardown()
			var errGrp errgroup.Group
			ctx, cancel := context.WithCancel(f.ctx)
			logging.Info(
				ctx,
				"Writing 100 chunks in parallel and cancelling the context",
			)
			for chunkIdex := 0; chunkIdex < 100; chunkIdex++ {
				chunkIndex := chunkIdex
				errGrp.Go(
					func() error {
						err := f.writeChunkData(ctx, chunkIndex)
						if err == nil {
							return nil
						}
						if strings.Contains(err.Error(), "context canceled") {
							return nil
						}

						return err
					},
				)
			}

			time.Sleep(
				common.RandomDuration(
					10*time.Millisecond,
					500*time.Millisecond,
				),
			)
			cancel()
			require.NoError(f.t, errGrp.Wait())

			logging.Info(
				f.ctx,
				"Write 100 more chunks to ensure that transactions do not hang",
			)
			transactionTimeout := time.Minute * 3
			ctx, cancel = context.WithTimeout(
				f.ctx,
				transactionTimeout,
			)
			defer cancel()

			for chunkIdex := 100; chunkIdex < 200; chunkIdex++ {
				chunkIndex := chunkIdex
				errGrp.Go(
					func() error {
						err := f.writeChunkData(ctx, chunkIndex)
						if err == nil {
							return nil
						}

						errorIsTimeout := strings.Contains(
							err.Error(),
							"context deadline exceeded",
						)
						if errorIsTimeout {
							return fmt.Errorf(
								"request to ydb took more than %v",
								transactionTimeout,
							)
						}

						return err
					},
				)
			}
			require.NoError(f.t, errGrp.Wait())
		}()
	}
}
