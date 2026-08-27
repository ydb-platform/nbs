package chunks

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	monitoring_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

////////////////////////////////////////////////////////////////////////////////

type TestCase struct {
	name  string
	useS3 bool
}

func testCases() []TestCase {
	return []TestCase{
		{
			name:  "ydb storage",
			useS3: false,
		},
		{
			name:  "s3 storage",
			useS3: true,
		},
	}
}

func newStorage(
	db *persistence.YDBClient,
	s3 *persistence.S3Client,
	config *snapshot_config.SnapshotConfig,
	useS3 bool,
) Storage {

	tablesPath := db.AbsolutePath(config.GetStorageFolder())
	metrics := metrics.New(monitoring_metrics.NewEmptyRegistry(), "storage")

	if useS3 {
		return NewStorageS3(
			db,
			s3,
			config.GetS3Bucket(),
			config.GetChunkBlobsS3KeyPrefix(),
			tablesPath,
			config.GetChunkBlobsTableName(),
			config.GetChunkBlobsShadowTableName(),
			metrics,
			map[string]uint32{
				"gzip": 0,
			},
		)
	} else {
		return NewStorageYDB(
			db,
			tablesPath,
			config.GetChunkBlobsTableName(),
			config.GetChunkBlobsShadowTableName(),
			metrics,
			map[string]uint32{
				"gzip": 0,
			},
		)
	}
}

func setupEnvironment(
	t *testing.T,
) (context.Context, *persistence.YDBClient, *persistence.S3Client, *snapshot_config.SnapshotConfig) {

	ctx := test.NewContext()

	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
	)
	database := "/Root"
	rootPath := "disk_manager"
	connectionTimeout := "10s"

	db, err := persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint:          &endpoint,
			Database:          &database,
			RootPath:          &rootPath,
			ConnectionTimeout: &connectionTimeout,
		},
		monitoring_metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	s3, err := test.NewS3Client()
	require.NoError(t, err)

	storageFolder := fmt.Sprintf("snapshot_chunk_storage_test/%v", t.Name())
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

	err = schema.Create(ctx, config, db, s3, false /* dropUnusedColumns */)
	require.NoError(t, err)

	return ctx, db, s3, config
}

////////////////////////////////////////////////////////////////////////////////

func chunkDataExists(
	t *testing.T,
	ctx context.Context,
	s3 *persistence.S3Client,
	db *persistence.YDBClient,
	config *snapshot_config.SnapshotConfig,
	chunkID string,
	useS3 bool,
) bool {

	if useS3 {
		return chunkDataExistsInS3(t, ctx, s3, config, chunkID)
	} else {
		return chunkDataExistsInYDB(t, ctx, db, config, chunkID)
	}
}

func chunkDataExistsInS3(
	t *testing.T,
	ctx context.Context,
	s3 *persistence.S3Client,
	config *snapshot_config.SnapshotConfig,
	chunkID string,
) bool {

	_, err := s3.GetObject(
		ctx,
		config.GetS3Bucket(),
		test.NewS3Key(config, chunkID),
	)
	if err == nil {
		return true
	}

	require.ErrorContains(t, err, "s3 object not found")
	return false
}

func chunkDataExistsInYDB(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	config *snapshot_config.SnapshotConfig,
	chunkID string,
) bool {

	res, err := db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%[1]v";
		declare $chunk_id as Utf8;

		select *
		from %[2]v
		where chunk_id = $chunk_id and referer = "";
	`, db.AbsolutePath(config.GetStorageFolder()), config.GetChunkBlobsTableName()),
		persistence.ValueParam("$chunk_id", persistence.UTF8Value(chunkID)),
	)
	require.NoError(t, err)
	defer res.Close()

	if !res.NextResultSet(ctx) {
		return false
	}

	return res.NextRow()
}

func writeTestChunk(
	t *testing.T,
	ctx context.Context,
	storage Storage,
) (string, string, error) {

	referer := "testReferer"
	chunkID := "testChunkID"

	err := storage.WriteChunk(ctx, referer, common.Chunk{
		ID:          chunkID,
		Data:        []byte("test data"),
		Compression: "lz4",
	})
	require.NoError(t, err)

	return referer, chunkID, nil
}

func deleteMetadata(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	config *snapshot_config.SnapshotConfig,
	chunkID string,
) {

	_, err := db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%[1]v";
		pragma AnsiInForEmptyOrNullableItemsCollections;
		declare $chunk_id as Utf8;

		delete from %[2]v
		where chunk_id = $chunk_id and
			referer = "" and
			refcnt <= 1;
	`, db.AbsolutePath(config.GetStorageFolder()), config.GetChunkBlobsTableName()),
		persistence.ValueParam("$chunk_id", persistence.UTF8Value(chunkID)),
	)
	require.NoError(t, err)
}

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

// Checks that the shadow copy repeats the main table: the chunk has the
// expected reference count in both (zero means the chunk is absent).
func requireSameRefCount(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	config *snapshot_config.SnapshotConfig,
	chunkID string,
	expected uint32,
) {

	tableNames := []string{
		config.GetChunkBlobsTableName(),
		config.GetChunkBlobsShadowTableName(),
	}
	for _, tableName := range tableNames {
		res, err := db.ExecuteRO(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%[1]v";
			declare $chunk_id as Utf8;

			select refcnt
			from %[2]v
			where chunk_id = $chunk_id and referer = "";
		`, db.AbsolutePath(config.GetStorageFolder()), tableName),
			persistence.ValueParam("$chunk_id", persistence.UTF8Value(chunkID)),
		)
		require.NoError(t, err)
		defer res.Close()

		var refCount uint32
		if res.NextResultSet(ctx) && res.NextRow() {
			err = res.ScanNamed(
				persistence.OptionalWithDefault("refcnt", &refCount),
			)
			require.NoError(t, err)
		}
		require.Equal(t, expected, refCount, "table %v", tableName)
	}
}

func TestWriteIdempotency(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, db, s3, config := setupEnvironment(t)
			storage := newStorage(db, s3, config, testCase.useS3)

			for i := 0; i < 2; i++ {
				_, _, err := writeTestChunk(t, ctx, storage)
				require.NoError(t, err)
			}
		})
	}
}

func TestWriteChunkWithOverriddenTableName(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, db, s3, config := setupEnvironment(t)

			tableName := "chunk_blobs_v2"
			config.ChunkBlobsTableName = &tableName
			err := schema.Create(ctx, config, db, s3, false /* dropUnusedColumns */)
			require.NoError(t, err)

			storage := newStorage(db, s3, config, testCase.useS3)

			_, chunkID, err := writeTestChunk(t, ctx, storage)
			require.NoError(t, err)
			require.True(t, chunkDataExists(t, ctx, s3, db, config, chunkID, testCase.useS3))
		})
	}
}

func TestShadowTableRepeatsWrites(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, db, s3, config := setupEnvironment(t)

			shadowTableName := "chunk_blobs_shadow"
			config.ChunkBlobsShadowTableName = &shadowTableName
			err := schema.Create(ctx, config, db, s3, false /* dropUnusedColumns */)
			require.NoError(t, err)

			storage := newStorage(db, s3, config, testCase.useS3)

			firstReferer, chunkID, err := writeTestChunk(t, ctx, storage)
			require.NoError(t, err)
			requireSameRefCount(t, ctx, db, config, chunkID, 1)

			secondReferer := "secondReferer"
			err = storage.RefChunk(ctx, secondReferer, chunkID)
			require.NoError(t, err)
			requireSameRefCount(t, ctx, db, config, chunkID, 2)

			err = storage.UnrefChunk(ctx, firstReferer, chunkID)
			require.NoError(t, err)
			requireSameRefCount(t, ctx, db, config, chunkID, 1)

			// The last unref deletes the chunk from both tables.
			err = storage.UnrefChunk(ctx, secondReferer, chunkID)
			require.NoError(t, err)
			requireSameRefCount(t, ctx, db, config, chunkID, 0)
		})
	}
}

func TestRefIdempotency(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, db, s3, config := setupEnvironment(t)
			storage := newStorage(db, s3, config, testCase.useS3)

			_, chunkID, err := writeTestChunk(t, ctx, storage)
			require.NoError(t, err)

			for i := 0; i < 2; i++ {
				err := storage.RefChunk(ctx, "newReferer", chunkID)
				require.NoError(t, err)
			}
		})
	}
}

func TestUnrefIdempotency(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, db, s3, config := setupEnvironment(t)
			storage := newStorage(db, s3, config, testCase.useS3)

			firstReferer, chunkID, err := writeTestChunk(t, ctx, storage)
			require.NoError(t, err)
			require.True(t, chunkDataExists(t, ctx, s3, db, config, chunkID, testCase.useS3))

			secondReferer := "secondReferer"
			err = storage.RefChunk(ctx, secondReferer, chunkID)
			require.NoError(t, err)
			require.True(t, chunkDataExists(t, ctx, s3, db, config, chunkID, testCase.useS3))

			for i := 0; i < 2; i++ {
				err = storage.UnrefChunk(ctx, firstReferer, chunkID)
				require.NoError(t, err)
				require.True(t, chunkDataExists(t, ctx, s3, db, config, chunkID, testCase.useS3))
			}

			for i := 0; i < 2; i++ {
				err = storage.UnrefChunk(ctx, secondReferer, chunkID)
				require.NoError(t, err)
				require.False(t, chunkDataExists(t, ctx, s3, db, config, chunkID, testCase.useS3))
			}
		})
	}
}

func TestLastUnrefShouldDeleteDataEvenIfMetadataIsAbsent(t *testing.T) {
	ctx, db, s3, config := setupEnvironment(t)
	storage := newStorage(db, s3, config, true)

	referer, chunkID, err := writeTestChunk(t, ctx, storage)
	require.NoError(t, err)
	require.True(t, chunkDataExists(t, ctx, s3, db, config, chunkID, true))

	deleteMetadata(t, ctx, db, config, chunkID)
	require.True(t, chunkDataExists(t, ctx, s3, db, config, chunkID, true))

	err = storage.UnrefChunk(ctx, referer, chunkID)
	require.NoError(t, err)
	require.False(t, chunkDataExists(t, ctx, s3, db, config, chunkID, true))
}

func TestS3BucketExists(t *testing.T) {
	ctx, _, s3, config := setupEnvironment(t)

	exists, err := s3.BucketExists(ctx, config.GetS3Bucket())
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = s3.BucketExists(ctx, "nonexistent-bucket")
	require.NoError(t, err)
	require.False(t, exists)
}

////////////////////////////////////////////////////////////////////////////////

func writeChunkAndRequireS3StorageClass(
	t *testing.T,
	ctx context.Context,
	storage Storage,
	s3 *persistence.S3Client,
	config *snapshot_config.SnapshotConfig,
	referer string,
	chunkID string,
	storageClass string,
	expectedStorageClass string,
) {

	err := storage.WriteChunk(ctx, referer, common.Chunk{
		ID:           chunkID,
		Data:         []byte("12345678"),
		Compression:  "",
		StorageClass: storageClass,
	})
	require.NoError(t, err)

	obj, err := s3.GetObject(
		ctx,
		config.GetS3Bucket(),
		test.NewS3Key(config, chunkID),
	)
	require.NoError(t, err)
	require.Equal(t, expectedStorageClass, obj.StorageClass)
}

func TestS3StorageClass(t *testing.T) {
	ctx, db, s3, config := setupEnvironment(t)
	storage := newStorage(db, s3, config, true)

	writeChunkAndRequireS3StorageClass(
		t,
		ctx,
		storage,
		s3,
		config,
		"intelligentReferer",
		"intelligentChunkID",
		"INTELLIGENT_TIERING",
		"INTELLIGENT_TIERING",
	)

	writeChunkAndRequireS3StorageClass(
		t,
		ctx,
		storage,
		s3,
		config,
		"standardIAReferer1",
		"standardIAChunkID1",
		"STANDARD_IA",
		"STANDARD_IA",
	)

	writeChunkAndRequireS3StorageClass(
		t,
		ctx,
		storage,
		s3,
		config,
		"standardIAReferer2",
		"standardIAChunkID2",
		"STANDARD_IA",
		"STANDARD",
	)
}
