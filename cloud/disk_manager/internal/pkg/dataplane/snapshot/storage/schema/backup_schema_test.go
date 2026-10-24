package schema

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	pc "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

// Exercise schema ownership directly, so coverage is attached to this package,
// and prove both backup tables accept their actual durable record shapes.
func TestBackupSchemaCreateRepeatDrop(t *testing.T) {
	ctx := logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.InfoLevel))
	db, err := persistence.NewYDBClient(ctx, &pc.PersistenceConfig{
		Endpoint: proto.String("localhost:" + os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT")),
		Database: proto.String("/Root"), RootPath: proto.String("disk_manager"),
		ConnectionTimeout: proto.String("10s"),
	}, metrics.NewEmptyRegistry())
	require.NoError(t, err)
	defer db.Close(ctx)
	cfg := &config.SnapshotConfig{
		StorageFolder:             proto.String("backup_schema_test/" + t.Name()),
		ChunkBlobsTableShardCount: proto.Uint64(2), ChunkMapTableShardCount: proto.Uint64(2),
	}
	for trial := 0; trial < 2; trial++ {
		require.NoError(t, Create(ctx, cfg, db, nil, false))
		result, err := db.ExecuteRW(ctx, fmt.Sprintf(`
   --!syntax_v1
   pragma TablePathPrefix = "%s";
   UPSERT INTO backup_chunks (snapshot_id,chunk_id,status) VALUES ("snap","owned",1);
   UPSERT INTO backup_chunk_queue (snapshot_id,chunk_id,stored_in_s3,encrypted_dek)
    VALUES ("snap","owned",true,"opaque-dek");
  `, db.AbsolutePath(cfg.GetStorageFolder())))
		require.NoError(t, err)
		result.Close()
	}
	result, err := db.ExecuteRO(ctx, fmt.Sprintf(`
  --!syntax_v1
  pragma TablePathPrefix = "%s";
  SELECT snapshot_id,chunk_id,stored_in_s3,encrypted_dek FROM backup_chunk_queue;
 `, db.AbsolutePath(cfg.GetStorageFolder())))
	require.NoError(t, err)
	require.True(t, result.NextResultSet(ctx))
	require.True(t, result.NextRow())
	var id, chunk, key string
	var s3 bool
	require.NoError(t, result.ScanNamed(persistence.OptionalWithDefault("snapshot_id", &id), persistence.OptionalWithDefault("chunk_id", &chunk), persistence.OptionalWithDefault("stored_in_s3", &s3), persistence.OptionalWithDefault("encrypted_dek", &key)))
	require.Equal(t, "snap", id)
	require.Equal(t, "owned", chunk)
	require.True(t, s3)
	require.Equal(t, "opaque-dek", key)
	require.False(t, result.NextRow())
	result.Close()
	require.NoError(t, Drop(ctx, cfg, db))
	for _, table := range []string{"backup_chunks", "backup_chunk_queue"} {
		result, err := db.ExecuteRO(ctx, fmt.Sprintf("SELECT * FROM `%s/%s`;", db.AbsolutePath(cfg.GetStorageFolder()), table))
		if err == nil {
			result.Close()
		}
		require.Error(t, err)
	}
	require.NoError(t, Drop(ctx, cfg, db))
	require.NoError(t, Create(ctx, cfg, db, nil, false))
}
