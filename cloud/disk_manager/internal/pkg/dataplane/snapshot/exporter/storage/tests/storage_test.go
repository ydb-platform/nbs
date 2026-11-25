package tests

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
	export_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

func newYDB(t *testing.T, ctx context.Context) *persistence.YDBClient {
	endpoint := "localhost:" + os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT")

	db, err := persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint:          proto.String(endpoint),
			Database:          proto.String("/Root"),
			RootPath:          proto.String("disk_manager"),
			ConnectionTimeout: proto.String("10s"),
		},
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	return db
}

func newStorage(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	folder string,
) export_storage.Storage {

	err := tasks_storage.CreateYDBTables(
		ctx,
		&tasks_config.TasksConfig{StorageFolder: &folder},
		db,
		false, // dropUnusedColumns
	)
	require.NoError(t, err)

	err = export_storage.CreateYDBTables(
		ctx,
		db,
		folder,
		false, // dropUnusedColumns
	)
	require.NoError(t, err)

	return export_storage.NewStorage(db, folder)
}

func newParts(first uint32, last uint32) []exporter.S3ManifestPart {
	var parts []exporter.S3ManifestPart
	for n := first; n <= last; n++ {
		sum := sha256.Sum256([]byte(fmt.Sprint(n)))
		parts = append(parts, exporter.S3ManifestPart{
			Number:       n,
			Key:          fmt.Sprintf("part-%08d.raw.lz4", n),
			Offset:       uint64(n-1) * 64,
			Length:       64,
			StoredLength: uint64(n),
			SHA256:       hex.EncodeToString(sum[:]),
		})
	}

	return parts
}

////////////////////////////////////////////////////////////////////////////////

func TestExportPartsStorage(t *testing.T) {
	ctx := newContext()
	db := newYDB(t, ctx)
	defer db.Close(ctx)

	store := newStorage(t, ctx, db, "export_parts_test")
	parts := newParts(1, 5)

	err := store.UpsertPartsInOwnTransaction(ctx, "task-a", parts)
	require.NoError(t, err)

	err = store.UpsertPartsInOwnTransaction(ctx, "task-b", parts[:1])
	require.NoError(t, err)

	// Lost acknowledgements may repeat the same confirmation.
	err = store.UpsertPartsInOwnTransaction(ctx, "task-a", parts[:1])
	require.NoError(t, err)

	page, err := store.ReadParts(ctx, "task-a", 0, 2)
	require.NoError(t, err)
	require.Equal(t, parts[:2], page)

	page, err = store.ReadParts(ctx, "task-a", 2, 10)
	require.NoError(t, err)
	require.Equal(t, parts[2:], page)

	for _, expected := range []uint64{2, 2, 1, 0} {
		deleted, err := store.DeletePartsInOwnTransaction(ctx, "task-a", 2)
		require.NoError(t, err)
		require.Equal(t, expected, deleted)
	}

	page, err = store.ReadParts(ctx, "task-a", 0, 10)
	require.NoError(t, err)
	require.Empty(t, page)

	page, err = store.ReadParts(ctx, "task-b", 0, 10)
	require.NoError(t, err)
	require.Equal(t, parts[:1], page)
}

// By default YDB limits the result of a regular query to 1000 rows, and the
// client fails on a truncated result. Reading parts must not depend on that
// limit.
func TestExportPartsStorageReadsMoreRowsThanQueryResultLimit(t *testing.T) {
	ctx := newContext()
	db := newYDB(t, ctx)
	defer db.Close(ctx)

	store := newStorage(t, ctx, db, "export_parts_result_limit_test")
	taskID := "task"
	parts := newParts(1, 2001)

	batchSize := 500
	for start := 0; start < len(parts); start += batchSize {
		end := start + batchSize
		if end > len(parts) {
			end = len(parts)
		}

		err := store.UpsertPartsInOwnTransaction(
			ctx,
			taskID,
			parts[start:end],
		)
		require.NoError(t, err)
	}

	limit := uint64(len(parts))

	actual, err := store.ReadParts(ctx, taskID, 0, limit)
	require.NoError(t, err)
	require.Equal(t, parts, actual)

	actual, err = store.ReadParts(ctx, taskID, 500, limit)
	require.NoError(t, err)
	require.Equal(t, parts[500:], actual)
}
