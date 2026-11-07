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

func TestExportPartsStorage(t *testing.T) {
	ctx := logging.SetLogger(
		context.Background(), logging.NewStderrLogger(logging.DebugLevel),
	)
	db, err := persistence.NewYDBClient(ctx, &persistence_config.PersistenceConfig{
		Endpoint: proto.String(
			"localhost:" + os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
		),
		Database: proto.String("/Root"), RootPath: proto.String("disk_manager"),
		ConnectionTimeout: proto.String("10s"),
	}, metrics.NewEmptyRegistry())
	require.NoError(t, err)
	defer db.Close(ctx)
	folder := "export_parts_test"
	require.NoError(t, tasks_storage.CreateYDBTables(
		ctx, &tasks_config.TasksConfig{StorageFolder: &folder}, db, false,
	))
	require.NoError(t, export_storage.CreateYDBTables(ctx, db, folder, false))
	store := export_storage.NewStorage(db, folder)
	var parts []exporter.S3ManifestPart
	for n := uint32(1); n <= 5; n++ {
		sum := sha256.Sum256([]byte(fmt.Sprint(n)))
		parts = append(parts, exporter.S3ManifestPart{
			Number: n, Key: fmt.Sprintf("part-%08d.raw.lz4", n),
			Offset: uint64(n-1) * 64,
			Length: 64, StoredLength: uint64(n), SHA256: hex.EncodeToString(sum[:]),
		})
	}
	rw := func(f func(*persistence.Transaction) error) error {
		execute := func(
			ctx context.Context,
			session *persistence.Session,
		) error {

			tx, err := session.BeginRWTransaction(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback(ctx)
			if err := f(tx); err != nil {
				return err
			}
			return tx.Commit(ctx)
		}
		return db.Execute(ctx, execute)
	}
	require.NoError(t, rw(func(tx *persistence.Transaction) error {
		if err := store.UpsertParts(ctx, tx, "task-a", parts); err != nil {
			return err
		}
		return store.UpsertParts(ctx, tx, "task-b", parts[:1])
	},
	))
	// Lost acknowledgements may repeat the same confirmation.
	require.NoError(t, rw(func(tx *persistence.Transaction) error {
		return store.UpsertParts(ctx, tx, "task-a", parts[:1])
	},
	))
	page, err := store.ReadParts(ctx, "task-a", 0, 2)
	require.NoError(t, err)
	require.Equal(t, parts[:2], page)
	page, err = store.ReadParts(ctx, "task-a", 2, 10)
	require.NoError(t, err)
	require.Equal(t, parts[2:], page)

	// A failed task-generation check must roll back its preparation too.
	err = rw(func(tx *persistence.Transaction) error {
		if err := store.UpsertParts(ctx, tx, "stale-task", parts); err != nil {
			return err
		}
		return fmt.Errorf("generation changed")
	})
	require.Error(t, err)
	page, err = store.ReadParts(ctx, "stale-task", 0, 10)
	require.NoError(t, err)
	require.Empty(t, page)

	for _, expected := range []uint64{2, 2, 1, 0} {
		require.NoError(t, rw(func(tx *persistence.Transaction) error {
			deleted, err := store.DeleteParts(ctx, tx, "task-a", 2)
			require.Equal(t, expected, deleted)
			return err
		},
		))
	}
	page, err = store.ReadParts(ctx, "task-a", 0, 10)
	require.NoError(t, err)
	require.Empty(t, page)
	page, err = store.ReadParts(ctx, "task-b", 0, 10)
	require.NoError(t, err)
	require.Equal(t, parts[:1], page)
}
