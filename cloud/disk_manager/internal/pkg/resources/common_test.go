package resources

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	disks_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
	ydb_status "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

func newYDB(ctx context.Context) (*persistence.YDBClient, error) {
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
	)
	database := "/Root"
	rootPath := "disk_manager"

	return persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint: &endpoint,
			Database: &database,
			RootPath: &rootPath,
		},
		metrics.NewEmptyRegistry(),
	)
}

func newStorage(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
) Storage {

	return newStorageWithBackupFolders(t, ctx, db, []string{"backed"})
}

func newStorageWithBackupFolders(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	backupFolderIDs []string,
) Storage {

	disksFolder := fmt.Sprintf("%v/disks", t.Name())
	imagesFolder := fmt.Sprintf("%v/images", t.Name())
	snapshotsFolder := fmt.Sprintf("%v/snapshots", t.Name())
	filesystemsFolder := fmt.Sprintf("%v/filesystems", t.Name())
	filesystemSnapshotsFolder := fmt.Sprintf(
		"%v/filesystem_snapshots",
		t.Name(),
	)
	placementGroupsFolder := fmt.Sprintf("%v/placement_groups", t.Name())

	err := CreateYDBTables(
		ctx,
		disksFolder,
		imagesFolder,
		snapshotsFolder,
		filesystemsFolder,
		filesystemSnapshotsFolder,
		placementGroupsFolder,
		db,
		false, // dropUnusedColumns
		true,
	)
	require.NoError(t, err)

	disksConfig := disks_config.DisksConfig{}
	endedMigrationExpirationTimeout, err := time.ParseDuration(
		disksConfig.GetEndedMigrationExpirationTimeout(),
	)
	require.NoError(t, err)

	storage, err := NewStorage(
		disksFolder,
		imagesFolder,
		snapshotsFolder,
		filesystemsFolder,
		filesystemSnapshotsFolder,
		placementGroupsFolder,
		db,
		endedMigrationExpirationTimeout,
		true, // backupEnabled
		backupFolderIDs,
	)
	require.NoError(t, err)

	return storage
}

////////////////////////////////////////////////////////////////////////////////

// Check the control-plane backup queues for images and snapshots. Dataplane
// chunk queues are covered by the snapshot storage schema tests.
func TestCreateAndDropResourceSchemaWithoutBackups(t *testing.T) {
	for _, kind := range []string{"images", "snapshots"} {
		t.Run(kind, func(t *testing.T) {
			ctx := newContext()
			db, err := newYDB(ctx)
			require.NoError(t, err)
			defer db.Close(ctx)

			folder := t.Name()

			idColumn := "image_id"
			create, drop := createImagesYDBTables, dropImagesYDBTables
			if kind == "snapshots" {
				idColumn = "snapshot_id"
				create, drop = createSnapshotsYDBTables, dropSnapshotsYDBTables
			}

			err = db.CreateOrAlterTable(
				ctx,
				folder,
				"backup_queue",
				persistence.NewCreateTableDescription(
					persistence.WithColumn(
						idColumn,
						persistence.Optional(persistence.TypeUTF8),
					),
					persistence.WithPrimaryKeyColumn(idColumn),
				),
				false, // dropUnusedColumns
			)
			require.NoError(t, err)

			err = create(
				ctx,
				folder,
				db,
				true,  // dropUnusedColumns
				false, // backupEnabled
			)
			require.NoError(t, err)

			// This column must not be added to the legacy backup table.
			query := fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				select backup_id from backup_queue;
			`, db.AbsolutePath(folder))
			res, err := db.ExecuteRO(ctx, query)
			if err == nil {
				res.Close()
			}
			require.True(
				t,
				ydb.IsOperationError(err, ydb_status.StatusIds_GENERIC_ERROR),
				"expected missing backup_id column, got: %v",
				err,
			)
			require.ErrorContains(t, err, "Member not found: backup_id")

			err = drop(ctx, folder, db, false /* backupEnabled */)
			require.NoError(t, err)

			query = fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				select %v from backup_queue;
			`, db.AbsolutePath(folder), idColumn)
			res, err = db.ExecuteRO(ctx, query)
			require.NoError(t, err)
			res.Close()
		})
	}
}
