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
		placementGroupsFolder,
		db,
		false, // dropUnusedColumns
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
	)
	require.NoError(t, err)

	return storage
}
