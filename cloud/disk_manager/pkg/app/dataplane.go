package app

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func initDataplane(
	ctx context.Context,
	config *server_config.ServerConfig,
	mon *monitoring.Monitoring,
	snapshotDB *persistence.YDBClient,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	nbsFactory nbs.Factory,
	s3 *persistence.S3Client,
	migrationDestinationDB *persistence.YDBClient,
	migrationDestinationS3 *persistence.S3Client,
) error {

	dataplaneConfig := config.GetDataplaneConfig()
	snapshotConfig := dataplaneConfig.GetSnapshotConfig()

	snapshotMetricsRegistry := mon.NewRegistry("snapshot_storage")

	snapshotStorage, err := snapshot_storage.NewStorage(
		snapshotConfig,
		snapshotMetricsRegistry,
		snapshotDB,
		s3,
	)
	if err != nil {
		return err
	}

	snapshotLegacyStorage := snapshot_storage.NewLegacyStorage(
		snapshotConfig,
		snapshotMetricsRegistry,
		snapshotDB,
	)
	migrationDestinationStorageConfig := dataplaneConfig.GetMigrationDestinationStorageConfig()
	var migrationDestinationStorage snapshot_storage.Storage
	if migrationDestinationDB != nil {
		migrationDestinationStorage, err = snapshot_storage.NewStorage(
			migrationDestinationStorageConfig,
			snapshotMetricsRegistry,
			migrationDestinationDB,
			migrationDestinationS3,
		)
		if err != nil {
			return err
		}
	}

	return dataplane.RegisterForExecution(
		ctx,
		taskRegistry,
		taskScheduler,
		nbsFactory,
		snapshotStorage,
		snapshotLegacyStorage,
		dataplaneConfig,
		snapshotMetricsRegistry,
		migrationDestinationStorage,
	)
}
