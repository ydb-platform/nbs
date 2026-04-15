package app

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane"
	filesystem_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing"
	filesystem_snapshot "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot"
	nodes_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/nodes"
	traversal_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
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
	migrationDstDB *persistence.YDBClient,
	migrationDstS3 *persistence.S3Client,
) error {

	dataplaneConfig := config.GetDataplaneConfig()
	snapshotConfig := dataplaneConfig.GetSnapshotConfig()

	performanceConfig := config.PerformanceConfig
	if performanceConfig == nil {
		performanceConfig = &performance_config.PerformanceConfig{}
	}

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
	migrationDstSnapshotConfig := dataplaneConfig.GetMigrationDstSnapshotConfig()
	var migrationDstStorage snapshot_storage.Storage
	var useS3InSnapshotMigration bool
	if migrationDstDB != nil {
		migrationDstStorage, err = snapshot_storage.NewStorage(
			migrationDstSnapshotConfig,
			snapshotMetricsRegistry,
			migrationDstDB,
			migrationDstS3,
		)
		if migrationDstS3 != nil {
			useS3InSnapshotMigration = true
		}
		if err != nil {
			return err
		}
	}

	return dataplane.RegisterForExecution(
		ctx,
		dataplaneConfig,
		performanceConfig,
		taskRegistry,
		taskScheduler,
		nbsFactory,
		snapshotStorage,
		snapshotLegacyStorage,
		snapshotMetricsRegistry,
		migrationDstStorage,
		useS3InSnapshotMigration,
	)
}

func initFilesystemDataplane(
	ctx context.Context,
	config *server_config.ServerConfig,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	nfsFactory nfs.Factory,
	filesystemDB *persistence.YDBClient,
) error {

	filesystemConfig := config.GetDataplaneConfig().GetFilesystemConfig()
	if filesystemConfig == nil {
		return nil
	}

	err := initFilesystemScrubbing(
		ctx,
		taskRegistry,
		nfsFactory,
		filesystemDB,
		filesystemConfig,
		taskScheduler,
	)
	if err != nil {
		return err
	}

	return initFilesystemSnapshot(taskRegistry, nfsFactory, filesystemDB, filesystemConfig)
}

func initFilesystemScrubbing(
	ctx context.Context,
	taskRegistry *tasks.Registry,
	nfsFactory nfs.Factory,
	filesystemDB *persistence.YDBClient,
	filesystemConfig *filesystem_config.FilesystemDataplaneConfig,
	taskScheduler tasks.Scheduler,
) error {

	scrubbingConfig := filesystemConfig.GetScrubbingConfig()
	if scrubbingConfig == nil {
		return nil
	}

	traversalConfig := scrubbingConfig.GetTraversalConfig()
	if traversalConfig == nil {
		return nil
	}

	traversalStorage := traversal_storage.NewStorage(
		filesystemDB,
		traversalConfig.GetStorageFolder(),
	)

	err := scrubbing.RegisterForExecution(
		taskRegistry,
		scrubbingConfig,
		nfsFactory,
		traversalStorage,
		taskScheduler,
	)
	if err != nil {
		return err
	}

	return scrubbing.ScheduleRegularScrubFilesystems(
		ctx,
		taskScheduler,
		scrubbingConfig,
	)
}

func initFilesystemSnapshot(
	taskRegistry *tasks.Registry,
	nfsFactory nfs.Factory,
	filesystemDB *persistence.YDBClient,
	filesystemConfig *filesystem_config.FilesystemDataplaneConfig,
) error {

	snapshotConfig := filesystemConfig.GetSnapshotConfig()
	if snapshotConfig == nil {
		return nil
	}

	traversalConfig := snapshotConfig.GetTraversalConfig()
	if traversalConfig == nil {
		return nil
	}

	traversalStorage := traversal_storage.NewStorage(
		filesystemDB,
		traversalConfig.GetStorageFolder(),
	)

	nodesStorage := nodes_storage.NewStorage(
		filesystemDB,
		snapshotConfig.GetNodesStorageFolder(),
		int(snapshotConfig.GetSnapshotDataDeletionLimit()),
	)

	return filesystem_snapshot.RegisterForExecution(
		taskRegistry,
		snapshotConfig,
		nfsFactory,
		traversalStorage,
		nodesStorage,
	)
}
