package app

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/disk_service"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

func initSnapshotExport(
	ctx context.Context,
	config *server_config.ServerConfig,
	mon *monitoring.Monitoring,
	creds auth.Credentials,
	registry *tasks.Registry,
	backupS3 *backup.S3,
) (func(), error) {

	// Controlplane hosts coordinate exports but never execute S3 transfers.
	if config.GetGrpcConfig() != nil {
		return func() {}, dataplane.RegisterExport(
			registry, nil, nil, nil, nil, nil,
		)
	}

	exportConfig := config.GetExportConfig()
	var storage snapshot_storage.Storage
	var diskServiceClient disk_service.Client
	var backupReader backup.ObjectReader
	var cleanups []func()
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	if exportConfig != nil {
		sourceRetryTimeout, err := time.ParseDuration(
			exportConfig.GetSourceRetryTimeout(),
		)
		if err != nil || sourceRetryTimeout <= 0 {
			return nil, fmt.Errorf("source retry timeout must be positive")
		}

		if err := exporter.ValidateS3PartSize(
			exportConfig.GetPartSize(),
		); err != nil {
			return nil, err
		}

		if err := exporter.ValidateReadWorkers(
			int(exportConfig.GetReadWorkerCount()),
		); err != nil {
			return nil, err
		}

		if err := exporter.ValidateS3UploadWorkers(
			int(exportConfig.GetUploadWorkerCount()),
		); err != nil {
			return nil, err
		}

		if err := exporter.ValidateS3Compression(
			exportConfig.GetCompression(),
		); err != nil {
			return nil, err
		}

		timeout, err := time.ParseDuration(exportConfig.GetS3CallTimeout())
		if err != nil || timeout <= 0 ||
			exportConfig.GetDiskServiceClientConfig() == nil {
			return nil, fmt.Errorf(
				"snapshot export requires DiskServiceClientConfig " +
					"and a positive S3 call timeout",
			)
		}

		var closeClient func() error
		diskServiceClient, closeClient, err = disk_service.NewClient(
			exportConfig.GetDiskServiceClientConfig(),
			creds,
		)
		if err != nil {
			return nil, err
		}

		cleanups = append(cleanups, func() { _ = closeClient() })
		source := exportConfig.GetSourceStorage()
		if source != server_config.ExportConfig_PRIMARY {
			if backupS3 == nil {
				cleanup()
				return nil, fmt.Errorf(
					"snapshot export source %v requires SnapshotStorageBackupConfig",
					source,
				)
			}

			backupReader = backupS3
		}

		if source != server_config.ExportConfig_BACKUP {
			snapshotConfig := exportConfig.GetSnapshotConfig()
			if snapshotConfig == nil {
				snapshotConfig = config.GetDataplaneConfig().GetSnapshotConfig()
			}

			if snapshotConfig == nil {
				cleanup()
				return nil, fmt.Errorf(
					"snapshot export source %v requires SnapshotConfig",
					source,
				)
			}

			sourceRegistry := mon.NewRegistry("export_snapshot_storage")
			snapshotDB, err := persistence.NewYDBClient(
				ctx,
				snapshotConfig.GetPersistenceConfig(),
				sourceRegistry,
				persistence.WithCredentials(creds),
			)
			if err != nil {
				cleanup()
				return nil, err
			}

			cleanups = append(cleanups, func() { _ = snapshotDB.Close(ctx) })
			var s3 *persistence.S3Client
			s3Config := snapshotConfig.GetPersistenceConfig().GetS3Config()
			if s3Config != nil {
				s3, err = persistence.NewS3ClientFromConfig(
					s3Config,
					mon.NewRegistry("export_source_s3"),
					nil,
					creds,
				)
				if err != nil {
					cleanup()
					return nil, err
				}
			}

			storage, err = snapshot_storage.NewStorage(
				snapshotConfig,
				sourceRegistry,
				snapshotDB,
				s3,
			)
			if err != nil {
				cleanup()
				return nil, err
			}
		}

		limits := config.GetTasksConfig().InflightTaskPerNodeLimits
		if limits == nil {
			limits = make(map[string]int64)
			config.GetTasksConfig().InflightTaskPerNodeLimits = limits
		}

		for _, taskType := range []string{
			"dataplane.ExportSnapshotToS3", "dataplane.ExportImageToS3",
		} {
			if _, configured := limits[taskType]; !configured {
				limits[taskType] = 1
			}
		}
	}

	err := dataplane.RegisterExport(
		registry, exportConfig, storage, backupReader, diskServiceClient,
		mon.NewRegistry("snapshot_export"),
	)
	if err != nil {
		cleanup()
		return nil, err
	}

	return cleanup, nil
}
