package dataplane

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/snapshot"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	config *config.DataplaneConfig,
	performanceConfig *performance_config.PerformanceConfig,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	nbsFactory nbs.Factory,
	storage storage.Storage,
	metricsRegistry metrics.Registry,
	snapshotStorageQuotaReporter snapshot.SnapshotStorageQuotaReporter,
	urlMetricsRegistry metrics.Registry,
	migrationDstStorage storage.Storage,
	useS3InMigration bool,
	followerS3 *backup.FollowerS3,
) error {

	err := taskRegistry.RegisterForExecution("dataplane.CreateSnapshotFromDisk", func() tasks.Task {
		return &createSnapshotFromDiskTask{
			config:            config,
			performanceConfig: performanceConfig,
			nbsFactory:        nbsFactory,
			storage:           storage,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("dataplane.CreateSnapshotFromSnapshot", func() tasks.Task {
		return &createSnapshotFromSnapshotTask{
			config:            config,
			performanceConfig: performanceConfig,
			storage:           storage,
		}
	})
	if err != nil {
		return err
	}

	httpClientTimeout, err := time.ParseDuration(config.GetHTTPClientTimeout())
	if err != nil {
		return err
	}

	httpClientMinRetryTimeout, err := time.ParseDuration(
		config.GetHTTPClientMinRetryTimeout(),
	)
	if err != nil {
		return err
	}

	httpClientMaxRetryTimeout, err := time.ParseDuration(
		config.GetHTTPClientMaxRetryTimeout(),
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("dataplane.CreateSnapshotFromURL", func() tasks.Task {
		return &createSnapshotFromURLTask{
			config:                    config,
			performanceConfig:         performanceConfig,
			storage:                   storage,
			httpClientTimeout:         httpClientTimeout,
			httpClientMinRetryTimeout: httpClientMinRetryTimeout,
			httpClientMaxRetryTimeout: httpClientMaxRetryTimeout,
			metricsRegistry:           urlMetricsRegistry,
		}
	})
	if err != nil {
		return err
	}

	if migrationDstStorage != nil {
		err = taskRegistry.RegisterForExecution("dataplane.MigrateSnapshotTask", func() tasks.Task {
			return &migrateSnapshotTask{
				config:     config,
				srcStorage: storage,
				dstStorage: migrationDstStorage,
				useS3:      useS3InMigration,
			}
		})
		if err != nil {
			return err
		}

		err = taskRegistry.RegisterForExecution("dataplane.MigrateSnapshotDatabaseTask", func() tasks.Task {
			return &migrateSnapshotDatabaseTask{
				config:     config,
				registry:   metricsRegistry,
				srcStorage: storage,
				dstStorage: migrationDstStorage,
				scheduler:  taskScheduler,
			}
		})
		if err != nil {
			return err
		}
	}

	err = taskRegistry.RegisterForExecution("dataplane.TransferFromSnapshotToDisk", func() tasks.Task {
		return &transferFromSnapshotToDiskTask{
			config:            config,
			performanceConfig: performanceConfig,
			nbsFactory:        nbsFactory,
			storage:           storage,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("dataplane.TransferFromDiskToDisk", func() tasks.Task {
		return &transferFromDiskToDiskTask{
			config:            config,
			performanceConfig: performanceConfig,
			nbsFactory:        nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("dataplane.ReplicateDisk", func() tasks.Task {
		return &replicateDiskTask{
			config:            config,
			performanceConfig: performanceConfig,
			nbsFactory:        nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("dataplane.DeleteSnapshot", func() tasks.Task {
		return &deleteSnapshotTask{
			storage:    storage,
			nbsFactory: nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("dataplane.DeleteSnapshotData", func() tasks.Task {
		return &deleteSnapshotDataTask{
			performanceConfig: performanceConfig,
			storage:           storage,
		}
	})
	if err != nil {
		return err
	}

	snapshotCollectionTimeout, err := time.ParseDuration(
		config.GetSnapshotCollectionTimeout(),
	)
	if err != nil {
		return err
	}

	collectSnapshotsTaskScheduleInterval, err := time.ParseDuration(
		config.GetCollectSnapshotsTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	backupChunksTaskScheduleInterval, err := time.ParseDuration(
		config.GetBackupChunksTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	deleteBackupObjectsTaskScheduleInterval, err := time.ParseDuration(
		config.GetDeleteBackupObjectsTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"dataplane.CollectSnapshots",
		func() tasks.Task {
			return &collectSnapshotsTask{
				scheduler:                       taskScheduler,
				storage:                         storage,
				snapshotCollectionTimeout:       snapshotCollectionTimeout,
				snapshotCollectionInflightLimit: int(config.GetSnapshotCollectionInflightLimit()),
			}
		},
	)
	if err != nil {
		return err
	}

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"dataplane.CollectSnapshots",
		tasks.TaskSchedule{
			ScheduleInterval: collectSnapshotsTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	snapshotMetricsCollectionInterval, err := time.ParseDuration(
		config.GetSnapshotMetricsCollectionInterval(),
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"dataplane.CollectSnapshotMetrics",
		func() tasks.Task {
			return &collectSnapshotMetricsTask{
				registry:                  metricsRegistry,
				storage:                   storage,
				storageQuotaReporter:      snapshotStorageQuotaReporter,
				metricsCollectionInterval: snapshotMetricsCollectionInterval,
				backupEnabled:             followerS3 != nil,
			}
		},
	)
	if err != nil {
		return err
	}

	collectSnapshotMetricsTaskScheduleInterval, err := time.ParseDuration(
		config.GetCollectSnapshotMetricsTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"dataplane.CollectSnapshotMetrics",
		tasks.TaskSchedule{
			ScheduleInterval: collectSnapshotMetricsTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	err = taskRegistry.RegisterForExecution(
		"dataplane.DeleteDiskFromIncremental",
		func() tasks.Task {
			return &deleteDiskFromIncrementalTask{
				config:  config,
				storage: storage,
			}
		},
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"dataplane.CreateDRBasedDiskCheckpoint",
		func() tasks.Task {
			return &createDRBasedDiskCheckpointTask{
				performanceConfig: performanceConfig,
				nbsFactory:        nbsFactory,
			}
		},
	)
	if err != nil {
		return err
	}

	if followerS3 != nil {
		err = taskRegistry.RegisterForExecution(
			"dataplane.BackupSnapshotChunks",
			func() tasks.Task {
				return &backupSnapshotChunksTask{
					storage:    storage,
					followerS3: followerS3,
					batchSize: int(
						config.GetBackupSnapshotChunksBatchSize(),
					),
				}
			},
		)
		if err != nil {
			return err
		}

		err = taskRegistry.RegisterForExecution(
			"dataplane.BackupChunks",
			func() tasks.Task {
				return &backupChunksTask{
					storage:     storage,
					followerS3:  followerS3,
					batchSize:   int(config.GetBackupChunksTaskBatchSize()),
					workerCount: int(config.GetBackupChunksTaskWorkerCount()),
					registry:    metricsRegistry,
				}
			},
		)
		if err != nil {
			return err
		}

		taskScheduler.ScheduleRegularTasks(
			ctx,
			"dataplane.BackupChunks",
			tasks.TaskSchedule{
				ScheduleInterval: backupChunksTaskScheduleInterval,
				MaxTasksInflight: 1,
			},
		)

		err = taskRegistry.RegisterForExecution(
			"dataplane.DeleteBackupObjects",
			func() tasks.Task {
				return &deleteBackupObjectsTask{
					storage:    storage,
					followerS3: followerS3,
					batchSize: int(
						config.GetDeleteBackupObjectsTaskBatchSize(),
					),
				}
			},
		)
		if err != nil {
			return err
		}

		taskScheduler.ScheduleRegularTasks(
			ctx,
			"dataplane.DeleteBackupObjects",
			tasks.TaskSchedule{
				ScheduleInterval: deleteBackupObjectsTaskScheduleInterval,
				MaxTasksInflight: 1,
			},
		)
	}

	return nil
}

func Register(ctx context.Context, taskRegistry *tasks.Registry) error {
	for taskType, newTask := range newTaskByTaskType {
		err := taskRegistry.Register(taskType, newTask)
		if err != nil {
			return err
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

var newTaskByTaskType = map[string]func() tasks.Task{
	"dataplane.CreateSnapshotFromDisk":      func() tasks.Task { return &createSnapshotFromDiskTask{} },
	"dataplane.CreateSnapshotFromSnapshot":  func() tasks.Task { return &createSnapshotFromSnapshotTask{} },
	"dataplane.CreateSnapshotFromURL":       func() tasks.Task { return &createSnapshotFromURLTask{} },
	"dataplane.MigrateSnapshotTask":         func() tasks.Task { return &migrateSnapshotTask{} },
	"dataplane.MigrateSnapshotDatabaseTask": func() tasks.Task { return &migrateSnapshotDatabaseTask{} },
	"dataplane.TransferFromSnapshotToDisk":  func() tasks.Task { return &transferFromSnapshotToDiskTask{} },
	"dataplane.TransferFromDiskToDisk":      func() tasks.Task { return &transferFromDiskToDiskTask{} },
	"dataplane.ReplicateDisk":               func() tasks.Task { return &replicateDiskTask{} },
	"dataplane.DeleteSnapshot":              func() tasks.Task { return &deleteSnapshotTask{} },
	"dataplane.DeleteSnapshotData":          func() tasks.Task { return &deleteSnapshotDataTask{} },
	"dataplane.DeleteDiskFromIncremental":   func() tasks.Task { return &deleteDiskFromIncrementalTask{} },
	"dataplane.CreateDRBasedDiskCheckpoint": func() tasks.Task { return &createDRBasedDiskCheckpointTask{} },
	"dataplane.BackupSnapshotChunks":        func() tasks.Task { return &backupSnapshotChunksTask{} },
	"dataplane.BackupChunks":                func() tasks.Task { return &backupChunksTask{} },
	"dataplane.DeleteBackupObjects":         func() tasks.Task { return &deleteBackupObjectsTask{} },
}
