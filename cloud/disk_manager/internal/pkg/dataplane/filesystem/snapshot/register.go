package snapshot

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/config"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage"
	nodes_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/nodes"
	traversal_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func validateConfig(config *snapshot_config.FilesystemSnapshotConfig) error {
	if config == nil {
		return errors.NewNonRetriableErrorf(
			"filesystem snapshot config should not be nil",
		)
	}

	if config.GetRestoreHardlinksBatchSize() == 0 {
		return errors.NewNonRetriableErrorf(
			"RestoreHardlinksBatchSize should not be zero",
		)
	}

	if config.GetFetchNodesFromStorageLimit() == 0 {
		return errors.NewNonRetriableErrorf(
			"FetchNodesFromStorageLimit should not be zero",
		)
	}

	if config.GetSnapshotDataDeletionLimit() == 0 {
		return errors.NewNonRetriableErrorf(
			"SnapshotDataDeletionLimit should not be zero",
		)
	}

	return nil
}

func Register(taskRegistry *tasks.Registry) error {
	err := taskRegistry.Register(
		"dataplane.DeleteFilesystemSnapshot",
		func() tasks.Task {
			return &deleteFilesystemSnapshotTask{}
		},
	)
	if err != nil {
		return err
	}

	err = taskRegistry.Register(
		"dataplane.DeleteFilesystemSnapshotData",
		func() tasks.Task {
			return &deleteFilesystemSnapshotDataTask{}
		},
	)
	if err != nil {
		return err
	}

	err = taskRegistry.Register(
		"dataplane.CollectFilesystemSnapshots",
		func() tasks.Task {
			return &collectFilesystemSnapshotsTask{}
		},
	)
	if err != nil {
		return err
	}

	err = taskRegistry.Register(
		"dataplane.TransferFromFilesystemToSnapshot",
		func() tasks.Task {
			return &transferFromFilesystemToSnapshotTask{}
		},
	)
	if err != nil {
		return err
	}

	return taskRegistry.Register(
		"dataplane.TransferFromSnapshotToFilesystem",
		func() tasks.Task {
			return &transferFromSnapshotToFilesystemTask{}
		},
	)
}

func RegisterForExecution(
	ctx context.Context,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	config *snapshot_config.FilesystemSnapshotConfig,
	factory nfs.Factory,
	storage snapshot_storage.Storage,
	traversalStorage traversal_storage.Storage,
	nodesStorage nodes_storage.Storage,
) error {

	err := validateConfig(config)
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

	err = taskRegistry.RegisterForExecution(
		"dataplane.DeleteFilesystemSnapshot",
		func() tasks.Task {
			return &deleteFilesystemSnapshotTask{
				storage: storage,
			}
		},
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"dataplane.DeleteFilesystemSnapshotData",
		func() tasks.Task {
			return &deleteFilesystemSnapshotDataTask{
				nodesStorage: nodesStorage,
			}
		},
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"dataplane.TransferFromFilesystemToSnapshot",
		func() tasks.Task {
			return &transferFromFilesystemToSnapshotTask{
				config:           config,
				factory:          factory,
				traversalStorage: traversalStorage,
				nodesStorage:     nodesStorage,
			}
		},
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"dataplane.TransferFromSnapshotToFilesystem",
		func() tasks.Task {
			return &transferFromSnapshotToFilesystemTask{
				config:           config,
				factory:          factory,
				traversalStorage: traversalStorage,
				nodesStorage:     nodesStorage,
			}
		},
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"dataplane.CollectFilesystemSnapshots",
		func() tasks.Task {
			return &collectFilesystemSnapshotsTask{
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
		"dataplane.CollectFilesystemSnapshots",
		tasks.TaskSchedule{
			ScheduleInterval: collectSnapshotsTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	return nil
}
