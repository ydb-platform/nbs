package scrubbing

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

func Register(taskRegistry *tasks.Registry) error {
	err := taskRegistry.Register(
		"dataplane.ScrubFilesystem",
		func() tasks.Task {
			return &scrubFilesystemTask{}
		},
	)
	if err != nil {
		return err
	}

	return taskRegistry.Register(
		"dataplane.RegularScrubFilesystems",
		func() tasks.Task {
			return &regularScrubFilesystemsTask{}
		},
	)
}

func RegisterForExecution(
	taskRegistry *tasks.Registry,
	config *config.FilesystemScrubbingConfig,
	factory nfs.Factory,
	storage storage.Storage,
	scheduler tasks.Scheduler,
) error {

	err := taskRegistry.RegisterForExecution(
		"dataplane.ScrubFilesystem",
		func() tasks.Task {
			return &scrubFilesystemTask{
				config:   config,
				factory:  factory,
				storage:  storage,
				callback: func(nodes []nfs.Node) {},
			}
		},
	)
	if err != nil {
		return err
	}

	return taskRegistry.RegisterForExecution(
		"dataplane.RegularScrubFilesystems",
		func() tasks.Task {
			return &regularScrubFilesystemsTask{
				config:    config,
				scheduler: scheduler,
			}
		},
	)
}

func ScheduleScrubFilesystem(
	ctx context.Context,
	scheduler tasks.Scheduler,
	zoneID string,
	filesystemID string,
) (string, error) {

	return scheduler.ScheduleTask(
		ctx,
		"dataplane.ScrubFilesystem",
		"Traverse filesystem to check for inconsistencies",
		&protos.ScrubFilesystemRequest{
			Filesystem: &types.Filesystem{
				ZoneId:       zoneID,
				FilesystemId: filesystemID,
			},
		},
	)
}

func ScheduleRegularScrubFilesystems(
	ctx context.Context,
	scheduler tasks.Scheduler,
	config *config.FilesystemScrubbingConfig,
) error {

	if len(config.GetFilesystemsWithRegularScrubbingEnabled()) == 0 {
		return nil
	}

	scheduleInterval, err := time.ParseDuration(
		config.GetRegularFilesystemScrubbingSchedulingInterval(),
	)
	if err != nil {
		return err
	}

	scheduler.ScheduleRegularTasks(
		ctx,
		"dataplane.RegularScrubFilesystems",
		tasks.TaskSchedule{
			ScheduleInterval: scheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	return nil
}
