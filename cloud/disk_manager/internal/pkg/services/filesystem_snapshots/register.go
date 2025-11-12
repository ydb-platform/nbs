package filesystem_snapshots

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
) error {
	err := taskRegistry.RegisterForExecution(
		"filesystem_snapshots.CreateFilesystemSnapshot",
		func() tasks.Task {
			return &createFilesystemSnapshotTask{
				scheduler: taskScheduler,
			}
		},
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"filesystem_snapshots.DeleteFilesystemSnapshot",
		func() tasks.Task {
			return &deleteFilesystemSnapshotTask{
				scheduler: taskScheduler,
			}
		})
	if err != nil {
		return err
	}

	return nil
}
