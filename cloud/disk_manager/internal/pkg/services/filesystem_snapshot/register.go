package filesystem_snapshot

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	cellSelector cells.CellSelector,
) error {
	err := taskRegistry.RegisterForExecution(
		"filesystem_snapshot.CreateFilesystemSnapshot",
		func() tasks.Task {
			return &createFilesystemSnapshotTask{
				scheduler:    taskScheduler,
				cellSelector: cellSelector,
			}
		},
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"filesystem_snapshot.DeleteFilesystemSnapshot",
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
