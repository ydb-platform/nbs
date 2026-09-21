package snapshots

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestScheduleBackupSnapshotTasks(t *testing.T) {
	ctx := logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	storage.On("ListSnapshotsToBackup", mock.Anything, 2).Return(
		[]string{"snap1", "snap2"},
		nil,
	)

	for _, snapshotID := range []string{"snap1", "snap2"} {
		id := snapshotID
		scheduler.On(
			"ScheduleTask",
			mock.MatchedBy(func(ctx context.Context) bool {
				return headers.GetIdempotencyKey(ctx) == "backup_snapshot_"+id
			}),
			"snapshots.BackupSnapshot",
			"",
			mock.MatchedBy(func(request *protos.BackupSnapshotRequest) bool {
				return request.SnapshotId == id
			}),
		).Return(id+"_task", nil)
	}

	task := &scheduleBackupSnapshotTasks{
		scheduler: scheduler,
		storage:   storage,
		limit:     2,
	}

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
}
