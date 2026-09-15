package dataplane

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	backup_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestCreateSnapshotFromDiskTaskSchedulesBackup(t *testing.T) {
	ctx := newContext()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task := &createSnapshotFromDiskTask{
		scheduler:     scheduler,
		backupEnabled: true,
		request: &protos.CreateSnapshotFromDiskRequest{
			DstSnapshotId: "snap1",
		},
	}

	execCtx.On("GetTaskID").Return("task1")
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.BackupSnapshot",
		"",
		mock.MatchedBy(func(request *backup_protos.BackupSnapshotRequest) bool {
			return request.SnapshotId == "snap1"
		}),
	).Return("backup-task", nil)

	err := task.scheduleBackup(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, scheduler, execCtx)
}

func TestCreateSnapshotFromDiskTaskWithoutBackup(t *testing.T) {
	ctx := newContext()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task := &createSnapshotFromDiskTask{
		scheduler: scheduler,
		request: &protos.CreateSnapshotFromDiskRequest{
			DstSnapshotId: "snap1",
		},
	}

	err := task.scheduleBackup(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, scheduler, execCtx)
}
