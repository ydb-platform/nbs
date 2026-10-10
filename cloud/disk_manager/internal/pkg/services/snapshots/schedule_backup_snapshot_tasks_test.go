package snapshots

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/operation"
)

////////////////////////////////////////////////////////////////////////////////

func newScheduleBackupSnapshotTasksTest(
	limit int,
	inflightLimit int,
) (
	context.Context,
	*resources_mocks.StorageMock,
	*tasks_mocks.SchedulerMock,
	*scheduleBackupSnapshotTasks,
) {

	ctx := logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()

	task := &scheduleBackupSnapshotTasks{
		scheduler:     scheduler,
		storage:       storage,
		registry:      metrics.NewEmptyRegistry(),
		limit:         limit,
		inflightLimit: inflightLimit,
	}
	return ctx, storage, scheduler, task
}

func expectBackupScheduled(
	storage *resources_mocks.StorageMock,
	scheduler *tasks_mocks.SchedulerMock,
	snapshotID string,
	backupID string,
) {

	taskID := snapshotID + "_task"
	scheduler.On(
		"ScheduleTask",
		mock.MatchedBy(func(ctx context.Context) bool {
			return headers.GetIdempotencyKey(ctx) ==
				"backup_snapshot_"+snapshotID+"_"+backupID
		}),
		"snapshots.BackupSnapshot",
		"",
		mock.MatchedBy(func(request *protos.BackupSnapshotRequest) bool {
			return request.SnapshotId == snapshotID &&
				request.BackupId == backupID
		}),
	).Return(taskID, nil).Once()

	storage.On(
		"SnapshotBackupScheduled",
		mock.Anything,
		snapshotID,
		backupID,
		taskID,
	).Return(time.Now().Add(-time.Minute), nil).Once()
}

func expectRunningCopies(
	storage *resources_mocks.StorageMock,
	scheduler *tasks_mocks.SchedulerMock,
	count int,
) {

	var scheduled []resources.ScheduledSnapshotBackup
	for i := 0; i < count; i++ {
		taskID := fmt.Sprintf("running%v", i)
		scheduled = append(scheduled, resources.ScheduledSnapshotBackup{
			SnapshotID: fmt.Sprintf("running_snap%v", i),
			BackupID:   "attempt",
			TaskID:     taskID,
		})
		scheduler.On("GetOperation", mock.Anything, taskID).Return(
			&operation.Operation{Id: taskID, Done: false},
			nil,
		).Once()
	}

	storage.On("ListScheduledSnapshotBackups", mock.Anything).Return(
		scheduled,
		nil,
	).Once()
}

////////////////////////////////////////////////////////////////////////////////

func TestScheduleBackupSnapshotTasks(t *testing.T) {
	ctx, storage, scheduler, task := newScheduleBackupSnapshotTasksTest(
		2, // limit
		0, // inflightLimit
	)
	execCtx := tasks_mocks.NewExecutionContextMock()

	expectRunningCopies(storage, scheduler, 0)
	storage.On("ListSnapshotsToBackup", mock.Anything, 2).Return(
		[]resources.SnapshotBackupRequest{
			{SnapshotID: "snap1", BackupID: "attempt1"},
			{SnapshotID: "snap2", BackupID: "attempt2"},
		},
		nil,
	)
	expectBackupScheduled(storage, scheduler, "snap1", "attempt1")
	expectBackupScheduled(storage, scheduler, "snap2", "attempt2")

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
}

func TestScheduleBackupSnapshotTasksStartsOnlyFreeSlots(t *testing.T) {
	ctx, storage, scheduler, task := newScheduleBackupSnapshotTasksTest(
		1000, // limit
		3,    // inflightLimit
	)
	execCtx := tasks_mocks.NewExecutionContextMock()

	// Two copies run, so one more may start.
	expectRunningCopies(storage, scheduler, 2)
	storage.On("ListSnapshotsToBackup", mock.Anything, 1).Return(
		[]resources.SnapshotBackupRequest{
			{SnapshotID: "snap1", BackupID: "attempt1"},
		},
		nil,
	)
	expectBackupScheduled(storage, scheduler, "snap1", "attempt1")

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
}

func TestScheduleBackupSnapshotTasksStartsNothingWhenSlotsAreTaken(
	t *testing.T,
) {

	ctx, storage, scheduler, task := newScheduleBackupSnapshotTasksTest(
		1000, // limit
		3,    // inflightLimit
	)
	execCtx := tasks_mocks.NewExecutionContextMock()

	expectRunningCopies(storage, scheduler, 3)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
	storage.AssertNotCalled(
		t,
		"ListSnapshotsToBackup",
		mock.Anything,
		mock.Anything,
	)
}

func TestScheduleBackupSnapshotTasksFreesSlotsOfEndedTasks(t *testing.T) {
	ctx, storage, scheduler, task := newScheduleBackupSnapshotTasksTest(
		1000, // limit
		2,    // inflightLimit
	)
	execCtx := tasks_mocks.NewExecutionContextMock()

	// Both tasks ended without removing their rows: one was force-finished,
	// the other was cleared from the task storage.
	storage.On("ListScheduledSnapshotBackups", mock.Anything).Return(
		[]resources.ScheduledSnapshotBackup{
			{SnapshotID: "snap1", BackupID: "attempt1", TaskID: "task1"},
			{SnapshotID: "snap2", BackupID: "attempt2", TaskID: "task2"},
		},
		nil,
	).Once()
	scheduler.On("GetOperation", mock.Anything, "task1").Return(
		&operation.Operation{Id: "task1", Done: true},
		nil,
	).Once()
	scheduler.On("GetOperation", mock.Anything, "task2").Return(
		nil,
		errors.NewNonRetriableError(errors.NewNotFoundErrorWithTaskID("task2")),
	).Once()
	storage.On(
		"RemoveSnapshotFromBackupQueue",
		mock.Anything,
		"snap1",
		"attempt1",
	).Return(nil).Once()
	storage.On(
		"RemoveSnapshotFromBackupQueue",
		mock.Anything,
		"snap2",
		"attempt2",
	).Return(nil).Once()

	storage.On("ListSnapshotsToBackup", mock.Anything, 2).Return(
		[]resources.SnapshotBackupRequest{
			{SnapshotID: "snap3", BackupID: "attempt3"},
		},
		nil,
	)
	expectBackupScheduled(storage, scheduler, "snap3", "attempt3")

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
}
