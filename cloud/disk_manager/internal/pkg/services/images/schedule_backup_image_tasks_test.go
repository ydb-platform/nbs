package images

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
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func newScheduleBackupImageTasksTest(
	limit int,
	inflightLimit int,
) (
	context.Context,
	*resources_mocks.StorageMock,
	*tasks_mocks.SchedulerMock,
	*scheduleBackupImageTasks,
) {

	ctx := logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()

	task := &scheduleBackupImageTasks{
		scheduler:     scheduler,
		storage:       storage,
		registry:      metrics.NewEmptyRegistry(),
		limit:         limit,
		inflightLimit: inflightLimit,
	}
	return ctx, storage, scheduler, task
}

func expectImageBackupScheduled(
	storage *resources_mocks.StorageMock,
	scheduler *tasks_mocks.SchedulerMock,
	imageID string,
	backupID string,
	idempotencyKey string,
) {

	taskID := imageID + "_task"
	scheduler.On(
		"ScheduleTask",
		mock.MatchedBy(func(ctx context.Context) bool {
			return headers.GetIdempotencyKey(ctx) == idempotencyKey
		}),
		"images.BackupImage",
		"",
		mock.MatchedBy(func(request *protos.BackupImageRequest) bool {
			return request.ImageId == imageID &&
				request.BackupId == backupID
		}),
	).Return(taskID, nil).Once()

	storage.On(
		"ImageBackupScheduled",
		mock.Anything,
		imageID,
		backupID,
		taskID,
	).Return(time.Now().Add(-time.Minute), nil).Once()
}

func expectRunningImageCopies(
	storage *resources_mocks.StorageMock,
	scheduler *tasks_mocks.SchedulerMock,
	count int,
	ended []string,
) {

	var scheduled []resources.ScheduledImageBackup
	var taskIDs []string
	for i := 0; i < count; i++ {
		taskID := fmt.Sprintf("running%v", i)
		scheduled = append(scheduled, resources.ScheduledImageBackup{
			ImageID:  fmt.Sprintf("running_image%v", i),
			BackupID: "attempt",
			TaskID:   taskID,
		})
		taskIDs = append(taskIDs, taskID)
	}

	if count != 0 {
		scheduler.On("GetEndedTasks", mock.Anything, taskIDs).Return(
			ended,
			nil,
		).Once()
	}

	storage.On(
		"ListScheduledImageBackups",
		mock.Anything,
		mock.Anything,
	).Return(scheduled, nil).Once()
}

////////////////////////////////////////////////////////////////////////////////

func TestScheduleBackupImageTasks(t *testing.T) {
	ctx, storage, scheduler, task := newScheduleBackupImageTasksTest(
		2, // limit
		0, // inflightLimit
	)
	execCtx := tasks_mocks.NewExecutionContextMock()

	expectRunningImageCopies(storage, scheduler, 0, nil)
	storage.On("ListImagesToBackup", mock.Anything, 2).Return(
		[]resources.ImageBackupRequest{
			{ImageID: "image1", BackupID: "attempt1"},
			{ImageID: "image2", BackupID: "attempt2"},
		},
		nil,
	)
	expectImageBackupScheduled(
		storage,
		scheduler,
		"image1",
		"attempt1",
		"backup_image_image1_attempt1",
	)
	expectImageBackupScheduled(
		storage,
		scheduler,
		"image2",
		"attempt2",
		"backup_image_image2_attempt2",
	)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
}

func TestScheduleBackupImageTasksStartsOnlyFreeSlots(t *testing.T) {
	ctx, storage, scheduler, task := newScheduleBackupImageTasksTest(
		1000, // limit
		3,    // inflightLimit
	)
	execCtx := tasks_mocks.NewExecutionContextMock()

	expectRunningImageCopies(storage, scheduler, 2, nil)
	storage.On("ListImagesToBackup", mock.Anything, 1).Return(
		[]resources.ImageBackupRequest{
			{ImageID: "image1", BackupID: "attempt1"},
		},
		nil,
	)
	expectImageBackupScheduled(
		storage,
		scheduler,
		"image1",
		"attempt1",
		"backup_image_image1_attempt1",
	)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
}

func TestScheduleBackupImageTasksStartsNothingWhenSlotsAreTaken(
	t *testing.T,
) {

	ctx, storage, scheduler, task := newScheduleBackupImageTasksTest(1000, 2)
	execCtx := tasks_mocks.NewExecutionContextMock()

	expectRunningImageCopies(storage, scheduler, 2, nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
	storage.AssertNotCalled(t, "ListImagesToBackup", mock.Anything, mock.Anything)
}

func TestScheduleBackupImageTasksFreesSlotsOfEndedTasks(t *testing.T) {
	ctx, storage, scheduler, task := newScheduleBackupImageTasksTest(1000, 2)
	execCtx := tasks_mocks.NewExecutionContextMock()

	expectRunningImageCopies(storage, scheduler, 2, []string{"running1"})
	storage.On(
		"RemoveImageFromBackupQueue",
		mock.Anything,
		"running_image1",
		"attempt",
	).Return(nil).Once()
	storage.On("ListImagesToBackup", mock.Anything, 1).Return(
		[]resources.ImageBackupRequest{
			{ImageID: "image1", BackupID: "attempt1"},
		},
		nil,
	)
	expectImageBackupScheduled(
		storage,
		scheduler,
		"image1",
		"attempt1",
		"backup_image_image1_attempt1",
	)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
}
