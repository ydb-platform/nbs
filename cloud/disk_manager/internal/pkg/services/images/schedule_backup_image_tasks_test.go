package images

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestScheduleBackupImageTasks(t *testing.T) {
	ctx := logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	storage.On("ListImagesToBackup", mock.Anything, 2).Return(
		[]string{"image1", "image2"},
		nil,
	)

	for _, imageID := range []string{"image1", "image2"} {
		id := imageID
		scheduler.On(
			"ScheduleTask",
			mock.MatchedBy(func(ctx context.Context) bool {
				return headers.GetIdempotencyKey(ctx) == "backup_image_"+id
			}),
			"images.BackupImage",
			"",
			mock.MatchedBy(func(request *protos.BackupImageRequest) bool {
				return request.ImageId == id
			}),
		).Return(id+"_task", nil)
	}

	task := &scheduleBackupImageTasks{
		scheduler: scheduler,
		storage:   storage,
		limit:     2,
	}

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
}
