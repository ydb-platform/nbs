package images

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	pools_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/mocks"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestShouldScheduleRetireBaseDisksTaskOnImageDeleting(t *testing.T) {
	ctx := context.Background()

	scheduler := tasks_mocks.NewSchedulerMock()
	resourceStorage := storage_mocks.NewStorageMock()
	poolService := pools_mocks.NewServiceMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	request := &protos.DeleteImageRequest{
		ImageId:           "ImageID",
		OperationCloudId:  "OperationCloudID",
		OperationFolderId: "OperationFolderID",
	}

	zoneIDs := []string{"zone-a", "zone-b"}
	task := &deleteImageTask{
		config: &config.ImagesConfig{
			DefaultDiskPoolConfigs: []*config.DiskPoolConfig{
				&config.DiskPoolConfig{
					ZoneId: &zoneIDs[0],
				},
				&config.DiskPoolConfig{
					ZoneId: &zoneIDs[1],
				},
			},
		},
		scheduler:   scheduler,
		storage:     resourceStorage,
		poolService: poolService,
		request:     request,
		state:       &protos.DeleteImageTaskState{},
	}

	for _, zoneID := range zoneIDs {
		execCtx.On("GetTaskID").Return("taskID")

		scheduler.On(
			"ScheduleTask",
			headers.SetIncomingIdempotencyKey(ctx, "taskID_"+zoneID),
			"pools.RetireBaseDisks",
			"",
			&pools_protos.RetireBaseDisksRequest{
				ImageId:          "ImageID",
				ZoneId:           zoneID,
				UseBaseDiskAsSrc: true,
			},
			"OperationCloudID",
			"OperationFolderID",
		).Return("retireBaseDisksTaskID", nil)
	}

	err := task.scheduleRetireBaseDisksTasks(ctx, execCtx)
	require.NoError(t, err)

	mock.AssertExpectationsForObjects(t, execCtx, scheduler)
}
