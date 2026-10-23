package images

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	pools_service_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/mocks"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestDeleteImageSchedulesRetireBaseDisksIdempotently(t *testing.T) {
	testCases := []struct {
		name            string
		folderID        string
		expectedZoneIDs []string
	}{
		{
			name:            "default fallback",
			folderID:        "other-folder",
			expectedZoneIDs: []string{"zone-a"},
		},
		{
			name:            "folder override",
			folderID:        "folder",
			expectedZoneIDs: []string{"zone-b", "zone-c"},
		},
		{
			name:     "empty override",
			folderID: "empty-folder",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := logging.SetLogger(
				context.Background(),
				logging.NewStderrLogger(logging.DebugLevel),
			)
			scheduler := tasks_mocks.NewSchedulerMock()
			storage := resources_storage_mocks.NewStorageMock()
			poolService := pools_service_mocks.NewServiceMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			imageConfig := &config.ImagesConfig{
				DefaultDiskPoolConfigs: []*config.DiskPoolConfig{
					{ZoneId: proto.String("zone-a")},
				},
				DiskPoolConfigsByFolder: map[string]*config.DiskPoolConfigs{
					"folder": {
						Configs: []*config.DiskPoolConfig{
							{ZoneId: proto.String("zone-b")},
							{ZoneId: proto.String("zone-c")},
						},
					},
					"empty-folder": {},
				},
			}

			request := &protos.DeleteImageRequest{
				ImageId: "image",
			}

			task := &deleteImageTask{
				config:      imageConfig,
				scheduler:   scheduler,
				storage:     storage,
				poolService: poolService,
				request:     request,
				state:       &protos.DeleteImageTaskState{},
			}

			imageSize := uint64(100)

			execCtx.On("GetTaskID").Return("delete_image_task")
			poolService.On("ImageDeleting", mock.Anything, mock.Anything).Return(mock.Anything, nil)
			scheduler.On("WaitTask", mock.Anything, mock.Anything).Return(mock.Anything, nil)
			scheduler.On("WaitTaskEnded", mock.Anything, mock.Anything).Maybe().Return(nil)
			scheduler.On(
				"ScheduleTask",
				mock.Anything,
				"dataplane.DeleteSnapshot",
				mock.Anything,
				mock.Anything,
			).Return(mock.Anything, nil)
			storage.On("ImageDeleted", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			storage.On(
				"DeleteImage",
				ctx,
				"image",
				"delete_image_task", // taskID
				mock.Anything,       // deletingAt
			).Return(&resources.ImageMeta{
				ID:           "image",
				FolderID:     testCase.folderID,
				Size:         imageSize,
				DeleteTaskID: "delete_image_task",
			}, nil)

			// Should schedule retire base disks tasks only during the first run.
			for _, zoneID := range testCase.expectedZoneIDs {
				scheduler.On(
					"ScheduleTask",
					mock.Anything, // ctx
					"pools.RetireBaseDisks",
					mock.Anything, // description
					&pools_protos.RetireBaseDisksRequest{
						ImageId:          "image",
						ZoneId:           zoneID,
						UseBaseDiskAsSrc: true,
						UseImageSize:     imageSize,
					},
				).Return(mock.Anything, nil).Once()
			}

			err := task.Run(ctx, execCtx)
			require.NoError(t, err)
			mock.AssertExpectationsForObjects(t, scheduler, storage, poolService, execCtx)

			var m *resources.ImageMeta
			storage.On(
				"DeleteImage",
				ctx,
				"image",
				"delete_image_task", // taskID
				mock.Anything,       // deletingAt
			).Unset().On(
				"DeleteImage",
				ctx,
				"image",
				"delete_image_task", // taskID
				mock.Anything,       // deletingAt
			).Return(m, nil)

			err = task.Run(ctx, execCtx)
			require.NoError(t, err)
			mock.AssertExpectationsForObjects(t, scheduler, storage, poolService, execCtx)
		})
	}
}
