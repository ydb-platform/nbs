package images

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	api "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

func TestExportImageStartsCopyOnlyWhenReady(t *testing.T) {
	for _, ready := range []bool{false, true} {
		name := "CREATING"
		if ready {
			name = "READY"
		}

		t.Run(name, func(t *testing.T) {
			ctx := headers.Append(context.Background(), map[string]string{
				"x-task-id":      "compute-export-task",
				"x-operation-id": "compute-operation",
			})
			storage := resources_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			storage.On("GetImageMeta", mock.Anything, "image").Return(
				&resources.ImageMeta{
					ID:       "image",
					FolderID: "folder",
					Ready:    ready,
				},
				nil,
			).Once()

			if ready {
				execCtx.On("GetTaskID").Return("export-task").Once()
				execCtx.On("SaveState", mock.Anything).Return(nil).Once()
				scheduler.On(
					"ScheduleTask", mock.Anything,
					"dataplane.ExportImageToS3", "",
					mock.MatchedBy(func(
						req *dataplane_protos.ExportSnapshotRequest,
					) bool {

						return req.SnapshotId == "image" &&
							req.ImageId == "image" &&
							req.FolderId == "folder" &&
							req.ComputeTaskId == "compute-export-task"
					}),
				).Return("copy-task", nil).Once()
				scheduler.On(
					"WaitTask", mock.Anything, execCtx, "copy-task",
				).Return(nil, errors.NewInterruptExecutionError()).Once()
			}

			task := &exportImageTask{
				scheduler: scheduler,
				storage:   storage,
				config:    &server_config.ExportConfig{},
				request: &api.ExportImageRequest{
					ImageId:  "image",
					FolderId: "folder",
				},
				state: &protos.ExportImageTaskState{},
			}
			err := task.Run(ctx, execCtx)
			if ready {
				require.ErrorIs(t, err, errors.NewInterruptExecutionError())
				require.Equal(t, "copy-task", task.state.DataplaneTaskId)
			} else {
				require.ErrorContains(t, err, "is absent or not ready")
				require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())
				require.Empty(t, task.state.DataplaneTaskId)
				scheduler.AssertNotCalled(
					t, "ScheduleTask", mock.Anything, mock.Anything,
					mock.Anything, mock.Anything,
				)
				execCtx.AssertNotCalled(t, "SaveState", mock.Anything)
			}

			mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
		})
	}
}

func TestExportImageRejectsForbiddenOrWrongFolder(t *testing.T) {
	for _, forbidden := range []bool{false, true} {
		ctx := headers.SetIncomingTaskID(
			context.Background(), "compute-task",
		)
		storage := resources_mocks.NewStorageMock()
		storage.On("GetImageMeta", mock.Anything, "image").Return(
			&resources.ImageMeta{Ready: true, FolderID: "actual-folder"},
			nil,
		).Once()
		scheduler := tasks_mocks.NewSchedulerMock()
		execCtx := tasks_mocks.NewExecutionContextMock()
		task := &exportImageTask{
			storage: storage, scheduler: scheduler,
			config: &server_config.ExportConfig{},
			request: &api.ExportImageRequest{
				ImageId: "image", FolderId: "wrong-folder",
			},
			state: &protos.ExportImageTaskState{},
		}
		if forbidden {
			task.request.FolderId = "actual-folder"
			task.config.ForbiddenFolderIds = []string{"actual-folder"}
		}

		err := task.Run(ctx, execCtx)
		require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())
		if forbidden {
			require.ErrorContains(t, err, "is forbidden")
		} else {
			require.ErrorContains(t, err, "folder does not match")
		}

		require.Empty(t, task.state.DataplaneTaskId)
		scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything,
			mock.Anything, mock.Anything, mock.Anything)
		execCtx.AssertNotCalled(t, "SaveState", mock.Anything)
		storage.AssertExpectations(t)
	}
}

func TestExportImageResumesExistingCopy(t *testing.T) {
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	response := &api.ExportResponse{
		Bucket: "bucket", ManifestKey: "prefix/manifest.json",
	}
	scheduler.On(
		"WaitTask", mock.Anything, execCtx, "existing-copy",
	).Return(response, nil).Once()
	execCtx.On("SaveState", mock.Anything).Return(nil).Once()
	task := &exportImageTask{
		scheduler: scheduler,
		state: &protos.ExportImageTaskState{
			DataplaneTaskId: "existing-copy",
		},
	}
	require.NoError(t, task.Run(context.Background(), execCtx))
	require.Equal(t, response, task.GetResponse())
	scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything,
		mock.Anything, mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, scheduler, execCtx)
}

func TestExportImageRequiresComputeTaskID(t *testing.T) {
	ctx := headers.SetIncomingOperationID(
		context.Background(), "compute-operation",
	)
	task := &exportImageTask{
		state: &protos.ExportImageTaskState{},
	}
	err := task.Run(ctx, nil)
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())
	require.ErrorContains(t, err, "missing from x-task-id")
}
