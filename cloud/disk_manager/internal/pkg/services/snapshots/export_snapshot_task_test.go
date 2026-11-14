package snapshots

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	api "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	service_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

func TestExportSnapshotStartsCopyOnlyWhenReady(t *testing.T) {
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
			storage.On("GetSnapshotMeta", mock.Anything, "snapshot").Return(
				&resources.SnapshotMeta{
					ID:       "snapshot",
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
					"dataplane.ExportSnapshotToS3", "",
					mock.MatchedBy(func(
						req *dataplane_protos.ExportSnapshotRequest,
					) bool {

						return req.SnapshotId == "snapshot" &&
							req.FolderId == "folder" &&
							req.ComputeTaskId == "compute-export-task"
					}),
				).Return("copy-task", nil).Once()
				scheduler.On(
					"WaitTask", mock.Anything, execCtx, "copy-task",
				).Return(nil, errors.NewInterruptExecutionError()).Once()
			}

			task := &exportSnapshotTask{
				scheduler: scheduler,
				storage:   storage,
				config:    &server_config.ExportConfig{},
				request: &api.ExportSnapshotRequest{
					SnapshotId: "snapshot",
					FolderId:   "folder",
				},
				state: &protos.ExportSnapshotTaskState{},
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

func TestExportSnapshotRequiresComputeTaskID(t *testing.T) {
	ctx := headers.SetIncomingOperationID(
		context.Background(), "compute-operation",
	)
	task := &exportSnapshotTask{
		state: &protos.ExportSnapshotTaskState{},
	}
	err := task.Run(ctx, nil)
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())
	require.ErrorContains(t, err, "missing from x-task-id")
}

func TestExportSnapshotDisabledReturnsUnimplemented(t *testing.T) {
	s := &service{}
	taskID, err := s.ExportSnapshot(context.Background(), nil)
	require.Empty(t, taskID)
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.True(t, errors.IsSilent(err))
	require.False(t, errors.CanRetry(err))
}

func TestSnapshotExportRegistrationRequiresConfig(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		registry := tasks.NewRegistry()
		scheduler := tasks_mocks.NewSchedulerMock()
		scheduler.On("ScheduleRegularTasks", mock.Anything, mock.Anything, mock.Anything).Return()
		var exportConfig *server_config.ExportConfig
		if enabled {
			exportConfig = &server_config.ExportConfig{}
		}
		err := RegisterForExecution(context.Background(), &service_config.SnapshotsConfig{},
			nil, registry, scheduler, nil, nil, nil, nil, nil, exportConfig)
		require.NoError(t, err)
		require.Contains(t, registry.TaskTypesForExecution(), "snapshots.ClearDeletedSnapshots")
		if enabled {
			require.Contains(t, registry.TaskTypesForExecution(), "snapshots.ExportSnapshot")
			_, err := registry.NewTask("snapshots.ExportSnapshot")
			require.NoError(t, err)
		} else {
			require.NotContains(t, registry.TaskTypes(), "snapshots.ExportSnapshot")
			_, err := registry.NewTask("snapshots.ExportSnapshot")
			require.Error(t, err)
		}
	}
}
