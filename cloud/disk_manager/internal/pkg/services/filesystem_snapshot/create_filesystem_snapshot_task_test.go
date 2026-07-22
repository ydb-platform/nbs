package filesystem_snapshot

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem_snapshot/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func newExecutionContextMock() *tasks_mocks.ExecutionContextMock {
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("toplevel_task_id")
	return execCtx
}

func TestCreateFilesystemSnapshotTask(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := newExecutionContextMock()

	request := &protos.CreateFilesystemSnapshotRequest{
		SrcFilesystem: &types.Filesystem{
			ZoneId:       "zone",
			FilesystemId: "filesystem",
		},
		DstSnapshotId: "snapshot",
		FolderId:      "folder",
	}

	task := &createFilesystemSnapshotTask{
		storage:   storage,
		scheduler: scheduler,
		request:   request,
		state:     &protos.CreateFilesystemSnapshotTaskState{},
	}

	storage.On("GetFilesystemMeta", ctx, "filesystem").Return(&resources.FilesystemMeta{
		ID:     "filesystem",
		ZoneID: "zone",
	}, nil)

	storage.On("CreateFilesystemSnapshot", ctx, mock.MatchedBy(func(meta resources.FilesystemSnapshotMeta) bool {
		return meta.ID == "snapshot" &&
			meta.FolderID == "folder" &&
			meta.Filesystem.ZoneId == "zone" &&
			meta.Filesystem.FilesystemId == "filesystem"
	})).Return(&resources.FilesystemSnapshotMeta{
		ID: "snapshot",
		Filesystem: &types.Filesystem{
			ZoneId:       "zone",
			FilesystemId: "filesystem",
		},
	}, nil)

	scheduler.On(
		"ScheduleZonalTask",
		headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_run"),
		"dataplane.CreateSnapshotFromFilesystem",
		"",
		"zone",
		&dataplane_protos.CreateFilesystemSnapshotRequest{
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "filesystem",
			},
			CheckpointId: "",
			SnapshotId:   "snapshot",
		},
	).Return("dataplane", nil)
	execCtx.On("SaveState", ctx).Return(nil)
	scheduler.On("WaitTask", ctx, execCtx, "dataplane").Return(nil, nil)
	storage.On("FilesystemSnapshotCreated", ctx, "snapshot", mock.Anything, uint64(0), uint64(0)).Return(nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
	require.NoError(t, err)
	require.Equal(t, "dataplane", task.state.DataplaneTaskID)
}
