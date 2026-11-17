package images

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

func TestCreateImageFromDiskPassesBlockSizeToDataplane(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	client := nbs_mocks.NewClientMock()
	for _, m := range []*mock.Mock{&storage.Mock, &scheduler.Mock, &execCtx.Mock, &client.Mock} {
		m.Test(t)
		t.Cleanup(func() { m.AssertExpectations(t) })
	}
	disk := &types.Disk{DiskId: "disk", ZoneId: "zone"}
	execCtx.On("GetTaskID").Return("create-image").Once()
	client.On("Describe", ctx, "disk").Return(nbs_client.DiskParams{
		BlockSize: 8192,
	}, nil).Once()
	storage.On("CreateImage", ctx, mock.Anything).
		Return(resources.ImageMeta{ID: "image"}, nil).Once()
	client.On("CreateCheckpoint", ctx, nbs_client.CheckpointParams{
		DiskID: "disk", CheckpointID: "image",
	}).Return(nil).Once()
	scheduler.On("ScheduleZonalTask", mock.Anything,
		"dataplane.CreateSnapshotFromDisk", "", "zone",
		&dataplane_protos.CreateSnapshotFromDiskRequest{
			SrcDisk: disk, SrcDiskCheckpointId: "image",
			DstSnapshotId: "image", BlockSize: 8192,
		}).Return("copy-task", nil).Once()
	scheduler.On("WaitTask", ctx, execCtx, "copy-task").
		Return(nil, errors.NewInterruptExecutionError()).Once()
	task := &createImageFromDiskTask{
		storage: storage, scheduler: scheduler,
		request: &protos.CreateImageFromDiskRequest{DstImageId: "image"},
		state:   &protos.CreateImageFromDiskTaskState{},
	}

	_, err := task.run(ctx, execCtx, client, disk)
	require.ErrorIs(t, err, errors.NewInterruptExecutionError())
	require.Equal(t, "copy-task", task.state.DataplaneTaskID)
}
