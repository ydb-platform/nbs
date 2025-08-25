package disks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	pools_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/mocks"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func testDeleteDiskTaskRun(t *testing.T, sync bool) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	poolService := pools_mocks.NewServiceMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	disk := &types.Disk{DiskId: "disk"}
	request := &protos.DeleteDiskRequest{Disk: disk, Sync: sync}

	task := &deleteDiskTask{
		storage:     storage,
		scheduler:   scheduler,
		poolService: poolService,
		nbsFactory:  nbsFactory,
		request:     request,
		state:       &protos.DeleteDiskTaskState{},
	}

	storage.On(
		"DeleteDisk",
		ctx,
		"disk",
		"toplevel_task_id",
		mock.Anything,
	).Return(&resources.DiskMeta{
		ZoneID:       "zone",
		DeleteTaskID: "toplevel_task_id",
	}, nil)
	storage.On("DiskDeleted", ctx, "disk", mock.Anything).Return(nil)

	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
	if sync {
		nbsClient.On("DeleteSync", ctx, "disk").Return(nil)
	} else {
		nbsClient.On("Delete", ctx, "disk").Return(nil)
	}

	scheduler.On(
		"ScheduleTask",
		headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_delete_disk_from_incremental"),
		"dataplane.DeleteDiskFromIncremental",
		"",
		mock.Anything,
	).Return("deleteTask", nil)

	scheduler.On("WaitTask", ctx, execCtx, "deleteTask").Return(nil, nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, scheduler, nbsFactory, nbsClient, execCtx)
	require.NoError(t, err)
}

func TestDeleteDiskTaskRun(t *testing.T) {
	testDeleteDiskTaskRun(t, false)
}

func TestDeleteDiskSyncTaskRun(t *testing.T) {
	testDeleteDiskTaskRun(t, true)
}

func TestDeleteDiskTaskRunWithDiskCreatedFromImage(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	poolService := pools_mocks.NewServiceMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	disk := &types.Disk{ZoneId: "zone", DiskId: "disk"}
	request := &protos.DeleteDiskRequest{Disk: disk}

	task := &deleteDiskTask{
		storage:     storage,
		scheduler:   scheduler,
		poolService: poolService,
		nbsFactory:  nbsFactory,
		request:     request,
		state:       &protos.DeleteDiskTaskState{},
	}

	storage.On(
		"DeleteDisk",
		ctx,
		"disk",
		"toplevel_task_id",
		mock.Anything,
	).Return(&resources.DiskMeta{
		ZoneID:       "zone",
		SrcImageID:   "image",
		DeleteTaskID: "toplevel_task_id",
	}, nil)
	storage.On("DiskDeleted", ctx, "disk", mock.Anything).Return(nil)

	scheduler.On(
		"ScheduleTask",
		headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_delete_disk_from_incremental"),
		"dataplane.DeleteDiskFromIncremental",
		"",
		mock.Anything,
	).Return("deleteTask", nil)

	scheduler.On("WaitTask", ctx, execCtx, "deleteTask").Return(nil, nil)

	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
	nbsClient.On("Delete", ctx, "disk").Return(nil)

	poolService.On(
		"ReleaseBaseDisk",
		headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_release_base_disk"),
		&pools_protos.ReleaseBaseDiskRequest{
			OverlayDisk: disk,
		}).Return("release", nil)
	scheduler.On("WaitTask", ctx, execCtx, "release").Return(nil, nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, scheduler, poolService, nbsFactory, nbsClient, execCtx)
	require.NoError(t, err)
}

func TestDeleteDiskTaskCancel(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	poolService := pools_mocks.NewServiceMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	disk := &types.Disk{ZoneId: "zone", DiskId: "disk"}
	request := &protos.DeleteDiskRequest{Disk: disk}

	task := &deleteDiskTask{
		storage:     storage,
		scheduler:   scheduler,
		poolService: poolService,
		nbsFactory:  nbsFactory,
		request:     request,
		state:       &protos.DeleteDiskTaskState{},
	}

	storage.On(
		"DeleteDisk",
		ctx,
		"disk",
		"toplevel_task_id",
		mock.Anything,
	).Return(&resources.DiskMeta{
		ZoneID:       "zone",
		SrcImageID:   "image",
		DeleteTaskID: "toplevel_task_id",
	}, nil)
	storage.On("DiskDeleted", ctx, "disk", mock.Anything).Return(nil)

	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
	nbsClient.On("Delete", ctx, "disk").Return(nil)

	scheduler.On(
		"ScheduleTask",
		headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_delete_disk_from_incremental"),
		"dataplane.DeleteDiskFromIncremental",
		"",
		mock.Anything,
	).Return("deleteTask", nil)

	scheduler.On("WaitTask", ctx, execCtx, "deleteTask").Return(nil, nil)

	poolService.On(
		"ReleaseBaseDisk",
		headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_release_base_disk"),
		&pools_protos.ReleaseBaseDiskRequest{
			OverlayDisk: disk,
		}).Return("release", nil)
	scheduler.On("WaitTask", ctx, execCtx, "release").Return(nil, nil)

	err := task.Cancel(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, scheduler, poolService, nbsFactory, nbsClient, execCtx)
	require.NoError(t, err)
}

func TestDeleteDiskTaskWithNonExistentDisk(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	poolService := pools_mocks.NewServiceMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	execCtx := newExecutionContextMock()

	disk := &types.Disk{DiskId: "disk"}
	request := &protos.DeleteDiskRequest{Disk: disk}

	task := &deleteDiskTask{
		storage:     storage,
		scheduler:   scheduler,
		poolService: poolService,
		nbsFactory:  nbsFactory,
		request:     request,
		state:       &protos.DeleteDiskTaskState{},
	}

	storage.On(
		"DeleteDisk",
		ctx,
		"disk",
		"toplevel_task_id",
		mock.Anything,
	).Return(&resources.DiskMeta{
		ZoneID:       "",
		SrcImageID:   "image",
		DeleteTaskID: "toplevel_task_id",
	}, nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, scheduler, poolService, nbsFactory, execCtx)
	require.NoError(t, err)
}
