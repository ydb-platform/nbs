package disks

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
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

	diskMeta := &resources.DiskMeta{
		ZoneID:       "zone",
		Kind:         "ssd",
		DeleteTaskID: "toplevel_task_id",
	}
	if sync {
		storage.On(
			"GetDiskMeta",
			ctx,
			"disk",
		).Return(diskMeta, nil).Once()
	}
	storage.On(
		"DeleteDisk",
		ctx,
		"disk",
		"toplevel_task_id",
		mock.Anything,
	).Return(diskMeta, nil).Once()
	storage.On("DiskDeleted", ctx, "disk", mock.Anything).Return(nil)

	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
	if sync {
		nbsClient.On("Describe", ctx, "disk").Return(nbs.DiskParams{}, nil)
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
		Kind:         "ssd",
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
		Kind:         "ssd",
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
	).Return((*resources.DiskMeta)(nil), nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, scheduler, poolService, nbsFactory, execCtx)
	require.NoError(t, err)
}

func testDeleteDiskTaskEstimatedInflightDurationForLocalDisks(
	t *testing.T,
	sync bool,
	diskKind types.DiskKind,
	expectedEstimatedInflightDuration time.Duration,
) {
	ctx := context.Background()

	SSDLocalDiskDeletingBandwidthMiBs := uint64(100)
	HDDLocalDiskDeletingBandwidthMiBs := uint64(10)
	performanceConfig := &performance_config.PerformanceConfig{
		SSDLocalDiskDeletingBandwidthMiBs: &SSDLocalDiskDeletingBandwidthMiBs,
		HDDLocalDiskDeletingBandwidthMiBs: &HDDLocalDiskDeletingBandwidthMiBs,
	}
	storage := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	poolService := pools_mocks.NewServiceMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	disk := &types.Disk{DiskId: "disk"}
	request := &protos.DeleteDiskRequest{Disk: disk, Sync: sync}

	task := &deleteDiskTask{
		performanceConfig: performanceConfig,
		storage:           storage,
		scheduler:         scheduler,
		poolService:       poolService,
		nbsFactory:        nbsFactory,
		request:           request,
		state:             &protos.DeleteDiskTaskState{},
	}

	diskMeta := &resources.DiskMeta{
		ZoneID:       "zone",
		DeleteTaskID: "toplevel_task_id",
	}

	if sync {
		storage.On(
			"GetDiskMeta",
			ctx,
			"disk",
		).Return(diskMeta, nil).Once()
		nbsClient.On("Describe", ctx, "disk").Return(nbs.DiskParams{
			Kind:        diskKind,
			BlocksCount: 100 * 256, // 100 MiB
			BlockSize:   4096,
		}, nil)
	}

	storage.On(
		"DeleteDisk",
		ctx,
		"disk",
		"toplevel_task_id",
		mock.Anything,
	).Return(diskMeta, nil).Once()
	storage.On("DiskDeleted", ctx, "disk", mock.Anything).Return(nil)

	if expectedEstimatedInflightDuration > 0 {
		execCtx.On("SetEstimatedInflightDuration", expectedEstimatedInflightDuration)
	}

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

func TestDeleteDiskTaskEstimatedInflightDurationForLocalDisks(t *testing.T) {
	testCases := []struct {
		name                              string
		sync                              bool
		diskKind                          types.DiskKind
		expectedEstimatedInflightDuration time.Duration
	}{
		{
			name:                              "No estimate for any disks except local; Sync=false",
			sync:                              false,
			diskKind:                          types.DiskKind_DISK_KIND_SSD,
			expectedEstimatedInflightDuration: 0,
		},
		{
			name:                              "No estimate for any disks except local; Sync=true",
			sync:                              true,
			diskKind:                          types.DiskKind_DISK_KIND_SSD,
			expectedEstimatedInflightDuration: 0,
		},
		{
			name:                              "No estimate for Sync=false; DiskKind=ssd-local",
			sync:                              false,
			diskKind:                          types.DiskKind_DISK_KIND_SSD_LOCAL,
			expectedEstimatedInflightDuration: 0,
		},
		{
			name:                              "Estimate set for Sync=true; DiskKind=ssd-local",
			sync:                              true,
			diskKind:                          types.DiskKind_DISK_KIND_SSD_LOCAL,
			expectedEstimatedInflightDuration: 1 * time.Second,
		},
		{
			name:                              "No estimate for Sync=false; DiskKind=hdd-local",
			sync:                              false,
			diskKind:                          types.DiskKind_DISK_KIND_HDD_LOCAL,
			expectedEstimatedInflightDuration: 0,
		},
		{
			name:                              "Estimate set for Sync=true; DiskKind=hdd-local",
			sync:                              true,
			diskKind:                          types.DiskKind_DISK_KIND_HDD_LOCAL,
			expectedEstimatedInflightDuration: 10 * time.Second,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testDeleteDiskTaskEstimatedInflightDurationForLocalDisks(
				t,
				testCase.sync,
				testCase.diskKind,
				testCase.expectedEstimatedInflightDuration,
			)
		})
	}
}
