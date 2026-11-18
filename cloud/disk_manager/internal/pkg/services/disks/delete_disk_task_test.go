package disks

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	pools_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/mocks"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
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

func testDeleteDiskTaskWithNonExistentDisk(
	t *testing.T,
	deleteIfNonExistent bool,
) {

	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	poolService := pools_mocks.NewServiceMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	disk := &types.Disk{DiskId: "disk", ZoneId: "zone"}
	request := &protos.DeleteDiskRequest{
		Disk:                disk,
		DeleteIfNonExistent: deleteIfNonExistent,
	}

	task := &deleteDiskTask{
		storage:     storage,
		scheduler:   scheduler,
		poolService: poolService,
		nbsFactory:  nbsFactory,
		request:     request,
		state:       &protos.DeleteDiskTaskState{},
	}

	// There is no such disk in the database, return nil.
	storage.On(
		"DeleteDisk",
		ctx,
		"disk",
		"toplevel_task_id",
		mock.Anything,
	).Return((*resources.DiskMeta)(nil), nil)

	if deleteIfNonExistent {
		cleanupID := diskCleanupPrefix + "toplevel_task_id"
		storage.On("PrepareDiskCleanup", ctx, "disk", cleanupID).Return(true, nil).Once()
		tx := &persistence.Transaction{}
		storage.On("CompleteDiskCleanup", ctx, tx, "disk", cleanupID).Return(nil).Once()
		execCtx.On("FinishWithPreparation", ctx, mock.Anything).Run(func(args mock.Arguments) {
			prepare := args.Get(1).(func(context.Context, *persistence.Transaction) error)
			require.NoError(t, prepare(ctx, tx))
		}).Return(nil).Once()

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
	}

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, scheduler, poolService, nbsFactory, execCtx)
	require.NoError(t, err)
}

func TestDeleteDiskTaskNonExistentDisk(t *testing.T) {
	testDeleteDiskTaskWithNonExistentDisk(t, false /* deleteIfNonExistent */)
}

func TestDeleteDiskTaskNonExistentDiskWithDeletionFromBlockstore(t *testing.T) {
	testDeleteDiskTaskWithNonExistentDisk(t, true /* deleteIfNonExistent */)
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

func TestDeleteDiskTaskTrackedDeletion(t *testing.T) {
	for _, tc := range []struct {
		name     string
		prepared bool
		sync     bool
		errorAt  string
	}{
		{name: "async", prepared: true}, {name: "sync", prepared: true, sync: true},
		{name: "legacy placeholder", prepared: false},
		{name: "reservation read error", errorAt: "PrepareDiskCleanup"},
		{name: "incremental retry keeps reservation", prepared: true, errorAt: "WaitTask"},
		{name: "NBS retry keeps reservation", prepared: true, errorAt: "Delete"},
		{name: "completion callback failure", prepared: true, errorAt: "CompleteDiskCleanup"},
		{name: "task commit failure", prepared: true, errorAt: "FinishWithPreparation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			storage := storage_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			factory := nbs_mocks.NewFactoryMock()
			client := nbs_mocks.NewClientMock()
			execCtx := newExecutionContextMock()
			task := &deleteDiskTask{storage: storage, scheduler: scheduler, nbsFactory: factory}
			request, err := proto.Marshal(&protos.DeleteDiskRequest{Disk: &types.Disk{DiskId: "disk", ZoneId: "cell"}, DeleteIfNonExistent: true, Sync: tc.sync})
			require.NoError(t, err)
			require.NoError(t, task.Load(request, nil))
			id := diskCleanupPrefix + "toplevel_task_id"
			errorFor := func(step string) error {
				if tc.errorAt == step {
					return assert.AnError
				}
				return nil
			}
			if tc.sync {
				storage.On("GetDiskMeta", ctx, "disk").Return((*resources.DiskMeta)(nil), nil).Once()
			}
			storage.On("DeleteDisk", ctx, "disk", "toplevel_task_id", mock.Anything).Return((*resources.DiskMeta)(nil), nil).Once()
			storage.On("PrepareDiskCleanup", ctx, "disk", id).Return(tc.prepared, errorFor("PrepareDiskCleanup")).Once()
			setup := func() {
				if tc.errorAt == "PrepareDiskCleanup" {
					return
				}
				scheduler.On("ScheduleTask", headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_delete_disk_from_incremental"), "dataplane.DeleteDiskFromIncremental", "", &dataplane_protos.DeleteDiskFromIncrementalRequest{Disk: &types.Disk{DiskId: "disk", ZoneId: "cell"}}).Return("incremental", nil).Once()
				scheduler.On("WaitTask", ctx, execCtx, "incremental").Return(nil, errorFor("WaitTask")).Once()
				if tc.errorAt == "WaitTask" {
					return
				}
				factory.On("GetClient", ctx, "cell").Return(client, nil).Once()
				method := "Delete"
				if tc.sync {
					method = "DeleteSync"
				}
				client.On(method, ctx, "disk").Return(errorFor("Delete")).Once()
				if tc.errorAt == "Delete" {
					return
				}
				if !tc.prepared {
					storage.On("DiskDeleted", ctx, "disk", mock.Anything).Return(nil).Once()
					return
				}
				tx := &persistence.Transaction{}
				storage.On("CompleteDiskCleanup", ctx, tx, "disk", id).Return(errorFor("CompleteDiskCleanup")).Once()
				finishErr := errorFor("CompleteDiskCleanup")
				if finishErr == nil {
					finishErr = errorFor("FinishWithPreparation")
				}
				execCtx.On("FinishWithPreparation", ctx, mock.Anything).Run(func(args mock.Arguments) {
					prep := args.Get(1).(func(context.Context, *persistence.Transaction) error)
					require.Equal(t, errorFor("CompleteDiskCleanup"), prep(ctx, tx))
				}).Return(finishErr).Once()
			}
			setup()
			err = task.Run(ctx, execCtx)
			if tc.errorAt != "" {
				require.True(t, errors.Is(err, assert.AnError))
			} else {
				require.NoError(t, err)
			}
			mock.AssertExpectationsForObjects(t, storage, scheduler, factory, client, execCtx)
			if tc.prepared {
				storage.AssertNotCalled(t, "DiskDeleted", mock.Anything, mock.Anything, mock.Anything)
			}
			scheduler.AssertNotCalled(t, "ScheduleNonCancellableTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		})
	}
}
