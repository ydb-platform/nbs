package pools

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	pools_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage/mocks"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestOptimizeBaseDisksTask(t *testing.T) {
	ctx := newContext()
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	execCtx.On("GetTaskID").Return("1")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)

	storage.On("GetReadyPoolInfos", ctx).Return([]pools_storage.PoolInfo{
		{
			ImageID:       "image1",
			ZoneID:        "zone1",
			FreeUnits:     111,
			AcquiredUnits: 20,
			Capacity:      100,
			ImageSize:     0,
			CreatedAt:     yesterday,
		},
		{
			ImageID:       "image2",
			ZoneID:        "zone2",
			FreeUnits:     111,
			AcquiredUnits: 2,
			Capacity:      100,
			ImageSize:     0,
			CreatedAt:     yesterday,
		},
		{
			ImageID:       "image3",
			ZoneID:        "zone3",
			FreeUnits:     111,
			AcquiredUnits: 10,
			Capacity:      100,
			ImageSize:     0,
			CreatedAt:     yesterday,
		},
		{
			ImageID:       "image4",
			ZoneID:        "zone4",
			FreeUnits:     111,
			AcquiredUnits: 20,
			Capacity:      100,
			ImageSize:     111,
			CreatedAt:     yesterday,
		},
		{
			ImageID:       "image5",
			ZoneID:        "zone5",
			FreeUnits:     111,
			AcquiredUnits: 20,
			Capacity:      100,
			ImageSize:     111,
			CreatedAt:     now,
		},
	}, nil)

	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"pools.ConfigurePool",
		"",
		&protos.ConfigurePoolRequest{
			ImageId:      "image2",
			ZoneId:       "zone2",
			Capacity:     100,
			UseImageSize: true,
		},
	).Return("task2", nil)

	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"pools.ConfigurePool",
		"",
		&protos.ConfigurePoolRequest{
			ImageId:      "image4",
			ZoneId:       "zone4",
			Capacity:     100,
			UseImageSize: false,
		},
	).Return("task4", nil)

	scheduler.On("WaitTask", mock.Anything, execCtx, "task2").Return(nil, nil)

	scheduler.On("WaitTask", mock.Anything, execCtx, "task4").Return(nil, nil)

	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"pools.RetireBaseDisks",
		"",
		&protos.RetireBaseDisksRequest{
			ImageId:          "image2",
			ZoneId:           "zone2",
			UseBaseDiskAsSrc: true,
		},
	).Return("task2_1", nil)

	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"pools.RetireBaseDisks",
		"",
		&protos.RetireBaseDisksRequest{
			ImageId:          "image4",
			ZoneId:           "zone4",
			UseBaseDiskAsSrc: true,
		},
	).Return("task4_1", nil)

	scheduler.On("WaitTask", mock.Anything, execCtx, "task2_1").Return(nil, nil)

	scheduler.On("WaitTask", mock.Anything, execCtx, "task4_1").Return(nil, nil)

	minPoolAge, err := time.ParseDuration("12h")
	require.NoError(t, err)

	task := &optimizeBaseDisksTask{
		scheduler:                               scheduler,
		storage:                                 storage,
		convertToImageSizedBaseDisksThreshold:   5,
		convertToDefaultSizedBaseDisksThreshold: 15,
		minPoolAge:                              minPoolAge,
	}

	err = task.Load(nil, nil)
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, scheduler, storage, execCtx)
}

func TestOptimizeBaseDisksTaskShouldPanicOnIncorrectConfig(t *testing.T) {
	ctx := newContext()
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	storage.On("GetReadyPoolInfos", ctx).Return([]pools_storage.PoolInfo{}, nil)
	execCtx.On("SaveState", mock.Anything).Return(nil)

	minPoolAge := time.Hour * 12

	task := &optimizeBaseDisksTask{
		scheduler:                               scheduler,
		storage:                                 storage,
		convertToImageSizedBaseDisksThreshold:   15,
		convertToDefaultSizedBaseDisksThreshold: 5,
		minPoolAge:                              minPoolAge,
	}

	err := task.Load(nil, nil)
	require.NoError(t, err)

	require.Panics(t, func() { _ = task.Run(ctx, execCtx) }, "This task should panic")
}

func TestOptimizeBaseDisksTaskIdleCleanup(t *testing.T) {
	ctx := newContext()
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	execCtx.On("GetTaskID").Return("1")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	yesterday := time.Now().AddDate(0, 0, -1)

	storage.On("GetReadyPoolInfos", ctx).Return([]pools_storage.PoolInfo{
		{
			ImageID:       "image1",
			ZoneID:        "zone1",
			FreeUnits:     640,
			AcquiredUnits: 0,
			Capacity:      1280,
			ImageSize:     0,
			CreatedAt:     yesterday,
		},
	}, nil)

	idleTTL := 24 * time.Hour

	storage.On(
		"GetIdleBaseDisks",
		ctx,
		"image1",
		"zone1",
		idleTTL,
		uint64(100),
	).Return([]pools_storage.BaseDisk{
		{
			ID:        "baseDisk1",
			ImageID:   "image1",
			ZoneID:    "zone1",
			FreeSlots: 80,
		},
	}, nil)

	// ConfigurePool to reduce capacity by min(units, maxActiveSlots) (80, 640) = 80.
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"pools.ConfigurePool",
		"",
		&protos.ConfigurePoolRequest{
			ImageId:      "image1",
			ZoneId:       "zone1",
			Capacity:     1200,
			UseImageSize: false,
		},
	).Return("idle_configure_task1", nil)

	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"idle_configure_task1",
	).Return(nil, nil)

	// RetireBaseDisk for idle disk.
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"pools.RetireBaseDisk",
		"",
		&protos.RetireBaseDiskRequest{
			BaseDiskId: "baseDisk1",
		},
	).Return("idle_retire_task1", nil)

	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"idle_retire_task1",
	).Return(nil, nil)

	task := &optimizeBaseDisksTask{
		scheduler:                 scheduler,
		storage:                   storage,
		baseDiskIdleTTL:           idleTTL,
		cleanupIdleBaseDisksLimit: 100,
	}

	err := task.Load(nil, nil)
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, scheduler, storage, execCtx)
}

func TestOptimizeBaseDisksTaskIdleCleanupSkipsOptimizedPools(t *testing.T) {
	ctx := newContext()
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	execCtx.On("GetTaskID").Return("1")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	yesterday := time.Now().AddDate(0, 0, -1)

	storage.On("GetReadyPoolInfos", ctx).Return([]pools_storage.PoolInfo{
		{
			ImageID:       "image1",
			ZoneID:        "zone1",
			FreeUnits:     640,
			AcquiredUnits: 2,
			Capacity:      1280,
			ImageSize:     0,
			CreatedAt:     yesterday,
		},
	}, nil)

	// Pool is optimized (AcquiredUnits=2 < threshold=5 => switch to image size).
	// GetIdleBaseDisks is NOT called — pool is skipped before the query.
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"pools.ConfigurePool",
		"",
		&protos.ConfigurePoolRequest{
			ImageId:      "image1",
			ZoneId:       "zone1",
			Capacity:     1280,
			UseImageSize: true,
		},
	).Return("optimize_task1", nil)

	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"optimize_task1",
	).Return(nil, nil)

	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"pools.RetireBaseDisks",
		"",
		&protos.RetireBaseDisksRequest{
			ImageId:          "image1",
			ZoneId:           "zone1",
			UseBaseDiskAsSrc: true,
		},
	).Return("optimize_retire_task1", nil)

	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"optimize_retire_task1",
	).Return(nil, nil)

	// No idle cleanup tasks should be scheduled for this pool.

	task := &optimizeBaseDisksTask{
		scheduler:                               scheduler,
		storage:                                 storage,
		convertToImageSizedBaseDisksThreshold:   5,
		convertToDefaultSizedBaseDisksThreshold: 15,
		minPoolAge:                              12 * time.Hour,
		baseDiskIdleTTL:                         24 * time.Hour,
		cleanupIdleBaseDisksLimit:               100,
	}

	err := task.Load(nil, nil)
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, scheduler, storage, execCtx)
}

func TestOptimizeBaseDisksTaskIdleCleanupMultipleDisks(t *testing.T) {
	ctx := newContext()
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	execCtx.On("GetTaskID").Return("1")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	yesterday := time.Now().AddDate(0, 0, -1)

	storage.On("GetReadyPoolInfos", ctx).Return([]pools_storage.PoolInfo{
		{
			ImageID:       "image1",
			ZoneID:        "zone1",
			FreeUnits:     1920,
			AcquiredUnits: 0,
			Capacity:      1920,
			ImageSize:     0,
			CreatedAt:     yesterday,
		},
	}, nil)

	idleTTL := 24 * time.Hour

	storage.On(
		"GetIdleBaseDisks",
		ctx,
		"image1",
		"zone1",
		idleTTL,
		uint64(100),
	).Return([]pools_storage.BaseDisk{
		{
			ID:        "baseDisk1",
			ImageID:   "image1",
			ZoneID:    "zone1",
			FreeSlots: 100,
		},
		{
			ID:        "baseDisk2",
			ImageID:   "image1",
			ZoneID:    "zone1",
			FreeSlots: 120,
		},
	}, nil)

	// Capacity reduced by 100 + 120 = 220.
	// New capacity = 1920 - 220 = 1700.
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"pools.ConfigurePool",
		"",
		&protos.ConfigurePoolRequest{
			ImageId:      "image1",
			ZoneId:       "zone1",
			Capacity:     1700,
			UseImageSize: false,
		},
	).Return("idle_configure_task1", nil)

	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"idle_configure_task1",
	).Return(nil, nil)

	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"pools.RetireBaseDisk",
		"",
		&protos.RetireBaseDiskRequest{
			BaseDiskId: "baseDisk1",
		},
	).Return("idle_retire_task1", nil)

	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"idle_retire_task1",
	).Return(nil, nil)

	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"pools.RetireBaseDisk",
		"",
		&protos.RetireBaseDiskRequest{
			BaseDiskId: "baseDisk2",
		},
	).Return("idle_retire_task2", nil)

	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"idle_retire_task2",
	).Return(nil, nil)

	task := &optimizeBaseDisksTask{
		scheduler:                 scheduler,
		storage:                   storage,
		baseDiskIdleTTL:           idleTTL,
		cleanupIdleBaseDisksLimit: 100,
	}

	err := task.Load(nil, nil)
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, scheduler, storage, execCtx)
}

func TestOptimizeBaseDisksTaskIdleCleanupContinuesOnConfigurePoolFailure(
	t *testing.T,
) {
	ctx := newContext()
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	execCtx.On("GetTaskID").Return("1")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	yesterday := time.Now().AddDate(0, 0, -1)

	storage.On("GetReadyPoolInfos", ctx).Return([]pools_storage.PoolInfo{
		{
			ImageID:       "image1",
			ZoneID:        "zone1",
			FreeUnits:     640,
			AcquiredUnits: 0,
			Capacity:      1280,
			ImageSize:     0,
			CreatedAt:     yesterday,
		},
	}, nil)

	idleTTL := 24 * time.Hour

	storage.On(
		"GetIdleBaseDisks",
		ctx,
		"image1",
		"zone1",
		idleTTL,
		uint64(100),
	).Return([]pools_storage.BaseDisk{
		{
			ID:        "baseDisk1",
			ImageID:   "image1",
			ZoneID:    "zone1",
			FreeSlots: 80,
		},
	}, nil)

	// ConfigurePool fails with non-retriable error (e.g. image deleted).
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"pools.ConfigurePool",
		"",
		&protos.ConfigurePoolRequest{
			ImageId:      "image1",
			ZoneId:       "zone1",
			Capacity:     1200,
			UseImageSize: false,
		},
	).Return("idle_configure_task1", nil)

	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"idle_configure_task1",
	).Return(nil, task_errors.NewNonRetriableErrorf("image deleted"))

	// RetireBaseDisk still runs — it is idempotent and handles deleted disks.
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"pools.RetireBaseDisk",
		"",
		&protos.RetireBaseDiskRequest{
			BaseDiskId: "baseDisk1",
		},
	).Return("idle_retire_task1", nil)

	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"idle_retire_task1",
	).Return(nil, nil)

	task := &optimizeBaseDisksTask{
		scheduler:                 scheduler,
		storage:                   storage,
		baseDiskIdleTTL:           idleTTL,
		cleanupIdleBaseDisksLimit: 100,
	}

	err := task.Load(nil, nil)
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, scheduler, storage, execCtx)
}

func TestOptimizeBaseDisksTaskNoCleanupWhenTTLZero(t *testing.T) {
	ctx := newContext()
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	execCtx.On("SaveState", mock.Anything).Return(nil)

	yesterday := time.Now().AddDate(0, 0, -1)

	storage.On("GetReadyPoolInfos", ctx).Return([]pools_storage.PoolInfo{
		{
			ImageID:       "image1",
			ZoneID:        "zone1",
			FreeUnits:     640,
			AcquiredUnits: 20,
			Capacity:      640,
			ImageSize:     0,
			CreatedAt:     yesterday,
		},
	}, nil)

	// GetIdleBaseDisks should NOT be called when baseDiskIdleTTL is 0.

	task := &optimizeBaseDisksTask{
		scheduler:                               scheduler,
		storage:                                 storage,
		convertToImageSizedBaseDisksThreshold:   5,
		convertToDefaultSizedBaseDisksThreshold: 15,
		minPoolAge:                              12 * time.Hour,
		baseDiskIdleTTL:                         0,
	}

	err := task.Load(nil, nil)
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, scheduler, storage, execCtx)
}
