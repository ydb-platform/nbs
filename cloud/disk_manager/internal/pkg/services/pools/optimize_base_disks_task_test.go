package pools

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	pools_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage/mocks"
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

	minPoolAge, err := time.ParseDuration("12h")
	require.NoError(t, err)

	// OptimizeBaseDisksTask should panic because of incorrect config.
	// ConvertToImageSizedBaseDisksThreshold should be lower than convertToDefaultSizedBaseDisksThreshold.

	task := &optimizeBaseDisksTask{
		scheduler:                               scheduler,
		storage:                                 storage,
		convertToImageSizedBaseDisksThreshold:   15,
		convertToDefaultSizedBaseDisksThreshold: 5,
		minPoolAge:                              minPoolAge,
	}

	err = task.Load(nil, nil)
	require.NoError(t, err)

	require.Panics(t, func() { task.Run(ctx, execCtx) }, "This task should panic")
}
