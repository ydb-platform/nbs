package pools

import (
	"context"
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	pools_storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestAcquireBaseDiskTask(t *testing.T) {
	ctx := newContext()
	scheduler := tasks_mocks.NewSchedulerMock()
	s := pools_storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	execCtx.On("SaveState", ctx).Return(nil)

	overlayDisk := &types.Disk{ZoneId: "zone", DiskId: "disk"}
	request := &protos.AcquireBaseDiskRequest{
		SrcImageId:  "image",
		OverlayDisk: overlayDisk,
	}

	task := &acquireBaseDiskTask{
		scheduler: scheduler,
		storage:   s,
		request:   request,
		state:     &protos.AcquireBaseDiskTaskState{},
	}

	s.On(
		"AcquireBaseDiskSlot",
		ctx,
		"image",
		storage.Slot{OverlayDisk: overlayDisk},
	).Return(storage.BaseDisk{
		ID:           "baseDisk",
		ImageID:      "image",
		ZoneID:       "zone",
		CheckpointID: "checkpoint",
		CreateTaskID: "acquire",
		Ready:        true,
	}, nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, s, execCtx)
}

func TestAcquireBaseDiskTaskShouldWaitForBaseDiskCreated(t *testing.T) {
	ctx := newContext()
	scheduler := tasks_mocks.NewSchedulerMock()
	s := pools_storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	execCtx.On("SaveState", ctx).Return(nil)

	overlayDisk := &types.Disk{ZoneId: "zone", DiskId: "disk"}
	request := &protos.AcquireBaseDiskRequest{
		SrcImageId:  "image",
		OverlayDisk: overlayDisk,
	}

	task := &acquireBaseDiskTask{
		scheduler: scheduler,
		storage:   s,
		request:   request,
		state:     &protos.AcquireBaseDiskTaskState{},
	}

	s.On(
		"AcquireBaseDiskSlot",
		ctx,
		"image",
		storage.Slot{OverlayDisk: overlayDisk},
	).Return(storage.BaseDisk{
		ID:           "baseDisk",
		ImageID:      "image",
		ZoneID:       "zone",
		CheckpointID: "checkpoint",
		CreateTaskID: "otherAcquire",
		Ready:        false,
	}, nil)
	scheduler.On("WaitTask", ctx, execCtx, "otherAcquire").Return(&empty.Empty{}, nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, s, execCtx)
}

func TestCancelAcquireBaseDiskTaskShouldReleaseBaseDisk(t *testing.T) {
	ctx := newContext()
	scheduler := tasks_mocks.NewSchedulerMock()
	s := pools_storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	disk := &types.Disk{ZoneId: "zone", DiskId: "disk"}
	request := &protos.AcquireBaseDiskRequest{
		SrcImageId:  "image",
		OverlayDisk: disk,
	}

	task := &acquireBaseDiskTask{
		scheduler: scheduler,
		storage:   s,
		request:   request,
		state:     &protos.AcquireBaseDiskTaskState{},
	}

	baseDisk := storage.BaseDisk{
		ID:           "baseDisk",
		ImageID:      "image",
		ZoneID:       "zone",
		CheckpointID: "checkpoint",
		CreateTaskID: "acquire",
		Ready:        false,
	}
	s.On("ReleaseBaseDiskSlot", ctx, disk).Return(baseDisk, nil)

	err := task.Cancel(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, s, execCtx)
}
