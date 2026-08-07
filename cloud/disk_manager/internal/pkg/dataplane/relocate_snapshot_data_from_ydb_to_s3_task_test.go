package dataplane

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestRelocateSnapshotDataFromYDBToS3TaskLockProgressUnlock(t *testing.T) {
	ctx := newContext()
	storageMock := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task := &relocateSnapshotDataFromYDBToS3Task{
		storage: storageMock,
		request: &protos.RelocateSnapshotDataFromYDBToS3Request{
			SnapshotId: "snapshot",
		},
		state: &protos.RelocateSnapshotDataFromYDBToS3TaskState{},
	}

	execCtx.On("GetTaskID").Return("task")
	storageMock.On("CheckSnapshotReady", ctx, "snapshot").Return(
		storage.SnapshotMeta{ID: "snapshot", ChunkCount: 10, Ready: true},
		nil,
	).Twice()
	storageMock.On("LockSnapshot", ctx, "snapshot", "task").Return(true, nil)
	storageMock.On(
		"RelocateSnapshotChunksToS3",
		ctx,
		"snapshot",
		uint32(0),
		mock.Anything,
	).Run(func(args mock.Arguments) {
		saveProgress := args.Get(3).(func(context.Context, uint32) error)
		require.NoError(t, saveProgress(ctx, 5))
	}).Return(nil)
	execCtx.On("SaveState", ctx).Return(nil).Twice()
	storageMock.On("UnlockSnapshot", ctx, "snapshot", "task").Return(nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Equal(t, float64(1), task.state.Progress)
	mock.AssertExpectationsForObjects(t, storageMock, execCtx)
}

func TestRelocateSnapshotDataFromYDBToS3TaskCancelUnlocks(t *testing.T) {
	ctx := newContext()
	storageMock := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task := &relocateSnapshotDataFromYDBToS3Task{
		storage: storageMock,
		request: &protos.RelocateSnapshotDataFromYDBToS3Request{
			SnapshotId: "snapshot",
		},
		state: &protos.RelocateSnapshotDataFromYDBToS3TaskState{},
	}

	execCtx.On("GetTaskID").Return("task")
	storageMock.On("UnlockSnapshot", ctx, "snapshot", "task").Return(nil)

	err := task.Cancel(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storageMock, execCtx)
}
