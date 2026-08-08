package dataplane

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestRelocateChunkDataFromYDBToS3TaskProcessesBatch(t *testing.T) {
	ctx := newContext()
	storageMock := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task := &relocateChunkDataFromYDBToS3Task{
		storage: storageMock,
		request: &protos.RelocateChunkDataFromYDBToS3Request{
			ChunkIds:    []string{"c1", "c2"},
			KeepYdbData: true,
		},
		state: &protos.RelocateChunkDataFromYDBToS3TaskState{},
	}

	storageMock.On("RelocateChunkDataToS3", ctx, "c1", true).Return(nil)
	storageMock.On("RelocateChunkDataToS3", ctx, "c2", true).Return(nil)
	execCtx.On("SaveState", ctx).Return(nil).Times(3)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Equal(t, uint32(2), task.state.MilestoneIndex)
	require.Equal(t, float64(1), task.state.Progress)
	storageMock.AssertExpectations(t)
	execCtx.AssertExpectations(t)
}

func TestRelocateChunkDataFromYDBToS3TaskResumesMilestone(t *testing.T) {
	ctx := newContext()
	storageMock := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task := &relocateChunkDataFromYDBToS3Task{
		storage: storageMock,
		request: &protos.RelocateChunkDataFromYDBToS3Request{
			ChunkIds:    []string{"c1", "c2", "c3"},
			KeepYdbData: false,
		},
		state: &protos.RelocateChunkDataFromYDBToS3TaskState{
			MilestoneIndex: 2,
		},
	}

	storageMock.On("RelocateChunkDataToS3", ctx, "c3", false).Return(nil)
	execCtx.On("SaveState", ctx).Return(nil).Twice()

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Equal(t, uint32(3), task.state.MilestoneIndex)
	storageMock.AssertExpectations(t)
}

func TestChunkBatchKeyStable(t *testing.T) {
	a := chunkBatchKey([]string{"a", "b"})
	b := chunkBatchKey([]string{"a", "b"})
	c := chunkBatchKey([]string{"b", "a"})
	require.Equal(t, a, b)
	require.NotEqual(t, a, c)
}
