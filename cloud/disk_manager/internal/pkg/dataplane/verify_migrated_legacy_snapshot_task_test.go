package dataplane

import (
	"crypto/rand"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func newStorageWithSnapshot(
	snapshotID string, data []byte,
) *storage_mocks.StorageMock {

	chunkCount := uint32(math.Ceil(float64(len(data)) / chunkSize))

	mockStorage := storage_mocks.NewStorageMock()

	snapshotMeta := storage.SnapshotMeta{
		Size:       uint64(len(data)),
		ChunkCount: chunkCount,
	}
	mockStorage.On(
		"CheckSnapshotReady", mock.Anything, snapshotID,
	).Return(snapshotMeta, nil)

	entries := make(chan storage.ChunkMapEntry, chunkCount)
	for i := range chunkCount {
		chunkId := fmt.Sprintf("chunk_%d", i)
		entries <- storage.ChunkMapEntry{ChunkIndex: i, ChunkID: chunkId}
	}
	errors := make(chan error)
	close(entries)
	close(errors)

	entriesOut := (<-chan storage.ChunkMapEntry)(entries)
	errorsOut := (<-chan error)(errors)
	mockStorage.On(
		"ReadChunkMap", mock.Anything, snapshotID, mock.Anything,
	).Return(entriesOut, errorsOut).Once()

	mockStorage.On(
		"ReadChunk", mock.Anything, mock.Anything,
	).Run(func(args mock.Arguments) {
		chunk := args[1].(*common.Chunk)
		left := chunkSize * int(chunk.Index)
		right := int(math.Min(float64(left+chunkSize), float64(len(data))))
		chunk.Data = data[left:right]
	}).Return(nil)

	return mockStorage
}

func buildTask(
	snapshotID string, storage, legacyStorage storage.Storage,
) *verifyMigratedLegacySnapshotTask {

	readerCount := uint32(11)
	writerCount := uint32(22)
	chunksInlfightLimit := uint32(100500)
	config := &config.DataplaneConfig{
		ReaderCount:         &readerCount,
		WriterCount:         &writerCount,
		ChunksInflightLimit: &chunksInlfightLimit,
	}

	return &verifyMigratedLegacySnapshotTask{
		config:        config,
		storage:       storage,
		legacyStorage: legacyStorage,
		request:       &protos.VerifyMigratedLegacySnapshotRequest{SnapshotId: snapshotID},
		state:         &protos.VerifyMigratedLegacySnapshotState{},
	}
}

////////////////////////////////////////////////////////////////////////////////

func testVerifyMigratedSnapshotSuccess(
	t *testing.T, snapshotSize int, chunkCount int,
) {

	ctx := newContext()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("SaveState", mock.Anything).Return(nil)
	snapshotID := "snapshot_42"

	data := make([]byte, snapshotSize)
	_, _ = rand.Read(data)

	storage1 := newStorageWithSnapshot(snapshotID, data)
	storage2 := newStorageWithSnapshot(snapshotID, data)

	task := buildTask(snapshotID, storage1, storage2)
	err := task.Run(ctx, execCtx)
	require.NoError(t, err)

	require.EqualValues(t, chunkCount, task.state.ChunkCount)
	require.EqualValues(t, chunkCount, task.state.MilestoneChunkIndex)
	require.EqualValues(t, chunkCount, task.state.LegacyMilestoneChunkIndex)
	require.Equal(t, 1.0, task.state.Progress)

	storage1.AssertExpectations(t)
	storage2.AssertExpectations(t)
}

func TestVerifyMigratedSnapshotSuccess(t *testing.T) {
	testVerifyMigratedSnapshotSuccess(t, 1 /* snapshotSize */, 1 /* chunkCount */)
	testVerifyMigratedSnapshotSuccess(t, chunkSize, 1)
	testVerifyMigratedSnapshotSuccess(t, 2*chunkSize, 2)
	testVerifyMigratedSnapshotSuccess(t, chunkSize+1, 2)
	testVerifyMigratedSnapshotSuccess(t, chunkSize+chunkSize/2, 2)
}

func TestVerifyMigratedSnapshotDifferentData(t *testing.T) {
	ctx := newContext()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("SaveState", mock.Anything).Return(nil)
	snapshotID := "snapshot_42"

	data1 := make([]byte, 1500)
	_, _ = rand.Read(data1)
	storage1 := newStorageWithSnapshot(snapshotID, data1)

	data2 := make([]byte, 1500)
	_, _ = rand.Read(data2)
	storage2 := newStorageWithSnapshot(snapshotID, data2)

	task := buildTask(snapshotID, storage1, storage2)
	err := task.Run(ctx, execCtx)
	require.ErrorContains(t, err, "checksum mismatch")

	require.Equal(t, 1.0, task.state.Progress)

	storage1.AssertExpectations(t)
	storage2.AssertExpectations(t)
}

func TestVerifyMigratedSnapshotDifferentOneByte(t *testing.T) {
	ctx := newContext()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("SaveState", mock.Anything).Return(nil)
	snapshotID := "snapshot_42"

	data1 := make([]byte, 1500)
	_, _ = rand.Read(data1)
	storage1 := newStorageWithSnapshot(snapshotID, data1)

	data2 := make([]byte, 1500)
	copy(data2, data1)
	data2[0] += 1
	storage2 := newStorageWithSnapshot(snapshotID, data2)

	task := buildTask(snapshotID, storage1, storage2)
	err := task.Run(ctx, execCtx)
	require.ErrorContains(t, err, "checksum mismatch at index 0")

	require.Equal(t, 1.0, task.state.Progress)

	storage1.AssertExpectations(t)
	storage2.AssertExpectations(t)
}

func TestVerifyMigratedSnapshotDifferentSize(t *testing.T) {
	ctx := newContext()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("SaveState", mock.Anything).Return(nil)
	snapshotID := "snapshot_42"

	data1 := make([]byte, 1000)
	storage1 := newStorageWithSnapshot(snapshotID, data1)

	data2 := make([]byte, 1500)
	storage2 := newStorageWithSnapshot(snapshotID, data2)

	task := buildTask(snapshotID, storage1, storage2)
	err := task.Run(ctx, execCtx)
	require.ErrorContains(t, err, "size mismatch: 1000 != 1500")

	require.Equal(t, 0.0, task.state.Progress)
}

func TestVerifyMigratedSnapshotCheckSnapshotReadyErrorIsMadeNonSilent(t *testing.T) {
	ctx := newContext()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("SaveState", mock.Anything).Return(nil)
	snapshotID := "snapshot_42"

	mockStorage := storage_mocks.NewStorageMock()
	mockStorage.On(
		"CheckSnapshotReady", mock.Anything, snapshotID,
	).Return(
		storage.SnapshotMeta{}, errors.NewSilentNonRetriableErrorf(
			"snapshot with id %v is not found", snapshotID,
		),
	).Once()

	task := buildTask(snapshotID, mockStorage, mockStorage)
	err := task.Run(ctx, execCtx)
	require.Error(t, err)
	require.False(t, errors.IsSilent(err))
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	mockStorage.AssertExpectations(t)
}

func TestMakeNonRetriableErrorsNonSilent(t *testing.T) {
	// Non-retriable errors are made non-silent
	silentErr := errors.NewSilentNonRetriableErrorf("error")
	err := makeNonRetriableErrorsNonSilent(silentErr)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.False(t, errors.IsSilent(err))

	// Retriable errors are kept as-is
	retriableErr := errors.NewEmptyRetriableError()
	err = makeNonRetriableErrorsNonSilent(retriableErr)
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))
}
