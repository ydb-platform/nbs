package dataplane

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

type backupChunkDeleterMock struct {
	mock.Mock
}

func (m *backupChunkDeleterMock) DeleteChunks(ctx context.Context, ids []string) ([]string, error) {
	args := m.Called(ctx, ids)
	return args.Get(0).([]string), args.Error(1)
}

func TestDeleteBackupChunksTaskConfirmsPartialBatches(t *testing.T) {
	ctx := test.NewContext()
	follower := &backupChunkDeleterMock{}
	storage := storage_mocks.NewStorageMock()

	deleted := make([]string, backupChunkDeleteRequestSize-1)
	for i := range deleted {
		deleted[i] = strconv.Itoa(i)
	}
	firstBatch := append(deleted, "failed-key")
	secondBatch := []string{"failed-request"}
	queue := append(firstBatch, secondBatch...)
	pending := []string{"failed-key", "failed-request"}
	task := deleteBackupChunksTask{
		storage:       storage,
		backupS3:      follower,
		batchSize:     len(queue),
		inflightLimit: 2,
		registry:      metrics.NewEmptyRegistry(),
	}

	s3Err := errors.NewRetriableErrorf("S3 delete failed")
	storage.On("GetBackupChunksToDelete", ctx, len(queue)).Return(queue, nil).Once()
	follower.On("DeleteChunks", ctx, firstBatch).Return(deleted, s3Err).Once()
	follower.On("DeleteChunks", ctx, secondBatch).Return([]string(nil), s3Err).Once()
	storage.On("BackupChunksDeleted", ctx, deleted).Return(nil).Once()
	require.ErrorIs(t, task.Run(ctx, nil), s3Err)

	storage.On("GetBackupChunksToDelete", ctx, len(queue)).Return(pending, nil).Once()
	follower.On("DeleteChunks", ctx, pending).Return(pending, nil).Once()
	storage.On("BackupChunksDeleted", ctx, pending).Return(nil).Once()
	storage.On("GetBackupChunksToDelete", ctx, len(queue)).Return([]string{}, nil).Once()
	require.ErrorIs(t, task.Run(ctx, nil), errors.NewInterruptExecutionError())

	follower.AssertExpectations(t)
	storage.AssertExpectations(t)
}
