package tasks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/storage/mocks"
)

func TestClearEndedTasksReconcilesDelayedQueue(t *testing.T) {
	ctx := newContext()
	for _, fail := range []bool{false, true} {
		s := mocks.NewStorageMock()
		var reconcileErr error
		if fail {
			reconcileErr = errors.NewRetriableErrorf("reconciliation failed")
		}
		reconcile := s.On("ReconcileReadyToRunDelayed", ctx, 10).Return(reconcileErr).Once()
		if !fail {
			s.On("ClearEndedTasks", ctx, mock.Anything, 10).Return(nil).Once().NotBefore(reconcile)
		}
		task := clearEndedTasksTask{storage: s, expirationTimeout: time.Hour, limit: 10}
		require.Equal(t, reconcileErr, task.Run(ctx, nil))
		s.AssertExpectations(t)
	}
}
