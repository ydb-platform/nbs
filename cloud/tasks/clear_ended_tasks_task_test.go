package tasks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/storage/mocks"
)

func TestClearEndedTasksDoesNotDependOnDelayedQueue(t *testing.T) {
	ctx := newContext()
	for _, gcErr := range []error{nil, errors.NewRetriableErrorf("GC failed")} {
		s := mocks.NewStorageMock()
		s.On("ClearEndedTasks", ctx, mock.Anything, 10).Return(gcErr).Once()
		task := clearEndedTasksTask{storage: s, expirationTimeout: time.Hour, limit: 10}
		require.Equal(t, gcErr, task.Run(ctx, nil))
		// No access to the delayed table is necessary to run GC.
		s.AssertExpectations(t)
	}
}
