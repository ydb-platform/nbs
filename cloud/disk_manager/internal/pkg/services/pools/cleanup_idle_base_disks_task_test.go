package pools

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	pools_storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage/mocks"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestCleanupIdleBaseDisksTaskNoIdleDisks(t *testing.T) {
	ctx := newContext()
	s := pools_storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task := &cleanupIdleBaseDisksTask{
		storage:         s,
		baseDiskIdleTTL: 24 * time.Hour,
		limit:           100,
	}

	s.On(
		"GetIdleBaseDisks",
		ctx,
		24*time.Hour,
		uint64(100),
	).Return([]storage.BaseDisk{}, nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, s, execCtx)
}

func TestCleanupIdleBaseDisksTaskWithIdleDisks(t *testing.T) {
	ctx := newContext()
	s := pools_storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task := &cleanupIdleBaseDisksTask{
		storage:         s,
		baseDiskIdleTTL: 7 * 24 * time.Hour,
		limit:           50,
	}

	idleDisks := []storage.BaseDisk{
		{
			ID:      "baseDisk1",
			ImageID: "image1",
			ZoneID:  "zone1",
		},
		{
			ID:      "baseDisk2",
			ImageID: "image1",
			ZoneID:  "zone1",
		},
		{
			ID:      "baseDisk3",
			ImageID: "image2",
			ZoneID:  "zone2",
		},
	}

	s.On(
		"GetIdleBaseDisks",
		ctx,
		7*24*time.Hour,
		uint64(50),
	).Return(idleDisks, nil)

	s.On(
		"EjectIdleBaseDisksFromPool",
		ctx,
		[]string{"baseDisk1", "baseDisk2", "baseDisk3"},
		mock.AnythingOfType("time.Time"),
	).Return(nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, s, execCtx)
}

func TestCleanupIdleBaseDisksTaskCancel(t *testing.T) {
	ctx := newContext()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task := &cleanupIdleBaseDisksTask{
		baseDiskIdleTTL: 24 * time.Hour,
		limit:           100,
	}

	err := task.Cancel(ctx, execCtx)
	require.NoError(t, err)
}
