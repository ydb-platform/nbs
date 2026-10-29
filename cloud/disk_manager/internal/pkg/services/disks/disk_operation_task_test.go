package disks

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

func TestDiskOperationGuardRejectsBusyDiskBeforeNbs(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	exec := tasks_mocks.NewExecutionContextMock()
	factory := nbs_mocks.NewFactoryMock()
	task := &resizeDiskTask{nbsFactory: factory, request: &protos.ResizeDiskRequest{
		Disk: &types.Disk{ZoneId: "cell", DiskId: "leader"}, Size: 8192,
	}, state: &protos.ResizeDiskTaskState{}}
	exec.On("GetTaskID").Return("resize-task")
	storage.On("GetDiskMeta", ctx, "leader").Return(&resources.DiskMeta{CreateTaskID: "create-task"}, nil).Once()
	expectDiskOperationCheckpoint(t, ctx, exec, errors.NewNonRetriableError(fmt.Errorf("%w", resources.ErrDiskOperationInProgress)))
	storage.On("ReserveDiskOperation", ctx, mock.Anything, resources.DiskOperation{
		DiskID: "leader", CellID: "cell", TaskID: "resize-task", Kind: resources.DiskOperationResize, CreateTaskID: "create-task",
	}).Return(nil, errors.NewNonRetriableError(fmt.Errorf("%w", resources.ErrDiskOperationInProgress))).Once()
	guard := guardDiskOperation(task, storage, resources.DiskOperationResize)
	require.ErrorIs(t, guard.Run(ctx, exec), errors.NewEmptyNonCancellableError())
	factory.AssertNotCalled(t, "GetClient", mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, storage, exec, factory)
}

func TestDiskOperationGuardKeepsMigrationReservationWhileWaiting(t *testing.T) {
	ctx := logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
	storage := storage_mocks.NewStorageMock()
	exec := tasks_mocks.NewExecutionContextMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	task := &migrateDiskTask{scheduler: scheduler, request: &protos.MigrateDiskRequest{
		Disk: &types.Disk{ZoneId: "cell", DiskId: "leader"}, DstZoneId: "target",
	}, state: &protos.MigrateDiskTaskState{Status: protos.MigrationStatus_Replicating, ReplicateTaskID: "replicate"}}
	exec.On("GetTaskID").Return("migration-task")
	storage.On("GetDiskMeta", ctx, "leader").Return(&resources.DiskMeta{CreateTaskID: "create-task"}, nil).Once()
	expectDiskOperationCheckpoint(t, ctx, exec, nil)
	storage.On("ReserveDiskOperation", ctx, mock.Anything, resources.DiskOperation{
		DiskID: "leader", CellID: "cell", TaskID: "migration-task", Kind: resources.DiskOperationMigrate, CreateTaskID: "create-task",
	}).Return(&resources.DiskMeta{CreateTaskID: "create-task"}, nil).Once()
	scheduler.On("WaitTask", ctx, mock.Anything, "replicate").Return(nil, errors.NewInterruptExecutionError()).Once()
	guard := guardDiskOperation(task, storage, resources.DiskOperationMigrate)
	require.ErrorIs(t, guard.Run(ctx, exec), errors.NewInterruptExecutionError())
	require.True(t, task.state.Operation.Acquired)
	require.False(t, task.state.Operation.Released)
	storage.AssertNotCalled(t, "ReleaseDiskOperation", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, storage, exec, scheduler)
}

func TestDiskOperationGuardFinishesResizeAndReleaseTogether(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	exec := tasks_mocks.NewExecutionContextMock()
	factory := nbs_mocks.NewFactoryMock()
	client := nbs_mocks.NewClientMock()
	task := &resizeDiskTask{nbsFactory: factory, request: &protos.ResizeDiskRequest{
		Disk: &types.Disk{ZoneId: "cell", DiskId: "leader"}, Size: 8192,
	}, state: &protos.ResizeDiskTaskState{}}
	exec.On("GetTaskID").Return("resize-task")
	exec.On("SaveState", ctx).Return(nil)
	storage.On("GetDiskMeta", ctx, "leader").Return(&resources.DiskMeta{CreateTaskID: "create-task"}, nil).Once()
	expectDiskOperationCheckpoint(t, ctx, exec, nil)
	storage.On("ReserveDiskOperation", ctx, mock.Anything, resources.DiskOperation{
		DiskID: "leader", CellID: "cell", TaskID: "resize-task", Kind: resources.DiskOperationResize, CreateTaskID: "create-task",
	}).Return(&resources.DiskMeta{CreateTaskID: "create-task"}, nil).Once()
	factory.On("GetClient", ctx, "cell").Return(client, nil).Once()
	client.On("Describe", ctx, "leader").Return(nbs.DiskParams{Kind: types.DiskKind_DISK_KIND_SSD}, nil).Once()
	client.On("Resize", ctx, mock.Anything, "leader", uint64(8192)).Run(func(args mock.Arguments) {
		require.NoError(t, args.Get(1).(func() error)())
	}).Return(nil).Once()
	storage.On("ReleaseDiskOperation", ctx, mock.Anything, "leader", "resize-task").Return(nil).Once()
	exec.On("FinishWithPreparation", ctx, mock.Anything).Run(func(args mock.Arguments) {
		require.True(t, task.state.Operation.Released)
		require.NoError(t, args.Get(1).(func(context.Context, *persistence.Transaction) error)(ctx, nil))
	}).Return(nil).Once()
	require.NoError(t, guardDiskOperation(task, storage, resources.DiskOperationResize).Run(ctx, exec))
	mock.AssertExpectationsForObjects(t, storage, exec, factory, client)
}

func TestDiskOperationGuardDoesNotRepeatAfterFailureRelease(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	exec := tasks_mocks.NewExecutionContextMock()
	factory := nbs_mocks.NewFactoryMock()
	client := nbs_mocks.NewClientMock()
	request := &protos.ResizeDiskRequest{Disk: &types.Disk{ZoneId: "cell", DiskId: "leader"}, Size: 8192}
	task := &resizeDiskTask{nbsFactory: factory, request: request, state: &protos.ResizeDiskTaskState{}}
	exec.On("GetTaskID").Return("resize-task")
	storage.On("GetDiskMeta", ctx, "leader").Return(&resources.DiskMeta{CreateTaskID: "create-task"}, nil).Once()
	expectDiskOperationCheckpoint(t, ctx, exec, nil)
	storage.On("ReserveDiskOperation", ctx, mock.Anything, resources.DiskOperation{
		DiskID: "leader", CellID: "cell", TaskID: "resize-task", Kind: resources.DiskOperationResize, CreateTaskID: "create-task",
	}).Return(&resources.DiskMeta{CreateTaskID: "create-task"}, nil).Once()
	factory.On("GetClient", ctx, "cell").Return(client, nil).Once()
	client.On("Describe", ctx, "leader").Return(nbs.DiskParams{Kind: types.DiskKind_DISK_KIND_SSD_LOCAL}, nil).Once()
	storage.On("ReleaseDiskOperation", ctx, mock.Anything, "leader", "resize-task").Return(nil).Once()
	var persisted []byte
	exec.On("SaveStateWithPreparation", ctx, mock.Anything).Run(func(args mock.Arguments) {
		var err error
		persisted, err = task.Save()
		require.NoError(t, err)
		require.NoError(t, args.Get(1).(func(context.Context, *persistence.Transaction) error)(ctx, nil))
	}).Return(fmt.Errorf("reply lost after commit")).Once()
	require.ErrorIs(t, guardDiskOperation(task, storage, resources.DiskOperationResize).Run(ctx, exec), errors.NewEmptyRetriableError())
	requestBytes, err := proto.Marshal(request)
	require.NoError(t, err)
	restored := &resizeDiskTask{nbsFactory: factory}
	guard := guardDiskOperation(restored, storage, resources.DiskOperationResize)
	require.NoError(t, guard.Load(requestBytes, persisted))
	require.ErrorIs(t, guard.Run(ctx, exec), errors.NewEmptyNonCancellableError())
	mock.AssertExpectationsForObjects(t, storage, exec, factory, client)
}

func expectDiskOperationCheckpoint(t *testing.T, ctx context.Context, exec *tasks_mocks.ExecutionContextMock, result error) {
	t.Helper()
	exec.On("SaveStateWithPreparation", ctx, mock.Anything).Run(func(args mock.Arguments) {
		err := args.Get(1).(func(context.Context, *persistence.Transaction) error)(ctx, nil)
		if isTerminalDiskOperationError(result) {
			require.Error(t, err)
			require.Equal(t, errors.ErrorForTracing(result), errors.ErrorForTracing(err))
		} else {
			require.NoError(t, err)
		}
	}).Return(result).Once()
}

func TestDiskOperationGuardRetriesTerminalCancelError(t *testing.T) {
	for _, test := range []struct {
		name     string
		terminal error
	}{
		{"non-retriable", errors.NewNonRetriableErrorf("destination unavailable")},
		{"non-cancellable", errors.NewNonCancellableErrorf("cleanup rejected")},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.DebugLevel))
			storage := storage_mocks.NewStorageMock()
			exec := tasks_mocks.NewExecutionContextMock()
			factory := nbs_mocks.NewFactoryMock()
			client := nbs_mocks.NewClientMock()
			task := &migrateDiskTask{nbsFactory: factory, request: &protos.MigrateDiskRequest{
				Disk: &types.Disk{ZoneId: "cell", DiskId: "leader"}, DstZoneId: "target",
			}, state: &protos.MigrateDiskTaskState{FillGeneration: 7, Operation: &protos.DiskOperationTaskState{Acquired: true}}}
			exec.On("GetTaskID").Return("migration-task")
			exec.On("HasEvent", ctx, int64(protos.MigrateDiskTaskEvents_FINISH_MIGRATION)).Return(false).Twice()
			factory.On("GetClient", ctx, "target").Return(client, test.terminal).Once()
			guard := guardDiskOperation(task, storage, resources.DiskOperationMigrate)
			err := guard.Cancel(ctx, exec)
			require.ErrorIs(t, err, errors.NewEmptyRetriableError())
			require.False(t, isTerminalDiskOperationError(err))
			require.True(t, task.state.Operation.Acquired)
			require.False(t, task.state.Operation.Released)
			storage.AssertNotCalled(t, "ReleaseDiskOperation", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			factory.On("GetClient", ctx, "target").Return(client, nil).Once()
			client.On("DeleteWithFillGeneration", ctx, "leader", uint64(7)).Return(nil).Once()
			storage.On("ReleaseDiskOperation", ctx, mock.Anything, "leader", "migration-task").Return(nil).Once()
			expectDiskOperationCheckpoint(t, ctx, exec, nil)
			require.NoError(t, guard.Cancel(ctx, exec))
			require.True(t, task.state.Operation.Released)
			require.False(t, task.state.Operation.Acquired)
			mock.AssertExpectationsForObjects(t, storage, exec, factory, client)
		})
	}
}

func TestDiskOperationGuardCancelsLegacyMigrationAfterReload(t *testing.T) {
	ctx := logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.DebugLevel))
	storage := storage_mocks.NewStorageMock()
	exec := tasks_mocks.NewExecutionContextMock()
	factory := nbs_mocks.NewFactoryMock()
	client := nbs_mocks.NewClientMock()
	request, err := proto.Marshal(&protos.MigrateDiskRequest{
		Disk: &types.Disk{ZoneId: "cell", DiskId: "leader"}, DstZoneId: "target",
	})
	require.NoError(t, err)
	legacy, err := proto.Marshal(&protos.MigrateDiskTaskState{
		IsDiskCloned: true, FillGeneration: 7, Status: protos.MigrationStatus_Replicating, ReplicateTaskID: "replicate",
	})
	require.NoError(t, err)
	task := &migrateDiskTask{nbsFactory: factory}
	guard := guardDiskOperation(task, storage, resources.DiskOperationMigrate)
	require.NoError(t, guard.Load(request, legacy))
	require.True(t, task.state.Operation.CleanupRequired)
	require.False(t, task.state.Operation.Acquired)
	saved, err := guard.Save()
	require.NoError(t, err)
	task = &migrateDiskTask{nbsFactory: factory}
	guard = guardDiskOperation(task, storage, resources.DiskOperationMigrate)
	require.NoError(t, guard.Load(request, saved))
	exec.On("GetTaskID").Return("migration-task")
	exec.On("HasEvent", ctx, int64(protos.MigrateDiskTaskEvents_FINISH_MIGRATION)).Return(false).Once()
	factory.On("GetClient", ctx, "target").Return(client, nil).Once()
	client.On("DeleteWithFillGeneration", ctx, "leader", uint64(7)).Return(nil).Once()
	storage.On("ReleaseDiskOperation", ctx, mock.Anything, "leader", "migration-task").Return(nil).Once()
	expectDiskOperationCheckpoint(t, ctx, exec, nil)
	require.NoError(t, guard.Cancel(ctx, exec))
	require.True(t, task.state.Operation.Released)
	require.NoError(t, guard.Cancel(ctx, exec))
	mock.AssertExpectationsForObjects(t, storage, exec, factory, client)
}

func TestDiskOperationGuardStopsObsoleteExecutionBeforeNbs(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	exec := tasks_mocks.NewExecutionContextMock()
	factory := nbs_mocks.NewFactoryMock()
	task := &resizeDiskTask{nbsFactory: factory, request: &protos.ResizeDiskRequest{
		Disk: &types.Disk{ZoneId: "cell", DiskId: "leader"}, Size: 8192,
	}, state: &protos.ResizeDiskTaskState{}}
	exec.On("GetTaskID").Return("resize-task")
	storage.On("GetDiskMeta", ctx, "leader").Return(&resources.DiskMeta{CreateTaskID: "create-task"}, nil).Once()
	storage.On("ReserveDiskOperation", ctx, mock.Anything, resources.DiskOperation{
		DiskID: "leader", CellID: "cell", TaskID: "resize-task", Kind: resources.DiskOperationResize, CreateTaskID: "create-task",
	}).Return(&resources.DiskMeta{CreateTaskID: "create-task"}, nil).Once()
	expectDiskOperationCheckpoint(t, ctx, exec, errors.NewWrongGenerationError())
	require.ErrorIs(t, guardDiskOperation(task, storage, resources.DiskOperationResize).Run(ctx, exec), errors.NewWrongGenerationError())
	require.False(t, task.state.Operation.Acquired)
	factory.AssertNotCalled(t, "GetClient", mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, storage, exec, factory)
}

func TestDiskOperationGuardSkipsCleanupOfNewUnstartedMigration(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	exec := tasks_mocks.NewExecutionContextMock()
	factory := nbs_mocks.NewFactoryMock()
	request, err := proto.Marshal(&protos.MigrateDiskRequest{
		Disk: &types.Disk{ZoneId: "cell", DiskId: "leader"}, DstZoneId: "target",
	})
	require.NoError(t, err)
	task := &migrateDiskTask{nbsFactory: factory}
	guard := guardDiskOperation(task, storage, resources.DiskOperationMigrate)
	require.NoError(t, guard.Load(request, nil))
	require.False(t, task.state.Operation.CleanupRequired)
	exec.On("GetTaskID").Return("migration-task")
	storage.On("ReleaseDiskOperation", ctx, mock.Anything, "leader", "migration-task").Return(nil).Once()
	expectDiskOperationCheckpoint(t, ctx, exec, nil)
	require.NoError(t, guard.Cancel(ctx, exec))
	factory.AssertNotCalled(t, "GetClient", mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, storage, exec, factory)
}

// The framework holds its execution-context mutex while running preparation.
// Fail immediately on reentry instead of hanging until the test suite times out.
type preparationLockedExecutionContext struct {
	tasks.ExecutionContext
	t     *testing.T
	mutex sync.Mutex
}

func (c *preparationLockedExecutionContext) GetTaskID() string {
	require.True(c.t, c.mutex.TryLock(), "GetTaskID reentered the execution context during preparation")
	defer c.mutex.Unlock()
	return c.ExecutionContext.GetTaskID()
}

func (c *preparationLockedExecutionContext) SaveStateWithPreparation(ctx context.Context, preparation func(context.Context, *persistence.Transaction) error) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.ExecutionContext.SaveStateWithPreparation(ctx, preparation)
}

func (c *preparationLockedExecutionContext) FinishWithPreparation(ctx context.Context, preparation func(context.Context, *persistence.Transaction) error) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.ExecutionContext.FinishWithPreparation(ctx, preparation)
}

func TestDiskOperationGuardReleasesReservationWithoutReenteringExecutionContext(t *testing.T) {
	for _, method := range []string{"SaveStateWithPreparation", "FinishWithPreparation"} {
		t.Run(method, func(t *testing.T) {
			ctx := context.Background()
			storage := storage_mocks.NewStorageMock()
			exec := tasks_mocks.NewExecutionContextMock()
			exec.On("GetTaskID").Return("resize-task").Once()
			exec.On(method, ctx, mock.Anything).Run(func(args mock.Arguments) {
				require.NoError(t, args.Get(1).(func(context.Context, *persistence.Transaction) error)(ctx, nil))
			}).Return(nil).Once()
			storage.On("ReleaseDiskOperation", ctx, mock.Anything, "leader", "resize-task").Return(nil).Once()
			task := &resizeDiskTask{
				request: &protos.ResizeDiskRequest{Disk: &types.Disk{ZoneId: "cell", DiskId: "leader"}},
				state: &protos.ResizeDiskTaskState{Operation: &protos.DiskOperationTaskState{
					Acquired: true,
				}},
			}
			guard := guardDiskOperation(task, storage, resources.DiskOperationResize).(*diskOperationTask)
			locked := &preparationLockedExecutionContext{ExecutionContext: exec, t: t}
			if method == "FinishWithPreparation" {
				guarded := &diskOperationExecutionContext{ExecutionContext: locked, task: guard}
				require.NoError(t, guarded.FinishWithPreparation(ctx, nil))
			} else {
				require.NoError(t, guard.saveRelease(ctx, locked))
			}
			require.True(t, task.state.Operation.Released)
			require.False(t, task.state.Operation.Acquired)
			mock.AssertExpectationsForObjects(t, storage, exec)
		})
	}
}
