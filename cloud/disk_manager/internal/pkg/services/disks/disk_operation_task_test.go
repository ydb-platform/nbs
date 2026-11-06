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

func TestDiskOperationGuardRejectsExclusiveOwnerBeforeNbs(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	exec := tasks_mocks.NewExecutionContextMock()
	factory := nbs_mocks.NewFactoryMock()
	task := &resizeDiskTask{nbsFactory: factory, request: &protos.ResizeDiskRequest{UseDiskOperationGuard: true,
		Disk: &types.Disk{ZoneId: "cell", DiskId: "leader"}, Size: 8192,
	}, state: &protos.ResizeDiskTaskState{}}
	exec.On("GetTaskID").Return("resize-task")
	expectDiskOperationCheckpoint(t, ctx, exec, errors.NewNonRetriableError(fmt.Errorf("%w", resources.ErrDiskOperationInProgress)))
	storage.On("ReserveDiskOperation", ctx, mock.Anything, resources.DiskOperation{
		DiskID: "leader",
		TaskID: "resize-task",
		Kind:   resources.DiskOperationResize,
	}).Return(nil, errors.NewNonRetriableError(fmt.Errorf("%w", resources.ErrDiskOperationInProgress))).Once()
	guard := guardDiskOperation(task, storage, resources.DiskOperationResize)
	require.ErrorIs(t, guard.Run(ctx, exec), errors.NewEmptyNonRetriableError())
	factory.AssertNotCalled(t, "GetClient", mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, storage, exec, factory)
}

func TestDiskOperationGuardStartsMigrationDuringAnotherCancellation(t *testing.T) {
	ctx := logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
	storage := storage_mocks.NewStorageMock()
	exec := tasks_mocks.NewExecutionContextMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	task := &migrateDiskTask{scheduler: scheduler, request: &protos.MigrateDiskRequest{UseDiskOperationGuard: true,
		Disk: &types.Disk{ZoneId: "cell", DiskId: "leader"}, DstZoneId: "target",
	}, state: &protos.MigrateDiskTaskState{Status: protos.MigrationStatus_Replicating, ReplicateTaskID: "replicate"}}
	exec.On("GetTaskID").Return("migration-task")
	expectDiskOperationCheckpoint(t, ctx, exec, nil)
	storage.On("ReserveDiskOperation", ctx, mock.Anything, resources.DiskOperation{
		DiskID: "leader",
		TaskID: "migration-task",
		Kind:   resources.DiskOperationMigrate,
	}).Return(&resources.DiskMeta{CreateTaskID: "create-task", ActiveOperationTaskIDs: []string{"cancelling-migration", "migration-task"}}, nil).Once()
	scheduler.On("WaitTask", ctx, mock.Anything, "replicate").Return(nil, errors.NewInterruptExecutionError()).Once()
	guard := guardDiskOperation(task, storage, resources.DiskOperationMigrate)
	require.ErrorIs(t, guard.Run(ctx, exec), errors.NewInterruptExecutionError())

	storage.AssertNotCalled(t, "ReleaseDiskOperation", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, storage, exec, scheduler)
}

func TestDiskOperationGuardFinishesResizeAndReleaseTogether(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	exec := tasks_mocks.NewExecutionContextMock()
	factory := nbs_mocks.NewFactoryMock()
	client := nbs_mocks.NewClientMock()
	task := &resizeDiskTask{nbsFactory: factory, request: &protos.ResizeDiskRequest{UseDiskOperationGuard: true,
		Disk: &types.Disk{ZoneId: "cell", DiskId: "leader"}, Size: 8192,
	}, state: &protos.ResizeDiskTaskState{}}
	exec.On("GetTaskID").Return("resize-task")
	exec.On("SaveState", ctx).Return(nil)
	expectDiskOperationCheckpoint(t, ctx, exec, nil)
	storage.On("ReserveDiskOperation", ctx, mock.Anything, resources.DiskOperation{
		DiskID: "leader",
		TaskID: "resize-task",
		Kind:   resources.DiskOperationResize,
	}).Return(&resources.DiskMeta{CreateTaskID: "create-task"}, nil).Once()
	factory.On("GetClient", ctx, "cell").Return(client, nil).Once()
	client.On("Describe", ctx, "leader").Return(nbs.DiskParams{Kind: types.DiskKind_DISK_KIND_SSD}, nil).Once()
	client.On("Resize", ctx, mock.Anything, "leader", uint64(8192)).Run(func(args mock.Arguments) {
		require.NoError(t, args.Get(1).(func() error)())
	}).Return(nil).Once()
	storage.On("ReleaseDiskOperation", ctx, mock.Anything, "leader", "resize-task").Return(nil).Once()
	exec.On("FinishWithPreparation", ctx, mock.Anything).Run(func(args mock.Arguments) {
		require.NoError(t, args.Get(1).(func(context.Context, *persistence.Transaction) error)(ctx, nil))
	}).Return(nil).Once()
	require.NoError(t, guardDiskOperation(task, storage, resources.DiskOperationResize).Run(ctx, exec))
	mock.AssertExpectationsForObjects(t, storage, exec, factory, client)
}

func TestDiskOperationGuardRepeatsRunAfterFailureReleaseError(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	exec := tasks_mocks.NewExecutionContextMock()
	factory := nbs_mocks.NewFactoryMock()
	client := nbs_mocks.NewClientMock()
	request := &protos.ResizeDiskRequest{
		UseDiskOperationGuard: true,
		Disk:                  &types.Disk{ZoneId: "cell", DiskId: "leader"},
		Size:                  8192,
	}

	task := &resizeDiskTask{
		nbsFactory: factory,
		request:    request,
		state:      &protos.ResizeDiskTaskState{},
	}

	exec.On("GetTaskID").Return("resize-task")
	expectDiskOperationCheckpoint(t, ctx, exec, nil)
	storage.On(
		"ReserveDiskOperation", ctx, mock.Anything,
		resources.DiskOperation{
			DiskID: "leader",
			TaskID: "resize-task",
			Kind:   resources.DiskOperationResize,
		},
	).Return(&resources.DiskMeta{CreateTaskID: "create-task"}, nil).Twice()
	factory.On("GetClient", ctx, "cell").Return(client, nil).Twice()
	client.On("Describe", ctx, "leader").Return(
		nbs.DiskParams{Kind: types.DiskKind_DISK_KIND_SSD_LOCAL}, nil,
	).Twice()
	storage.On(
		"ReleaseDiskOperation", ctx, mock.Anything, "leader", "resize-task",
	).Return(nil).Twice()

	var persisted []byte
	exec.On("SaveStateWithPreparation", ctx, mock.Anything).Run(
		func(args mock.Arguments) {
			var err error
			persisted, err = task.Save()
			require.NoError(t, err)
			prepare := args.Get(1).(func(
				context.Context, *persistence.Transaction,
			) error)
			require.NoError(t, prepare(ctx, nil))
		},
	).Return(errors.NewRetriableErrorf("reply lost after commit")).Once()
	guard := guardDiskOperation(task, storage, resources.DiskOperationResize)
	require.ErrorIs(
		t, guard.Run(ctx, exec), errors.NewEmptyRetriableError(),
	)

	requestBytes, err := proto.Marshal(request)
	require.NoError(t, err)
	restored := &resizeDiskTask{nbsFactory: factory}
	guard = guardDiskOperation(restored, storage, resources.DiskOperationResize)
	require.NoError(t, guard.Load(requestBytes, persisted))

	expectDiskOperationCheckpoint(t, ctx, exec, nil)
	expectDiskOperationCheckpoint(t, ctx, exec, nil)
	require.ErrorIs(
		t, guard.Run(ctx, exec), errors.NewEmptyNonCancellableError(),
	)

	mock.AssertExpectationsForObjects(t, storage, exec, factory, client)
}

func expectDiskOperationCheckpoint(t *testing.T, ctx context.Context, exec *tasks_mocks.ExecutionContextMock, result error) {
	t.Helper()
	exec.On("SaveStateWithPreparation", ctx, mock.Anything).Run(func(args mock.Arguments) {
		err := args.Get(1).(func(context.Context, *persistence.Transaction) error)(ctx, nil)
		if errors.Is(result, errors.NewEmptyNonRetriableError()) ||
			errors.Is(result, errors.NewEmptyNonCancellableError()) {
			require.Error(t, err)
			require.Equal(t, errors.ErrorForTracing(result), errors.ErrorForTracing(err))
		} else {
			require.NoError(t, err)
		}
	}).Return(result).Once()
}

func TestDiskOperationGuardPreservesCancelErrors(t *testing.T) {
	for _, test := range []struct {
		name    string
		err     error
		release bool
	}{
		{
			name:    "non-retriable",
			err:     errors.NewNonRetriableErrorf("destination unavailable"),
			release: true,
		},
		{
			name:    "non-cancellable",
			err:     errors.NewNonCancellableErrorf("cleanup rejected"),
			release: true,
		},
		{
			name: "retry",
			err:  errors.NewRetriableErrorf("destination unavailable"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := logging.SetLogger(
				context.Background(),
				logging.NewStderrLogger(logging.DebugLevel),
			)
			storage := storage_mocks.NewStorageMock()
			exec := tasks_mocks.NewExecutionContextMock()
			factory := nbs_mocks.NewFactoryMock()
			client := nbs_mocks.NewClientMock()
			task := &migrateDiskTask{
				nbsFactory: factory,
				request: &protos.MigrateDiskRequest{
					UseDiskOperationGuard: true,
					Disk: &types.Disk{
						ZoneId: "cell",
						DiskId: "leader",
					},
					DstZoneId: "target",
				},
				state: &protos.MigrateDiskTaskState{
					FillGeneration: 7,
				},
			}

			exec.On("GetTaskID").Return("migration-task")
			exec.On(
				"HasEvent", ctx,
				int64(protos.MigrateDiskTaskEvents_FINISH_MIGRATION),
			).Return(false).Once()
			factory.On("GetClient", ctx, "target").Return(
				client, test.err,
			).Once()
			if test.release {
				storage.On(
					"ReleaseDiskOperation", ctx, mock.Anything,
					"leader", "migration-task",
				).Return(nil).Once()
				expectDiskOperationCheckpoint(t, ctx, exec, nil)
			}

			guard := guardDiskOperation(
				task, storage, resources.DiskOperationMigrate,
			)
			require.Equal(t, test.err, guard.Cancel(ctx, exec))

			if !test.release {
				storage.AssertNotCalled(
					t, "ReleaseDiskOperation", mock.Anything,
					mock.Anything, mock.Anything, mock.Anything,
				)
			}

			mock.AssertExpectationsForObjects(t, storage, exec, factory, client)
		})
	}
}

func TestDiskOperationGuardStopsObsoleteExecutionBeforeNbs(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	exec := tasks_mocks.NewExecutionContextMock()
	factory := nbs_mocks.NewFactoryMock()
	task := &resizeDiskTask{nbsFactory: factory, request: &protos.ResizeDiskRequest{UseDiskOperationGuard: true,
		Disk: &types.Disk{ZoneId: "cell", DiskId: "leader"}, Size: 8192,
	}, state: &protos.ResizeDiskTaskState{}}
	exec.On("GetTaskID").Return("resize-task")
	storage.On("ReserveDiskOperation", ctx, mock.Anything, resources.DiskOperation{
		DiskID: "leader",
		TaskID: "resize-task",
		Kind:   resources.DiskOperationResize,
	}).Return(&resources.DiskMeta{CreateTaskID: "create-task"}, nil).Once()
	expectDiskOperationCheckpoint(t, ctx, exec, errors.NewWrongGenerationError())
	require.ErrorIs(t, guardDiskOperation(task, storage, resources.DiskOperationResize).Run(ctx, exec), errors.NewWrongGenerationError())

	factory.AssertNotCalled(t, "GetClient", mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, storage, exec, factory)
}

func TestDiskOperationGuardCancelsUnstartedMigration(t *testing.T) {
	ctx := logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
	storage := storage_mocks.NewStorageMock()
	exec := tasks_mocks.NewExecutionContextMock()
	factory := nbs_mocks.NewFactoryMock()
	client := nbs_mocks.NewClientMock()
	request, err := proto.Marshal(&protos.MigrateDiskRequest{
		UseDiskOperationGuard: true,
		Disk:                  &types.Disk{ZoneId: "cell", DiskId: "leader"},
		DstZoneId:             "target",
	})
	require.NoError(t, err)
	task := &migrateDiskTask{nbsFactory: factory}
	guard := guardDiskOperation(task, storage, resources.DiskOperationMigrate)
	require.NoError(t, guard.Load(request, nil))
	exec.On("GetTaskID").Return("migration-task")
	exec.On(
		"HasEvent", ctx, int64(protos.MigrateDiskTaskEvents_FINISH_MIGRATION),
	).Return(false).Once()
	factory.On("GetClient", ctx, "target").Return(client, nil).Once()
	storage.On(
		"ReleaseDiskOperation", ctx, mock.Anything, "leader", "migration-task",
	).Return(nil).Once()
	expectDiskOperationCheckpoint(t, ctx, exec, nil)
	require.NoError(t, guard.Cancel(ctx, exec))
	client.AssertNotCalled(
		t, "DeleteWithFillGeneration", mock.Anything, mock.Anything,
		mock.Anything,
	)
	mock.AssertExpectationsForObjects(t, storage, exec, factory, client)
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
				request: &protos.ResizeDiskRequest{UseDiskOperationGuard: true, Disk: &types.Disk{ZoneId: "cell", DiskId: "leader"}},
				state:   &protos.ResizeDiskTaskState{},
			}
			guard := guardDiskOperation(task, storage, resources.DiskOperationResize).(*diskOperationTask)
			locked := &preparationLockedExecutionContext{ExecutionContext: exec, t: t}
			if method == "FinishWithPreparation" {
				guarded := &diskOperationExecutionContext{ExecutionContext: locked, task: guard}
				require.NoError(t, guarded.FinishWithPreparation(ctx, nil))
			} else {
				require.NoError(t, guard.release(ctx, locked))
			}

			mock.AssertExpectationsForObjects(t, storage, exec)
		})
	}
}

func TestDiskOperationGuardPreservesReservationError(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	exec := tasks_mocks.NewExecutionContextMock()
	factory := nbs_mocks.NewFactoryMock()
	task := &resizeDiskTask{
		nbsFactory: factory,
		request: &protos.ResizeDiskRequest{
			UseDiskOperationGuard: true,
			Disk:                  &types.Disk{DiskId: "leader"},
		},
		state: &protos.ResizeDiskTaskState{},
	}

	failure := errors.NewNonRetriableErrorf("unable to find disk %q", "leader")
	exec.On("GetTaskID").Return("resize-task")
	storage.On(
		"ReserveDiskOperation", ctx, mock.Anything,
		resources.DiskOperation{
			DiskID: "leader",
			TaskID: "resize-task",
			Kind:   resources.DiskOperationResize,
		},
	).Return(nil, failure).Once()
	expectDiskOperationCheckpoint(t, ctx, exec, failure)
	guard := guardDiskOperation(task, storage, resources.DiskOperationResize)
	require.Equal(t, failure, guard.Run(ctx, exec))
	factory.AssertNotCalled(t, "GetClient", mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, storage, exec, factory)
}

func TestDiskOperationGuardRetriesFailedFinishPreparation(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	exec := tasks_mocks.NewExecutionContextMock()
	task := &resizeDiskTask{
		request: &protos.ResizeDiskRequest{
			UseDiskOperationGuard: true,
			Disk:                  &types.Disk{DiskId: "leader"},
		},
		state: &protos.ResizeDiskTaskState{},
	}

	guard := guardDiskOperation(
		task, storage, resources.DiskOperationResize,
	).(*diskOperationTask)
	guarded := &diskOperationExecutionContext{
		ExecutionContext: exec,
		task:             guard,
	}

	failure := errors.NewRetriableErrorf("preparation failed")
	exec.On("GetTaskID").Return("resize-task").Twice()
	exec.On("FinishWithPreparation", ctx, mock.Anything).Run(
		func(args mock.Arguments) {
			prepare := args.Get(1).(func(
				context.Context, *persistence.Transaction,
			) error)
			require.ErrorIs(t, prepare(ctx, nil), failure)
		},
	).Return(failure).Once()
	err := guarded.FinishWithPreparation(
		ctx,
		func(context.Context, *persistence.Transaction) error {
			return failure
		},
	)
	require.ErrorIs(t, err, failure)

	storage.AssertNotCalled(
		t, "ReleaseDiskOperation", mock.Anything, mock.Anything,
		mock.Anything, mock.Anything,
	)

	prepared := false
	exec.On("FinishWithPreparation", ctx, mock.Anything).Run(
		func(args mock.Arguments) {
			prepare := args.Get(1).(func(
				context.Context, *persistence.Transaction,
			) error)
			require.NoError(t, prepare(ctx, nil))
		},
	).Return(nil).Once()
	storage.On(
		"ReleaseDiskOperation", ctx, mock.Anything, "leader", "resize-task",
	).Run(func(mock.Arguments) {
		require.True(t, prepared)
	}).Return(nil).Once()
	err = guarded.FinishWithPreparation(
		ctx,
		func(context.Context, *persistence.Transaction) error {
			prepared = true
			return nil
		},
	)
	require.NoError(t, err)

	mock.AssertExpectationsForObjects(t, storage, exec)
}
