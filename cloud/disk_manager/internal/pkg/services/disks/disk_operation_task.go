package disks

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

// The guard owns the logical resource across worker restarts and dependency
// waits. The embedded task keeps its existing NBS algorithm and state.
type diskOperationTask struct {
	tasks.Task
	storage       resources.Storage
	kind          string
	disk          func() *types.Disk
	state         func() *protos.DiskOperationTaskState
	hasSavedState bool
}

func (t *diskOperationTask) ensureState(state **protos.DiskOperationTaskState) *protos.DiskOperationTaskState {
	if *state == nil {
		// An older saved task may already have effects without an Operation field.
		*state = &protos.DiskOperationTaskState{CleanupRequired: t.hasSavedState}
	}
	return *state
}

func guardDiskOperation(task tasks.Task, storage resources.Storage, kind string) tasks.Task {
	guard := &diskOperationTask{Task: task, storage: storage, kind: kind}
	switch task := task.(type) {
	case *resizeDiskTask:
		guard.disk = func() *types.Disk { return task.request.Disk }
		guard.state = func() *protos.DiskOperationTaskState { return guard.ensureState(&task.state.Operation) }
	case *alterDiskTask:
		guard.disk = func() *types.Disk { return task.request.Disk }
		guard.state = func() *protos.DiskOperationTaskState { return guard.ensureState(&task.state.Operation) }
	case *migrateDiskTask:
		guard.disk = func() *types.Disk { return task.request.Disk }
		guard.state = func() *protos.DiskOperationTaskState { return guard.ensureState(&task.state.Operation) }
	default:
		panic("unsupported guarded disk task")
	}
	return guard
}

func (t *diskOperationTask) Load(request, state []byte) error {
	if err := t.Task.Load(request, state); err != nil {
		return err
	}
	t.hasSavedState = len(state) != 0
	t.state()
	return nil
}

func (t *diskOperationTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	state := t.state()
	if state.ErrorMessage != "" {
		return t.releaseFailure(ctx, execCtx)
	}
	if state.Released {
		return nil
	}
	disk := t.disk()
	_, err := acquireDiskOperation(ctx, execCtx, t.storage, resources.DiskOperation{
		DiskID: disk.GetDiskId(), CellID: disk.GetZoneId(), Kind: t.kind,
	}, &state.Acquired, &state.CreateTaskId)
	if err != nil {
		if errors.Is(err, errors.NewEmptyNonRetriableError()) {
			if state.CleanupRequired {
				return err
			}
			return errors.NewNonCancellableError(err)
		}
		return err
	}
	guarded := &diskOperationExecutionContext{ExecutionContext: execCtx, task: t}
	err = t.Task.Run(ctx, guarded)
	if err == nil {
		if state.Released {
			return nil
		}
		return guarded.FinishWithPreparation(ctx, nil)
	}
	if ctx.Err() != nil {
		return err
	}
	if errors.Is(err, errors.NewEmptyNonCancellableError()) {
		state.ErrorMessage = errors.ErrorForTracing(err)
		return t.releaseFailure(ctx, execCtx)
	}
	return err
}

func (t *diskOperationTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	state := t.state()
	if state.Released {
		return nil
	}
	if state.ErrorMessage == "" && (state.Acquired || state.CleanupRequired) {
		if err := t.Task.Cancel(ctx, execCtx); err != nil {
			if ctx.Err() != nil || errors.Is(err, errors.NewWrongGenerationError()) || errors.Is(err, errors.NewInterruptExecutionError()) {
				return err
			}
			// Cancel runner treats terminal errors as completed cancellation.
			return errors.NewRetriableErrorWithIgnoreRetryLimitf("disk operation cleanup failed: %v", err)
		}
	}
	state.ErrorMessage = "disk operation cancelled"
	return t.saveRelease(ctx, execCtx)
}

func (t *diskOperationTask) releaseFailure(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if !t.state().Released {
		if err := t.saveRelease(ctx, execCtx); err != nil {
			return err
		}
	}
	return errors.NewNonCancellableErrorf("%s", t.state().ErrorMessage)
}

func (t *diskOperationTask) saveRelease(ctx context.Context, execCtx tasks.ExecutionContext) error {
	taskID := execCtx.GetTaskID()
	state := t.state()
	acquired, released := state.Acquired, state.Released
	state.Acquired, state.Released = false, true
	err := execCtx.SaveStateWithPreparation(ctx, func(ctx context.Context, tx *persistence.Transaction) error {
		return t.storage.ReleaseDiskOperation(ctx, tx, t.disk().GetDiskId(), taskID)
	})
	if err != nil {
		state.Acquired, state.Released = acquired, released
		if ctx.Err() != nil || errors.Is(err, errors.NewWrongGenerationError()) {
			return err
		}
		return errors.NewRetriableErrorWithIgnoreRetryLimitf("disk reservation release failed: %v", err)
	}
	return nil
}

type diskOperationExecutionContext struct {
	tasks.ExecutionContext
	task *diskOperationTask
}

func (c *diskOperationExecutionContext) FinishWithPreparation(ctx context.Context, preparation func(context.Context, *persistence.Transaction) error) error {
	// Preparation runs with the execution-context mutex held.
	taskID := c.GetTaskID()
	state := c.task.state()
	acquired, released := state.Acquired, state.Released
	state.Acquired, state.Released = false, true
	err := c.ExecutionContext.FinishWithPreparation(ctx, func(ctx context.Context, tx *persistence.Transaction) error {
		if preparation != nil {
			if err := preparation(ctx, tx); err != nil {
				return err
			}
		}
		return c.task.storage.ReleaseDiskOperation(ctx, tx, c.task.disk().GetDiskId(), taskID)
	})
	if err != nil {
		state.Acquired, state.Released = acquired, released
	}
	return err
}

func saveDiskOperationState(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if err := execCtx.SaveState(ctx); err != nil {
		if ctx.Err() != nil || errors.Is(err, errors.NewWrongGenerationError()) {
			return err
		}
		return errors.NewRetriableErrorWithIgnoreRetryLimitf("disk operation state persistence failed: %v", err)
	}
	return nil
}

func isTerminalDiskOperationError(err error) bool {
	return errors.Is(err, errors.NewEmptyNonRetriableError()) ||
		errors.Is(err, errors.NewEmptyNonCancellableError())
}

func acquireDiskOperation(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	storage resources.Storage,
	operation resources.DiskOperation,
	acquired *bool,
	createTaskID *string,
) (*resources.DiskMeta, error) {
	if *createTaskID == "" {
		meta, err := storage.GetDiskMeta(ctx, operation.DiskID)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, errors.NewWrongGenerationError()) || errors.Is(err, errors.NewInterruptExecutionError()) || isTerminalDiskOperationError(err) {
				return nil, err
			}
			return nil, errors.NewRetriableErrorWithIgnoreRetryLimitf("disk metadata lookup failed: %v", err)
		}
		if meta == nil || meta.CreateTaskID == "" {
			return nil, errors.NewNonRetriableErrorf("disk %q has no creation task", operation.DiskID)
		}
		*createTaskID = meta.CreateTaskID
	}
	operation.TaskID = execCtx.GetTaskID()
	operation.CreateTaskID = *createTaskID
	operation.Resume = *acquired
	previous := *acquired
	*acquired = true
	var meta *resources.DiskMeta
	var reservationErr error
	err := execCtx.SaveStateWithPreparation(ctx, func(ctx context.Context, tx *persistence.Transaction) error {
		meta, reservationErr = storage.ReserveDiskOperation(ctx, tx, operation)
		return reservationErr
	})
	if err != nil {
		*acquired = previous
		if ctx.Err() != nil || errors.Is(err, errors.NewWrongGenerationError()) || errors.Is(err, errors.NewInterruptExecutionError()) {
			return nil, err
		}
		if reservationErr != nil && isTerminalDiskOperationError(reservationErr) {
			return nil, reservationErr
		}
		return nil, errors.NewRetriableErrorWithIgnoreRetryLimitf("disk operation reservation failed: %v", err)
	}
	return meta, nil
}
