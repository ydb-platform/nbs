package disks

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

// The guard adds an idempotent shared reservation to the task lifecycle.
// Run and Cancel may be repeated, including after a successful release.
// The underlying task owns its state, cleanup, and error policy.
type diskOperationTask struct {
	tasks.Task
	storage resources.Storage
	kind    string
	request func() diskOperationRequest
}

type diskOperationRequest interface {
	GetDisk() *types.Disk
	GetUseDiskOperationGuard() bool
}

func guardDiskOperation(
	task tasks.Task,
	storage resources.Storage,
	kind string,
) tasks.Task {

	guard := &diskOperationTask{Task: task, storage: storage, kind: kind}
	switch task := task.(type) {
	case *resizeDiskTask:
		guard.request = func() diskOperationRequest { return task.request }
	case *alterDiskTask:
		guard.request = func() diskOperationRequest { return task.request }
	case *migrateDiskTask:
		guard.request = func() diskOperationRequest { return task.request }
	default:
		panic("unsupported guarded disk task")
	}

	return guard
}

func (t *diskOperationTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if !t.request().GetUseDiskOperationGuard() {
		return t.Task.Run(ctx, execCtx)
	}

	// Capture identifiers before preparation takes the execution-context mutex.
	operation := resources.DiskOperation{
		DiskID: t.request().GetDisk().GetDiskId(),
		TaskID: execCtx.GetTaskID(),
		Kind:   t.kind,
	}

	// Fence the reservation with the current task execution generation.
	err := execCtx.SaveStateWithPreparation(
		ctx,
		func(ctx context.Context, tx *persistence.Transaction) error {
			_, err := t.storage.ReserveDiskOperation(ctx, tx, operation)
			return err
		},
	)
	if err != nil {
		return err
	}

	diskOperationExecCtx := &diskOperationExecutionContext{
		ExecutionContext: execCtx,
		task:             t,
	}

	err = t.Task.Run(ctx, diskOperationExecCtx)
	if err == nil {
		return diskOperationExecCtx.FinishWithPreparation(ctx, nil)
	}

	// This error finishes the task without Cancel, so release here.
	if ctx.Err() == nil &&
		errors.Is(err, errors.NewEmptyNonCancellableError()) {
		if releaseErr := t.release(ctx, execCtx); releaseErr != nil {
			return releaseErr
		}
	}

	return err
}

func (t *diskOperationTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if !t.request().GetUseDiskOperationGuard() {
		return t.Task.Cancel(ctx, execCtx)
	}

	err := t.Task.Cancel(ctx, execCtx)
	if ctx.Err() != nil {
		return err
	}

	// Follow the runner's policy for when cancellation has ended.
	if err != nil &&
		!errors.Is(err, errors.NewEmptyNonRetriableError()) &&
		!errors.Is(err, errors.NewEmptyNonCancellableError()) {
		return err
	}

	if releaseErr := t.release(ctx, execCtx); releaseErr != nil {
		return releaseErr
	}

	return err
}

func (t *diskOperationTask) release(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	diskID := t.request().GetDisk().GetDiskId()
	taskID := execCtx.GetTaskID()
	return execCtx.SaveStateWithPreparation(
		ctx,
		func(ctx context.Context, tx *persistence.Transaction) error {
			return t.storage.ReleaseDiskOperation(ctx, tx, diskID, taskID)
		},
	)
}

type diskOperationExecutionContext struct {
	tasks.ExecutionContext
	task *diskOperationTask
}

// Run the underlying task's preparation (e.g. DiskRelocated for migration)
// and release the reservation in the same transaction that finishes the task.
func (c *diskOperationExecutionContext) FinishWithPreparation(
	ctx context.Context,
	preparation func(context.Context, *persistence.Transaction) error,
) error {

	diskID := c.task.request().GetDisk().GetDiskId()
	taskID := c.GetTaskID()
	return c.ExecutionContext.FinishWithPreparation(
		ctx,
		func(ctx context.Context, tx *persistence.Transaction) error {
			if preparation != nil {
				if err := preparation(ctx, tx); err != nil {
					return err
				}
			}

			return c.task.storage.ReleaseDiskOperation(ctx, tx, diskID, taskID)
		},
	)
}
