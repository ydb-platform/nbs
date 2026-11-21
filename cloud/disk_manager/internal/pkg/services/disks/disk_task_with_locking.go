package disks

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

// The decorator holds an exclusive disk lock for the task lifecycle.
// Run and Cancel may be repeated, including after a successful release.
// The underlying task owns its state, cleanup, and error policy.
//
// ForceFinishTask bypasses this lifecycle and does not release the disk lock.
// Manual release requires verifying that the operation and its cleanup have
// stopped and reconciling any effects already applied to NBS.
type diskTaskWithLocking struct {
	lockableDiskTask
	storage resources.Storage
}

type diskOperationRequest interface {
	GetDisk() *types.Disk
	GetPreventConcurrentDiskModifications() bool
}

type lockableDiskTask interface {
	tasks.Task

	// Read the current request, including after Load replaces its pointer.
	getDiskOperationRequest() diskOperationRequest
}

func newDiskTaskWithLocking(
	task lockableDiskTask,
	storage resources.Storage,
) tasks.Task {

	return &diskTaskWithLocking{
		lockableDiskTask: task,
		storage:          storage,
	}
}

// Lock transactions run outside the underlying task's error handling.
// A resumed migration may already have deleted its source. Lock acquisition
// runs before its Run can apply the unlimited retry policy for Finishing.
// Unlock failures must also keep retrying after NonCancellableError.
// Ignore the limit for retriable transaction errors; other errors retain
// their normal handling in the task runner.
func withUnlimitedLockRetries(err error) error {
	if errors.Is(err, errors.NewEmptyRetriableError()) {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return err
}

func (t *diskTaskWithLocking) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	request := t.getDiskOperationRequest()

	// Preserve the original behavior for tasks created without locking.
	// The saved mode is independent of the worker's current config.
	if !request.GetPreventConcurrentDiskModifications() {
		return t.lockableDiskTask.Run(ctx, execCtx)
	}

	// Capture identifiers before preparation takes the execution-context mutex.
	input := resources.LockDiskInput{
		DiskID: request.GetDisk().GetDiskId(),
		TaskID: execCtx.GetTaskID(),
	}

	// Fence the reservation with the current task execution generation.
	err := execCtx.SaveStateWithPreparation(
		ctx,
		func(ctx context.Context, tx *persistence.Transaction) error {
			_, err := t.storage.LockDisk(ctx, tx, input)
			return err
		},
	)
	if err != nil {
		return withUnlimitedLockRetries(err)
	}

	diskOperationExecCtx := &diskOperationExecutionContext{
		ExecutionContext: execCtx,
		task:             t,
	}

	err = t.lockableDiskTask.Run(ctx, diskOperationExecCtx)
	if err == nil {
		return diskOperationExecCtx.FinishWithPreparation(ctx, nil)
	}

	// NonCancellableError ends the task without Cancel, so unlock here.
	// The runner ignores the result if the worker context is cancelled.
	// Skip unlocking when this execution is already interrupted.
	if ctx.Err() == nil &&
		errors.Is(err, errors.NewEmptyNonCancellableError()) {

		if releaseErr := t.release(ctx, execCtx); releaseErr != nil {
			return releaseErr
		}
	}

	return err
}

func (t *diskTaskWithLocking) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	request := t.getDiskOperationRequest()

	// Preserve the original behavior for tasks created without locking.
	// The saved mode is independent of the worker's current config.
	if !request.GetPreventConcurrentDiskModifications() {
		return t.lockableDiskTask.Cancel(ctx, execCtx)
	}

	err := t.lockableDiskTask.Cancel(ctx, execCtx)
	// A cancelled worker context leaves task cancellation unfinished.
	// Skip unlocking so the next execution can resume cleanup.
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

////////////////////////////////////////////////////////////////////////////////

func (t *diskTaskWithLocking) release(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	diskID := t.getDiskOperationRequest().GetDisk().GetDiskId()
	taskID := execCtx.GetTaskID()
	err := execCtx.SaveStateWithPreparation(
		ctx,
		func(ctx context.Context, tx *persistence.Transaction) error {
			return t.storage.UnlockDisk(ctx, tx, diskID, taskID)
		},
	)
	return withUnlimitedLockRetries(err)
}

////////////////////////////////////////////////////////////////////////////////

type diskOperationExecutionContext struct {
	tasks.ExecutionContext
	task *diskTaskWithLocking
}

// Run the underlying task's preparation (e.g. DiskRelocated for migration)
// and release the reservation in the same transaction that finishes the task.
func (c *diskOperationExecutionContext) FinishWithPreparation(
	ctx context.Context,
	preparation func(context.Context, *persistence.Transaction) error,
) error {

	diskID := c.task.getDiskOperationRequest().GetDisk().GetDiskId()
	taskID := c.GetTaskID()
	err := c.ExecutionContext.FinishWithPreparation(
		ctx,
		func(ctx context.Context, tx *persistence.Transaction) error {
			if preparation != nil {
				if err := preparation(ctx, tx); err != nil {
					return err
				}
			}

			return c.task.storage.UnlockDisk(ctx, tx, diskID, taskID)
		},
	)
	return withUnlimitedLockRetries(err)
}
