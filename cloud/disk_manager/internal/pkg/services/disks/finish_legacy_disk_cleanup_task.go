package disks

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

// An older executor can finish a legacy parent without clearing its reservation.
// This independent observer releases that reservation only after the parent ends.
type finishLegacyDiskCleanupTask struct {
	deleteDiskTask
	legacyTaskID string
	cleanupID    string
}

func (t *finishLegacyDiskCleanupTask) Load(request, state []byte) error {
	var req protos.FinishLegacyDiskCleanupRequest
	if err := proto.Unmarshal(request, &req); err != nil {
		return err
	}
	t.request = &protos.DeleteDiskRequest{Disk: &types.Disk{DiskId: req.DiskId}}
	t.legacyTaskID = req.LegacyTaskId
	t.cleanupID = req.CleanupId
	t.state = &protos.DeleteDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *finishLegacyDiskCleanupTask) finish(ctx context.Context, execCtx tasks.ExecutionContext) error {
	pending, err := t.storage.IsDiskCleanupPending(ctx, t.request.Disk.DiskId, t.cleanupID)
	if err != nil {
		return err
	}
	if !pending {
		return nil
	}
	// Zero timeout checks terminal status once. A still-running/cancelling parent
	// interrupts this observer for a later retry; it never blocks the user operation.
	err = t.scheduler.WaitTaskEndedWithTimeout(ctx, t.legacyTaskID, 0)
	if errors.Is(err, errors.NewInterruptExecutionError()) {
		// No dependency is registered by this terminal-state check. Use normal
		// retry backoff instead of an immediate interrupt/reschedule loop.
		return errors.NewRetriableErrorWithIgnoreRetryLimitf("legacy disk deletion task %q has not ended", t.legacyTaskID)
	}

	// The task collector removes ended tasks only; an absent recorded parent ended.
	if err != nil && !errors.Is(err, errors.NewEmptyNotFoundError()) {
		return err
	}
	return execCtx.FinishWithPreparation(ctx, func(ctx context.Context, tx *persistence.Transaction) error {
		return t.storage.CompleteDiskCleanup(ctx, tx, t.request.Disk.DiskId, t.cleanupID)
	})
}

func (t *finishLegacyDiskCleanupTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if err := t.finish(ctx, execCtx); err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}
	return nil
}

func (t *finishLegacyDiskCleanupTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return t.finish(ctx, execCtx)
}
