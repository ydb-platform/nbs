package disks

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	grpc_metadata "google.golang.org/grpc/metadata"
)

const registerLegacyDiskCleanupTaskType = "disks.RegisterLegacyDiskCleanup"
const legacyDiskCleanupPageSize = uint64(100)

// This background-capable version must be on every executor before activation.
// Then this paginated scan covers all untracked old attempts: any new external
// deletion already registers itself before calling incremental or NBS.
// The scan waits for database operations only, never for the old tasks or cells.
type registerLegacyDiskCleanupTask struct {
	storage   resources.Storage
	scheduler tasks.Scheduler
	state     *protos.RegisterLegacyDiskCleanupTaskState
}

func (t *registerLegacyDiskCleanupTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *registerLegacyDiskCleanupTask) Load(request, state []byte) error {
	t.state = &protos.RegisterLegacyDiskCleanupTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *registerLegacyDiskCleanupTask) register(ctx context.Context, execCtx tasks.ExecutionContext) error {
	page, err := t.scheduler.ListUnfinishedTasks(ctx, "disks.DeleteDisk",
		t.state.AfterTaskId, legacyDiskCleanupPageSize)
	if err != nil {
		return err
	}
	if len(page) == 0 {
		return nil
	}
	for _, parent := range page {
		var request protos.DeleteDiskRequest
		if err := proto.Unmarshal(parent.Request, &request); err != nil {
			return err
		}
		var state protos.DeleteDiskTaskState
		if err := proto.Unmarshal(parent.State, &state); err != nil {
			return err
		}
		if request.DeleteIfNonExistent && !request.BackgroundCleanup && len(state.CleanupCellIds) == 0 &&
			request.Disk != nil && request.Disk.DiskId != "" && request.Disk.ZoneId != "" {
			cleanupID := legacyDiskCleanupPrefix + parent.ID
			prepared, err := t.storage.PrepareDiskCleanup(ctx, request.Disk.DiskId, []string{cleanupID}, true /* legacy */)
			if err != nil {
				return err
			}
			if prepared {
				// The parent and an earlier observer may have ended after this
				// page was read. Do not re-pin an ended parent and then reuse its
				// already-finished observer: that would retain the disk forever.
				endedErr := t.scheduler.WaitTaskEndedWithTimeout(ctx, parent.ID, 0)
				if endedErr == nil || errors.Is(endedErr, errors.NewEmptyNotFoundError()) {
					err = execCtx.SaveStateWithPreparation(ctx, func(ctx context.Context, tx *persistence.Transaction) error {
						return t.storage.CompleteDiskCleanup(ctx, tx, request.Disk.DiskId, cleanupID)
					})
				} else if errors.Is(endedErr, errors.NewInterruptExecutionError()) {
					err = scheduleLegacyDiskCleanupObserver(ctx, t.scheduler, request.Disk.DiskId, parent.ID, cleanupID)
				} else {
					err = endedErr
				}
				if err != nil {
					return err
				}
			}
		}
	}
	// Persist only after every reservation and observer in the page is durable.
	// A failed page is safe to repeat because both use stable IDs.
	t.state.AfterTaskId = page[len(page)-1].ID
	if err := execCtx.SaveState(ctx); err != nil {
		return err
	}
	return errors.NewInterruptExecutionError()
}

func (t *registerLegacyDiskCleanupTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if err := t.register(ctx, execCtx); err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}
	return nil
}

func (t *registerLegacyDiskCleanupTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return t.Run(ctx, execCtx)
}

func (t *registerLegacyDiskCleanupTask) GetMetadata(ctx context.Context) (proto.Message, error) {
	return &empty.Empty{}, nil
}

func (t *registerLegacyDiskCleanupTask) GetResponse() proto.Message { return &empty.Empty{} }

func scheduleLegacyDiskCleanupObserver(ctx context.Context, scheduler tasks.Scheduler, diskID, parentID, cleanupID string) error {
	_, err := scheduler.ScheduleNonCancellableTask(
		diskCleanupTaskContext(ctx, cleanupID+"_finish"), "disks.FinishLegacyDiskCleanup", "", "",
		&protos.FinishLegacyDiskCleanupRequest{DiskId: diskID, LegacyTaskId: parentID, CleanupId: cleanupID},
	)
	return err
}

func (t *deleteDiskTask) registerLegacyCleanup(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if t.state.LegacyCleanupRegistered {
		return nil
	}
	if t.state.LegacyCleanupRegistrationTaskId == "" {
		// All accounts share the same migration. Replace, rather than append, the
		// incoming key so a caller's idempotency key cannot create another scan.
		migrationCtx := headers.SetAccountID(ctx, "")
		migrationCtx = diskCleanupTaskContext(migrationCtx, registerLegacyDiskCleanupTaskType)
		id, err := t.scheduler.ScheduleNonCancellableTask(migrationCtx, registerLegacyDiskCleanupTaskType, "", "", &empty.Empty{})
		if err != nil {
			return err
		}
		t.state.LegacyCleanupRegistrationTaskId = id
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
	}
	_, err := t.scheduler.WaitTask(ctx, execCtx, t.state.LegacyCleanupRegistrationTaskId)
	// Only successful completion can end this non-cancellable migration. If its
	// recorded task has already been collected, the registration pass finished.
	if err != nil && !errors.Is(err, errors.NewEmptyNotFoundError()) {
		return err
	}
	t.state.LegacyCleanupRegistered = true
	return execCtx.SaveState(ctx)
}

func diskCleanupTaskContext(ctx context.Context, key string) context.Context {
	metadata, _ := grpc_metadata.FromIncomingContext(ctx)
	metadata = metadata.Copy()
	metadata.Set("idempotency-key", key)
	return grpc_metadata.NewIncomingContext(ctx, metadata)
}
