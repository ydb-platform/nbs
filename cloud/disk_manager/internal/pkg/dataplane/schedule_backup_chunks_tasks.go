package dataplane

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

// Keeps as many dataplane.BackupChunks tasks running as the queue needs: one
// per batch, up to tasksLimit. Workers end on their own when the queue is
// empty, so an idle cluster runs none of them.
type scheduleBackupChunksTasks struct {
	scheduler  tasks.Scheduler
	storage    storage.Storage
	tasksLimit int
	batchSize  int
	state      *protos.ScheduleBackupChunksTasksState
}

func (t *scheduleBackupChunksTasks) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *scheduleBackupChunksTasks) Load(_, state []byte) error {
	t.state = &protos.ScheduleBackupChunksTasksState{}
	return proto.Unmarshal(state, t.state)
}

func (t *scheduleBackupChunksTasks) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// The scan stops where another worker would not change anything.
	queued, err := t.storage.CountQueuedBackupChunks(
		ctx,
		t.tasksLimit*t.batchSize,
	)
	if err != nil {
		return err
	}

	want := (queued + t.batchSize - 1) / t.batchSize
	want = min(want, t.tasksLimit)

	err = t.dropEndedWorkers(ctx)
	if err != nil {
		return err
	}

	for len(t.state.WorkerTaskIds) < want {
		err = t.scheduleWorker(ctx, execCtx)
		if err != nil {
			return err
		}
	}

	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	return errors.NewInterruptExecutionError()
}

func (t *scheduleBackupChunksTasks) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *scheduleBackupChunksTasks) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *scheduleBackupChunksTasks) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

// Keeps the copy tasks that have not ended yet. A task missing from the task
// storage ended long ago and was cleared.
func (t *scheduleBackupChunksTasks) dropEndedWorkers(
	ctx context.Context,
) error {

	var live []string
	for _, id := range t.state.WorkerTaskIds {
		op, err := t.scheduler.GetOperation(ctx, id)
		if errors.Is(err, errors.NewEmptyNotFoundError()) {
			continue
		}

		if err != nil {
			return err
		}

		if !op.Done {
			live = append(live, id)
		}
	}

	t.state.WorkerTaskIds = live
	return nil
}

// The key is the dispatcher's task ID and a counter persisted in the state,
// so a retry after a failed SaveState gets the same worker back instead of a
// second one.
func (t *scheduleBackupChunksTasks) scheduleWorker(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	idempotencyKey := fmt.Sprintf(
		"backup_chunks_%v_%v",
		execCtx.GetTaskID(),
		t.state.ScheduledCount,
	)

	id, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, idempotencyKey),
		"dataplane.BackupChunks",
		"",
		&empty.Empty{},
	)
	if err != nil {
		return err
	}

	t.state.WorkerTaskIds = append(t.state.WorkerTaskIds, id)
	t.state.ScheduledCount++
	return nil
}
