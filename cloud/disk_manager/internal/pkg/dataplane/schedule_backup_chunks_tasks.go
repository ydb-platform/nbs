package dataplane

import (
	"context"
	"fmt"
	"math"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

// More ranges than tasks, so that fast nodes take the ranges a slow one has
// not reached.
const backupChunkRangesPerTask = 4

////////////////////////////////////////////////////////////////////////////////

// Splits the shard_id space of backup_chunk_queue into ranges and keeps at
// most maxTasks dataplane.BackupChunks tasks running, one per non-empty range.
// A range has at most one task at a time, so no chunk is copied twice. A task
// ends when its range is empty; an empty queue runs none.
type scheduleBackupChunksTasks struct {
	scheduler tasks.Scheduler
	storage   storage.Storage
	maxTasks  int
	state     *protos.ScheduleBackupChunksTasksState
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

	err := t.schedule(ctx, execCtx)
	if errors.Is(err, errors.NewEmptyRetriableError()) {
		// The dispatcher lives forever: transient errors must not add up to
		// a failure, as a new dispatcher would not know the running tasks.
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	if err != nil {
		return err
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

func (t *scheduleBackupChunksTasks) schedule(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	running, err := t.dropEndedTasks(ctx)
	if err != nil {
		return err
	}

	rangeCount := uint32(t.maxTasks * backupChunkRangesPerTask)
	if rangeCount != t.state.RangeCount {
		// Ranges of the old split overlap the new ones: wait for their tasks.
		if running != 0 || rangeCount == 0 {
			return nil
		}

		t.state.RangeCount = rangeCount
		t.state.RangeTaskIds = make([]string, rangeCount)
		t.state.RangeScheduledCounts = make([]uint64, rangeCount)
		t.state.NextRange = 0
	}

	if running >= t.maxTasks {
		return nil
	}

	queued, err := t.hasQueuedChunks(ctx, 0, math.MaxUint64)
	if err != nil || !queued {
		return err
	}

	start := t.state.NextRange
	for i := uint32(0); i < rangeCount && running < t.maxTasks; i++ {
		index := (start + i) % rangeCount
		if len(t.state.RangeTaskIds[index]) != 0 {
			continue
		}

		firstShardID, lastShardID := backupChunkShardRange(index, rangeCount)
		queued, err := t.hasQueuedChunks(ctx, firstShardID, lastShardID)
		if err != nil {
			return err
		}

		if !queued {
			continue
		}

		err = t.scheduleTask(ctx, execCtx, index, firstShardID, lastShardID)
		if err != nil {
			return err
		}

		running++
		t.state.NextRange = (index + 1) % rangeCount
	}

	return nil
}

// Clears the ranges whose tasks ended; returns how many tasks still run. A
// task missing from the task storage ended long ago and was cleared.
func (t *scheduleBackupChunksTasks) dropEndedTasks(
	ctx context.Context,
) (int, error) {

	running := 0
	for index, taskID := range t.state.RangeTaskIds {
		if len(taskID) == 0 {
			continue
		}

		op, err := t.scheduler.GetOperation(ctx, taskID)
		if errors.Is(err, errors.NewEmptyNotFoundError()) {
			t.state.RangeTaskIds[index] = ""
			continue
		}

		if err != nil {
			return 0, err
		}

		if op.Done {
			t.state.RangeTaskIds[index] = ""
			continue
		}

		running++
	}

	return running, nil
}

func (t *scheduleBackupChunksTasks) hasQueuedChunks(
	ctx context.Context,
	firstShardID uint64,
	lastShardID uint64,
) (bool, error) {

	entries, err := t.storage.GetQueuedChunksToBackup(
		ctx,
		firstShardID,
		lastShardID,
		nil, // after
		1,   // limit
	)
	return len(entries) != 0, err
}

// The key names the range and counts the range's tasks, so a retry after a
// failed SaveState gets the same task of the same range back instead of a
// second one.
func (t *scheduleBackupChunksTasks) scheduleTask(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	index uint32,
	firstShardID uint64,
	lastShardID uint64,
) error {

	idempotencyKey := fmt.Sprintf(
		"backup_chunks_%v_%v_%v_%v",
		execCtx.GetTaskID(),
		t.state.RangeCount,
		index,
		t.state.RangeScheduledCounts[index],
	)

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, idempotencyKey),
		"dataplane.BackupChunks",
		"",
		&protos.BackupChunksRequest{
			FirstShardId: firstShardID,
			LastShardId:  lastShardID,
		},
	)
	if err != nil {
		return err
	}

	t.state.RangeTaskIds[index] = taskID
	t.state.RangeScheduledCounts[index]++
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// Returns the inclusive bounds of range index out of count equal ranges of
// the uint64 shard_id space.
func backupChunkShardRange(index uint32, count uint32) (uint64, uint64) {
	if count == 1 {
		return 0, math.MaxUint64
	}

	step := math.MaxUint64/uint64(count) + 1
	first := uint64(index) * step
	if index == count-1 {
		return first, math.MaxUint64
	}

	return first, first + step - 1
}
