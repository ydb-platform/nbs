package dataplane

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

// Splits the shard_id space of backup_chunk_queue into rangeCount ranges and
// keeps at most maxTasks dataplane.BackupChunks tasks running, one per
// non-empty range. A range has at most one task at a time, so no chunk is
// copied twice. A task ends when its range is empty; an empty queue runs none.
//
// Ranges with chunks are found by a skip scan: one read gives the first
// queued chunk at or after a range, the next read starts after the range it
// is in. Empty ranges cost nothing, so a run reads the queue about as many
// times as it schedules tasks, whatever the range count.
type scheduleBackupChunksTasks struct {
	scheduler  tasks.Scheduler
	storage    storage.Storage
	registry   metrics.Registry
	rangeCount uint32
	maxTasks   int
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

	runStart := time.Now()

	running, err := t.dropEndedTasks(ctx)
	if err != nil {
		return err
	}

	rangeCount := t.rangeCount
	if rangeCount < uint32(t.maxTasks) {
		logging.Warn(
			ctx,
			"backup chunk queue is split into %v parts, fewer than %v tasks: "+
				"at most %v tasks run",
			rangeCount,
			t.maxTasks,
			rangeCount,
		)
	}

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
		t.report(ctx, runStart, running, 0, 0)
		return nil
	}

	// Ranges are taken in order from NextRange, wrapping around, so that a
	// range that frees first is not always the same one.
	scheduled, reads := 0, 0
	start := t.state.NextRange
	for _, segment := range [][2]uint32{{start, rangeCount}, {0, start}} {
		part := segment[0]
		for part < segment[1] && running < t.maxTasks {
			// A range with a task is left to it.
			for part < segment[1] && len(t.state.RangeTaskIds[part]) != 0 {
				part++
			}

			if part >= segment[1] {
				break
			}

			firstShardID, _ := backupChunkShardRange(part, rangeCount)
			shardID, found, err := t.storage.GetFirstQueuedChunkShardID(
				ctx,
				firstShardID,
			)
			reads++
			if err != nil {
				return err
			}

			if !found {
				break
			}

			part = backupChunkShardIndex(shardID, rangeCount)
			if part >= segment[1] {
				break
			}

			if len(t.state.RangeTaskIds[part]) != 0 {
				part++
				continue
			}

			first, last := backupChunkShardRange(part, rangeCount)
			err = t.scheduleTask(ctx, execCtx, part, first, last)
			if err != nil {
				return err
			}

			running++
			scheduled++
			t.state.NextRange = (part + 1) % rangeCount
			part++
		}
	}

	t.report(ctx, runStart, running, scheduled, reads)
	return nil
}

func (t *scheduleBackupChunksTasks) report(
	ctx context.Context,
	runStart time.Time,
	running int,
	scheduled int,
	reads int,
) {

	duration := time.Since(runStart)
	t.registry.Timer("backup/dispatcherRunTime").RecordDuration(duration)
	t.registry.Gauge("backup/dispatcherRunningTasks").Set(float64(running))
	t.registry.Counter("backup/dispatcherScheduledTasks").Add(int64(scheduled))
	t.registry.Counter("backup/dispatcherQueueReads").Add(int64(reads))

	logging.Info(
		ctx,
		"backup chunks dispatcher: %v tasks running, %v scheduled, "+
			"%v queue reads, %v",
		running,
		scheduled,
		reads,
		duration,
	)
}

// Clears the ranges whose tasks ended; returns how many tasks still run.
func (t *scheduleBackupChunksTasks) dropEndedTasks(
	ctx context.Context,
) (int, error) {

	var taskIDs []string
	for _, taskID := range t.state.RangeTaskIds {
		if len(taskID) != 0 {
			taskIDs = append(taskIDs, taskID)
		}
	}

	if len(taskIDs) == 0 {
		return 0, nil
	}

	ended, err := t.scheduler.GetEndedTasks(ctx, taskIDs)
	if err != nil {
		return 0, err
	}

	endedTaskIDs := make(map[string]struct{}, len(ended))
	for _, taskID := range ended {
		endedTaskIDs[taskID] = struct{}{}
	}

	for index, taskID := range t.state.RangeTaskIds {
		if _, ok := endedTaskIDs[taskID]; ok {
			t.state.RangeTaskIds[index] = ""
		}
	}

	return len(taskIDs) - len(ended), nil
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

// Returns the index of the range of count that shardID falls into; the
// inverse of backupChunkShardRange.
func backupChunkShardIndex(shardID uint64, count uint32) uint32 {
	if count == 1 {
		return 0
	}

	step := math.MaxUint64/uint64(count) + 1
	return uint32(shardID / step)
}
