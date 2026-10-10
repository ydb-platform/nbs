package dataplane

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/operation"
)

////////////////////////////////////////////////////////////////////////////////

// With two tasks the queue is split into eight ranges.
const testBackupChunkRangeCount = 2 * backupChunkRangesPerTask

type scheduleBackupChunksTasksTest struct {
	ctx       context.Context
	storage   *storage_mocks.StorageMock
	scheduler *tasks_mocks.SchedulerMock
	execCtx   *tasks_mocks.ExecutionContextMock
	task      *scheduleBackupChunksTasks
}

func newScheduleBackupChunksTasksTest() scheduleBackupChunksTasksTest {
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("dispatcher")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	storage := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()

	return scheduleBackupChunksTasksTest{
		ctx: logging.SetLogger(
			context.Background(),
			logging.NewStderrLogger(logging.DebugLevel),
		),
		storage:   storage,
		scheduler: scheduler,
		execCtx:   execCtx,
		task: &scheduleBackupChunksTasks{
			scheduler: scheduler,
			storage:   storage,
			maxTasks:  2,
			state:     &protos.ScheduleBackupChunksTasksState{},
		},
	}
}

func (s scheduleBackupChunksTasksTest) run(t *testing.T) {
	err := s.task.Run(s.ctx, s.execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	mock.AssertExpectationsForObjects(t, s.storage, s.scheduler)
}

func (s scheduleBackupChunksTasksTest) expectQueued(
	firstShardID uint64,
	lastShardID uint64,
	queued bool,
) {

	var entries []snapshot_storage.BackupChunkQueueEntry
	if queued {
		entries = append(entries, snapshot_storage.BackupChunkQueueEntry{
			SnapshotID: "snapshot",
			ChunkID:    "chunk",
		})
	}

	s.storage.On(
		"GetQueuedChunksToBackup",
		mock.Anything,
		firstShardID,
		lastShardID,
		(*snapshot_storage.BackupChunkQueueEntry)(nil),
		1,
	).Return(entries, nil).Once()
}

func (s scheduleBackupChunksTasksTest) expectQueueQueued(queued bool) {
	s.expectQueued(0, math.MaxUint64, queued)
}

func (s scheduleBackupChunksTasksTest) expectRangeQueued(
	index uint32,
	queued bool,
) {

	first, last := backupChunkShardRange(index, testBackupChunkRangeCount)
	s.expectQueued(first, last, queued)
}

func (s scheduleBackupChunksTasksTest) expectTaskScheduled(
	index uint32,
	rangeScheduledCount int,
) {

	first, last := backupChunkShardRange(index, testBackupChunkRangeCount)
	key := fmt.Sprintf(
		"backup_chunks_dispatcher_%v_%v_%v",
		testBackupChunkRangeCount,
		index,
		rangeScheduledCount,
	)
	s.scheduler.On(
		"ScheduleTask",
		mock.MatchedBy(func(ctx context.Context) bool {
			return headers.GetIdempotencyKey(ctx) == key
		}),
		"dataplane.BackupChunks",
		"",
		mock.MatchedBy(func(request *protos.BackupChunksRequest) bool {
			return request.FirstShardId == first &&
				request.LastShardId == last
		}),
	).Return(fmt.Sprintf("task_%v", index), nil).Once()
}

func (s scheduleBackupChunksTasksTest) expectTaskDone(
	taskID string,
	done bool,
) {

	s.scheduler.On("GetOperation", mock.Anything, taskID).Return(
		&operation.Operation{Id: taskID, Done: done},
		nil,
	).Once()
}

// The state of a dispatcher that split the queue for two tasks and has
// scheduled one task for each range in taskIDs.
func (s scheduleBackupChunksTasksTest) withRangeTasks(
	taskIDs map[uint32]string,
) {

	s.task.state.RangeCount = testBackupChunkRangeCount
	s.task.state.RangeTaskIds = make([]string, testBackupChunkRangeCount)
	s.task.state.RangeScheduledCounts = make(
		[]uint64,
		testBackupChunkRangeCount,
	)
	for index, taskID := range taskIDs {
		s.task.state.RangeTaskIds[index] = taskID
		s.task.state.RangeScheduledCounts[index] = 1
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestScheduleBackupChunksTasksSchedulesNothingForEmptyQueue(t *testing.T) {
	s := newScheduleBackupChunksTasksTest()
	s.expectQueueQueued(false)

	s.run(t)
	require.EqualValues(t, testBackupChunkRangeCount, s.task.state.RangeCount)
	require.Equal(
		t,
		make([]string, testBackupChunkRangeCount),
		s.task.state.RangeTaskIds,
	)
}

func TestScheduleBackupChunksTasksSchedulesOneTaskPerQueuedRange(
	t *testing.T,
) {

	s := newScheduleBackupChunksTasksTest()
	s.expectQueueQueued(true)
	s.expectRangeQueued(0, false)
	s.expectRangeQueued(1, true)
	s.expectTaskScheduled(1, 0)
	s.expectRangeQueued(2, false)
	s.expectRangeQueued(3, true)
	s.expectTaskScheduled(3, 0)

	// Two tasks run: ranges 4..7 are not looked at.
	s.run(t)
	require.Equal(t, "task_1", s.task.state.RangeTaskIds[1])
	require.Equal(t, "task_3", s.task.state.RangeTaskIds[3])
	require.EqualValues(t, 1, s.task.state.RangeScheduledCounts[1])
	require.EqualValues(t, 1, s.task.state.RangeScheduledCounts[3])
	require.EqualValues(t, 4, s.task.state.NextRange)
}

func TestScheduleBackupChunksTasksLeavesRangeToItsTask(t *testing.T) {
	s := newScheduleBackupChunksTasksTest()
	s.withRangeTasks(map[uint32]string{0: "task_0"})
	s.expectTaskDone("task_0", false)
	s.expectQueueQueued(true)
	// Range 0 still has its task, so it is not looked at again.
	s.expectRangeQueued(1, true)
	s.expectTaskScheduled(1, 0)

	s.run(t)
	require.Equal(t, "task_0", s.task.state.RangeTaskIds[0])
	require.Equal(t, "task_1", s.task.state.RangeTaskIds[1])
}

func TestScheduleBackupChunksTasksReplacesEndedTasks(t *testing.T) {
	s := newScheduleBackupChunksTasksTest()
	s.withRangeTasks(map[uint32]string{1: "old_1", 2: "old_2"})
	s.expectTaskDone("old_1", true)
	// The task of range 2 ended long ago and was cleared from the storage.
	s.scheduler.On("GetOperation", mock.Anything, "old_2").Return(
		nil,
		errors.NewNonRetriableError(errors.NewNotFoundErrorWithTaskID("old_2")),
	).Once()
	s.expectQueueQueued(true)
	s.expectRangeQueued(0, false)
	// The new task of a range gets the next key of the range.
	s.expectRangeQueued(1, true)
	s.expectTaskScheduled(1, 1)
	s.expectRangeQueued(2, true)
	s.expectTaskScheduled(2, 1)

	s.run(t)
	require.Equal(t, "task_1", s.task.state.RangeTaskIds[1])
	require.Equal(t, "task_2", s.task.state.RangeTaskIds[2])
}

func TestScheduleBackupChunksTasksWaitsForTasksOfOldSplit(t *testing.T) {
	s := newScheduleBackupChunksTasksTest()
	// The queue was split for one task; the limit is two tasks now.
	s.task.state.RangeCount = backupChunkRangesPerTask
	s.task.state.RangeTaskIds = []string{"old_0", "", "", ""}
	s.task.state.RangeScheduledCounts = []uint64{1, 0, 0, 0}

	// The old range overlaps the new ones: nothing starts while it runs.
	s.expectTaskDone("old_0", false)
	s.run(t)
	require.EqualValues(t, backupChunkRangesPerTask, s.task.state.RangeCount)

	s.expectTaskDone("old_0", true)
	s.expectQueueQueued(true)
	s.expectRangeQueued(0, true)
	s.expectTaskScheduled(0, 0)
	s.expectRangeQueued(1, false)
	s.expectRangeQueued(2, false)
	s.expectRangeQueued(3, false)
	s.expectRangeQueued(4, false)
	s.expectRangeQueued(5, false)
	s.expectRangeQueued(6, false)
	s.expectRangeQueued(7, false)

	s.run(t)
	require.EqualValues(t, testBackupChunkRangeCount, s.task.state.RangeCount)
	require.Equal(t, "task_0", s.task.state.RangeTaskIds[0])
}

func TestScheduleBackupChunksTasksIgnoresRetryLimit(t *testing.T) {
	s := newScheduleBackupChunksTasksTest()
	s.withRangeTasks(map[uint32]string{0: "task_0"})
	s.scheduler.On("GetOperation", mock.Anything, "task_0").Return(
		nil,
		errors.NewRetriableErrorf("storage is unavailable"),
	).Once()

	// A failed dispatcher would be replaced by one that does not know the
	// running tasks, so transient errors never add up to a failure.
	err := s.task.Run(s.ctx, s.execCtx)
	retriable := errors.NewEmptyRetriableError()
	require.True(t, errors.As(err, &retriable))
	require.True(t, retriable.IgnoreRetryLimit)
	mock.AssertExpectationsForObjects(t, s.storage, s.scheduler)
}

func TestBackupChunkShardRangesCoverShardSpace(t *testing.T) {
	for _, count := range []uint32{1, 3, 8, 200} {
		first, _ := backupChunkShardRange(0, count)
		require.Zero(t, first)

		_, last := backupChunkShardRange(count-1, count)
		require.Equal(t, uint64(math.MaxUint64), last)

		for index := uint32(1); index < count; index++ {
			_, previousLast := backupChunkShardRange(index-1, count)
			first, last := backupChunkShardRange(index, count)
			require.Equal(t, previousLast+1, first)
			require.Less(t, first, last)
		}
	}
}
