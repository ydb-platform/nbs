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
)

////////////////////////////////////////////////////////////////////////////////

const testBackupChunkRangeCount = 8

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
			scheduler:  scheduler,
			storage:    storage,
			rangeCount: testBackupChunkRangeCount,
			maxTasks:   2,
			state:      &protos.ScheduleBackupChunksTasksState{},
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

func (s scheduleBackupChunksTasksTest) expectEndedTasks(
	taskIDs []string,
	ended []string,
) {

	s.scheduler.On("GetEndedTasks", mock.Anything, taskIDs).Return(
		ended,
		nil,
	).Once()
}

// The state of a dispatcher that split the queue into
// testBackupChunkRangeCount ranges and has scheduled one task for each range
// in taskIDs.
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
	s.expectEndedTasks([]string{"task_0"}, nil)
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
	s.expectEndedTasks(
		[]string{"old_1", "old_2"},
		[]string{"old_1", "old_2"},
	)
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
	// The queue was split into four ranges; the setting is eight now.
	s.task.state.RangeCount = 4
	s.task.state.RangeTaskIds = []string{"old_0", "", "", ""}
	s.task.state.RangeScheduledCounts = []uint64{1, 0, 0, 0}

	// The old range overlaps the new ones: nothing starts while it runs.
	s.expectEndedTasks([]string{"old_0"}, nil)
	s.run(t)
	require.EqualValues(t, 4, s.task.state.RangeCount)

	s.expectEndedTasks([]string{"old_0"}, []string{"old_0"})
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

func TestScheduleBackupChunksTasksTakesNewTaskLimitAtOnce(t *testing.T) {
	s := newScheduleBackupChunksTasksTest()
	s.withRangeTasks(map[uint32]string{0: "task_0", 1: "task_1"})
	// The limit grows from two tasks to three: the split stays and one more
	// task starts at once.
	s.task.maxTasks = 3
	s.expectEndedTasks([]string{"task_0", "task_1"}, nil)
	s.expectQueueQueued(true)
	s.expectRangeQueued(2, true)
	s.expectTaskScheduled(2, 0)

	s.run(t)
	require.EqualValues(t, testBackupChunkRangeCount, s.task.state.RangeCount)
	require.Equal(t, "task_0", s.task.state.RangeTaskIds[0])
	require.Equal(t, "task_2", s.task.state.RangeTaskIds[2])
}

func TestScheduleBackupChunksTasksIgnoresRetryLimit(t *testing.T) {
	s := newScheduleBackupChunksTasksTest()
	s.withRangeTasks(map[uint32]string{0: "task_0"})
	s.scheduler.On(
		"GetEndedTasks",
		mock.Anything,
		[]string{"task_0"},
	).Return(nil, errors.NewRetriableErrorf("storage is unavailable")).Once()

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
