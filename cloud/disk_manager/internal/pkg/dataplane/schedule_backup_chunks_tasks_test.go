package dataplane

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
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
			registry:   metrics.NewEmptyRegistry(),
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

// The read from range fromIndex finds the first queued chunk in range
// foundIndex, or nothing when foundIndex is negative.
func (s scheduleBackupChunksTasksTest) expectFirstQueued(
	fromIndex uint32,
	foundIndex int,
) {

	from, _ := backupChunkShardRange(fromIndex, testBackupChunkRangeCount)
	if foundIndex < 0 {
		s.storage.On(
			"GetFirstQueuedChunkShardID",
			mock.Anything,
			from,
		).Return(uint64(0), false, nil).Once()
		return
	}

	found, _ := backupChunkShardRange(
		uint32(foundIndex),
		testBackupChunkRangeCount,
	)
	s.storage.On(
		"GetFirstQueuedChunkShardID",
		mock.Anything,
		from,
	).Return(found, true, nil).Once()
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
	s.expectFirstQueued(0, -1)

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
	// Ranges 0 and 2 are empty: the reads skip them.
	s.expectFirstQueued(0, 1)
	s.expectTaskScheduled(1, 0)
	s.expectFirstQueued(2, 3)
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
	// Range 0 still has its task, so it is not looked at again.
	s.expectFirstQueued(1, 1)
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
	// The new task of a range gets the next key of the range.
	s.expectFirstQueued(0, 1)
	s.expectTaskScheduled(1, 1)
	s.expectFirstQueued(2, 2)
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
	s.expectFirstQueued(0, 0)
	s.expectTaskScheduled(0, 0)
	// Nothing is queued after range 0: one read covers ranges 1..7.
	s.expectFirstQueued(1, -1)

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
	s.expectFirstQueued(2, 2)
	s.expectTaskScheduled(2, 0)

	s.run(t)
	require.EqualValues(t, testBackupChunkRangeCount, s.task.state.RangeCount)
	require.Equal(t, "task_0", s.task.state.RangeTaskIds[0])
	require.Equal(t, "task_2", s.task.state.RangeTaskIds[2])
}

func TestScheduleBackupChunksTasksWrapsAroundFromNextRange(t *testing.T) {
	s := newScheduleBackupChunksTasksTest()
	s.withRangeTasks(nil)
	s.task.state.NextRange = 6

	// Nothing from range 6 on; the scan wraps to the start and finds range 1.
	s.expectFirstQueued(6, -1)
	s.expectFirstQueued(0, 1)
	s.expectTaskScheduled(1, 0)
	s.expectFirstQueued(2, -1)

	s.run(t)
	require.Equal(t, "task_1", s.task.state.RangeTaskIds[1])
	require.EqualValues(t, 2, s.task.state.NextRange)
}

func TestScheduleBackupChunksTasksStopsWrappedScanAtNextRange(t *testing.T) {
	s := newScheduleBackupChunksTasksTest()
	s.withRangeTasks(nil)
	s.task.state.NextRange = 4

	// Only range 5 is queued: the wrapped scan from 0 finds it again and
	// stops, as ranges from 4 on were already looked at.
	s.expectFirstQueued(4, 5)
	s.expectTaskScheduled(5, 0)
	s.expectFirstQueued(6, -1)
	s.expectFirstQueued(0, 5)

	s.run(t)
	require.Equal(t, "task_5", s.task.state.RangeTaskIds[5])
	require.EqualValues(t, 6, s.task.state.NextRange)
}

func TestScheduleBackupChunksTasksSkipsRangeWithTaskFoundByRead(
	t *testing.T,
) {

	s := newScheduleBackupChunksTasksTest()
	s.withRangeTasks(map[uint32]string{3: "task_3"})
	s.expectEndedTasks([]string{"task_3"}, nil)

	// The read from range 0 lands in range 3, which has its task: the next
	// read starts after it.
	s.expectFirstQueued(0, 3)
	s.expectFirstQueued(4, 6)
	s.expectTaskScheduled(6, 0)

	s.run(t)
	require.Equal(t, "task_3", s.task.state.RangeTaskIds[3])
	require.Equal(t, "task_6", s.task.state.RangeTaskIds[6])
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

func TestBackupChunkShardIndexInvertsShardRange(t *testing.T) {
	for _, count := range []uint32{1, 3, 8, 200, 400} {
		for index := uint32(0); index < count; index++ {
			first, last := backupChunkShardRange(index, count)
			require.Equal(t, index, backupChunkShardIndex(first, count))
			require.Equal(t, index, backupChunkShardIndex(last, count))
		}
	}
}
