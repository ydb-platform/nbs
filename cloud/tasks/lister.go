package tasks

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/common"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type listTasksFunc = func(
	ctx context.Context,
	limit uint64,
) ([]storage.TaskInfo, error)

type lister struct {
	listTasks             listTasksFunc
	channels              []*channel
	tasksToListLimit      uint64
	pollForTasksPeriodMin time.Duration
	pollForTasksPeriodMax time.Duration
	inflightTasks         sync.Map
	inflightTasksByType   sync.Map
	inflightTaskCount     uint32
	inflightTaskLimits    map[string]int64 // by task type
}

func (l *lister) loop(ctx context.Context) {
	defer func() {
		for _, c := range l.channels {
			c.close()
		}
		logging.Debug(ctx, "lister stopped")
	}()

	wait := func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(
			common.RandomDuration(
				l.pollForTasksPeriodMin,
				l.pollForTasksPeriodMax,
			)):
		}

		return nil
	}

	for {
		inflightTaskCount := atomic.LoadUint32(&l.inflightTaskCount)
		channelsLen := uint32(len(l.channels))
		if channelsLen <= inflightTaskCount {
			err := wait()
			if err != nil {
				return
			}

			continue
		}

		tasks, err := l.listTasks(ctx, l.tasksToListLimit)
		if err == nil {
			logging.Debug(ctx, "lister listed %v tasks", len(tasks))

			// HACK: Random shuffle tasks in order to reduce contention between
			// nodes.
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(
				len(tasks),
				func(i, j int) { tasks[i], tasks[j] = tasks[j], tasks[i] },
			)

			taskIdx := 0
			for _, channel := range l.channels {
				if taskIdx >= len(tasks) {
					break
				}

				task := tasks[taskIdx]

				_, loaded := l.inflightTasks.Load(task.ID)
				if loaded {
					taskIdx++
					// This task is already executing, drop it.
					continue
				}

				taskLimit, hasInflightTaskLimit := l.inflightTaskLimits[task.TaskType]

				if hasInflightTaskLimit {
					value, _ := l.inflightTasksByType.LoadOrStore(task.TaskType, int64(0))
					taskCount := value.(int64)

					logging.Debug(
						ctx,
						"lister listed %v tasks with taskType %v",
						taskCount,
						task.TaskType,
					)

					if taskCount >= taskLimit {
						taskIdx++
						// Skip task in order not to exceed limit for inflight tasks count
						// with the same type.
						continue
					}

					ok := l.inflightTasksByType.CompareAndSwap(
						task.TaskType,
						taskCount,
						taskCount+1,
					)
					if !ok {
						continue
					}
				}

				l.inflightTasks.Store(task.ID, struct{}{})
				atomic.AddUint32(&l.inflightTaskCount, 1)

				handle := taskHandle{
					task: task,
					onClose: func(hasInflightTaskLimit bool) func() {
						return func() {
							l.inflightTasks.Delete(task.ID)

							if hasInflightTaskLimit {
								for {
									value, _ := l.inflightTasksByType.Load(task.TaskType)
									taskCount := value.(int64)

									ok := l.inflightTasksByType.CompareAndSwap(task.TaskType, taskCount, taskCount-1)
									if ok {
										break
									}
								}
							}

							// Decrement.
							atomic.AddUint32(&l.inflightTaskCount, ^uint32(0))
						}
					}(hasInflightTaskLimit),
				}
				if channel.send(handle) {
					taskIdx++
				}
			}
		}

		err = wait()
		if err != nil {
			return
		}
	}
}

func (l *lister) getInflightTaskCount() uint32 {
	return atomic.LoadUint32(&l.inflightTaskCount)
}

////////////////////////////////////////////////////////////////////////////////

func newLister(
	ctx context.Context,
	listTasks listTasksFunc,
	channelsCount uint64,
	tasksToListLimit uint64,
	pollForTasksPeriodMin time.Duration,
	pollForTasksPeriodMax time.Duration,
	inflightTaskLimits map[string]int64,
) *lister {

	lister := &lister{
		listTasks:             listTasks,
		channels:              make([]*channel, channelsCount),
		tasksToListLimit:      tasksToListLimit,
		pollForTasksPeriodMin: pollForTasksPeriodMin,
		pollForTasksPeriodMax: pollForTasksPeriodMax,
		inflightTaskLimits:    inflightTaskLimits,
	}
	for i := 0; i < len(lister.channels); i++ {
		lister.channels[i] = &channel{
			handle: make(chan taskHandle),
		}
	}
	go lister.loop(ctx)
	return lister
}
