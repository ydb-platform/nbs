package storage

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type storageYDB struct {
	db                  *persistence.YDBClient
	folder              string
	tablesPath          string
	taskStallingTimeout time.Duration
	updateTaskTimeout   time.Duration
	livenessWindow      time.Duration
	ZoneIDs             []string
	metrics             storageMetrics

	exceptHangingTaskTypes            []string
	inflightDurationHangTimeout       time.Duration
	stallingDurationHangTimeout       time.Duration
	totalDurationHangTimeout          time.Duration
	missedEstimatesUntilTaskIsHanging uint64
}

func (s *storageYDB) CreateTask(
	ctx context.Context,
	state TaskState,
) (string, error) {

	var taskID string

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			taskID, err = s.createTask(ctx, session, state)
			return err
		},
	)
	return taskID, err
}

func (s *storageYDB) CreateRegularTasks(
	ctx context.Context,
	state TaskState,
	schedule TaskSchedule,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.createRegularTasks(ctx, session, state, schedule)
		},
	)
}

func (s *storageYDB) GetTask(
	ctx context.Context,
	taskID string,
) (TaskState, error) {

	var task TaskState

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			task, err = s.getTask(ctx, session, taskID)
			return err
		},
	)
	return task, err
}

func (s *storageYDB) GetTaskByIdempotencyKey(
	ctx context.Context,
	idempotencyKey string,
	accountID string,
) (TaskState, error) {

	var task TaskState

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			task, err = s.getTaskByIdempotencyKey(
				ctx,
				session,
				idempotencyKey,
				accountID,
			)
			return err
		},
	)
	return task, err
}

func (s *storageYDB) ListTasksReadyToRun(
	ctx context.Context,
	limit uint64,
	taskTypeWhitelist []string,
) ([]TaskInfo, error) {

	var tasks []TaskInfo

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			tasks, err = s.listTasks(
				ctx,
				session,
				"ready_to_run",
				limit,
				taskTypeWhitelist,
			)
			return err
		},
	)
	return tasks, err
}

func (s *storageYDB) ListTasksReadyToCancel(
	ctx context.Context,
	limit uint64,
	taskTypeWhitelist []string,
) ([]TaskInfo, error) {

	var tasks []TaskInfo

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			tasks, err = s.listTasks(
				ctx,
				session,
				"ready_to_cancel",
				limit,
				taskTypeWhitelist,
			)
			return err
		},
	)
	return tasks, err
}

func (s *storageYDB) ListTasksStallingWhileRunning(
	ctx context.Context,
	excludingHostname string,
	limit uint64,
	taskTypeWhitelist []string,
) ([]TaskInfo, error) {

	var tasks []TaskInfo

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			tasks, err = s.listTasksStallingWhileExecuting(
				ctx,
				session,
				excludingHostname,
				"running",
				limit,
				taskTypeWhitelist,
			)
			return err
		},
	)
	return tasks, err
}

func (s *storageYDB) ListTasksStallingWhileCancelling(
	ctx context.Context,
	excludingHostname string,
	limit uint64,
	taskTypeWhitelist []string,
) ([]TaskInfo, error) {

	var tasks []TaskInfo

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			tasks, err = s.listTasksStallingWhileExecuting(
				ctx,
				session,
				excludingHostname,
				"cancelling",
				limit,
				taskTypeWhitelist,
			)
			return err
		},
	)
	return tasks, err
}

func (s *storageYDB) ListTasksRunning(
	ctx context.Context,
	limit uint64,
) ([]TaskInfo, error) {

	var tasks []TaskInfo

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			tasks, err = s.listTasks(
				ctx,
				session,
				"running",
				limit,
				nil, // taskTypeWhitelist
			)
			return err
		},
	)
	return tasks, err
}

func (s *storageYDB) ListTasksCancelling(
	ctx context.Context,
	limit uint64,
) ([]TaskInfo, error) {

	var tasks []TaskInfo

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			tasks, err = s.listTasks(
				ctx,
				session,
				"cancelling",
				limit,
				nil, // taskTypeWhitelist
			)
			return err
		},
	)
	return tasks, err
}

func (s *storageYDB) ListTasksWithStatus(
	ctx context.Context,
	status string,
) ([]TaskInfo, error) {

	limit := ^uint64(0)

	switch status {
	case TaskStatusToString(TaskStatusReadyToRun):
		return s.ListTasksReadyToRun(ctx, limit, nil /* taskTypeWhitelist */)
	case TaskStatusToString(TaskStatusReadyToCancel):
		return s.ListTasksReadyToCancel(ctx, limit, nil /* taskTypeWhitelist */)
	case TaskStatusToString(TaskStatusRunning):
		return s.ListTasksRunning(ctx, limit)
	case TaskStatusToString(TaskStatusCancelling):
		return s.ListTasksCancelling(ctx, limit)
	default:
		return nil, errors.NewNonRetriableErrorf(
			"listing of tasks with status %s is not supported",
			status,
		)
	}
}

func (s *storageYDB) ListHangingTasks(
	ctx context.Context,
	limit uint64,
) ([]TaskInfo, error) {

	var tasks []TaskInfo
	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			tasks, err = s.listHangingTasks(ctx, session, limit)
			return err
		},
	)
	return tasks, err
}

func (s *storageYDB) ListFailedTasks(
	ctx context.Context,
	since time.Time,
) ([]string, error) {

	var tasks []string

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			tasks, err = s.listFailedTasks(ctx, session, since)
			return err
		},
	)
	return tasks, err
}

func (s *storageYDB) ListSlowTasks(
	ctx context.Context,
	since time.Time,
	estimateMiss time.Duration,
) ([]string, error) {

	var tasks []string

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			tasks, err = s.listSlowTasks(ctx, session, since, estimateMiss)
			return err
		},
	)
	return tasks, err
}

func (s *storageYDB) LockTaskToRun(
	ctx context.Context,
	taskInfo TaskInfo,
	at time.Time,
	hostname string,
	runner string,
) (TaskState, error) {

	var task TaskState

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			task, err = s.lockTaskToExecute(
				ctx,
				session,
				taskInfo,
				at,
				func(status TaskStatus) bool {
					return status == TaskStatusReadyToRun || status == TaskStatusRunning
				},
				TaskStatusRunning,
				hostname,
				runner,
			)
			return err
		},
	)
	return task, err
}

func (s *storageYDB) LockTaskToCancel(
	ctx context.Context,
	taskInfo TaskInfo,
	at time.Time,
	hostname string,
	runner string,
) (TaskState, error) {

	var task TaskState

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			task, err = s.lockTaskToExecute(
				ctx,
				session,
				taskInfo,
				at,
				func(status TaskStatus) bool {
					return status == TaskStatusReadyToCancel || status == TaskStatusCancelling
				},
				TaskStatusCancelling,
				hostname,
				runner,
			)
			return err
		},
	)
	return task, err
}

func (s *storageYDB) MarkForCancellation(
	ctx context.Context,
	taskID string,
	at time.Time,
) (bool, error) {

	var cancelling bool

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			cancelling, err = s.markForCancellation(ctx, session, taskID, at)
			return err
		},
	)
	return cancelling, err
}

func (s *storageYDB) UpdateTaskWithPreparation(
	ctx context.Context,
	state TaskState,
	preparation func(context.Context, *persistence.Transaction) error,
) (TaskState, error) {

	var newState TaskState

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			newState, err = s.updateTaskWithPreparation(ctx, session, state, preparation)
			return err
		},
	)
	return newState, err
}

func (s *storageYDB) UpdateTask(
	ctx context.Context,
	state TaskState,
) (TaskState, error) {

	var newState TaskState

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			newState, err = s.updateTask(ctx, session, state)
			return err
		},
	)
	return newState, err
}

func (s *storageYDB) SendEvent(
	ctx context.Context,
	taskID string,
	event int64,
) error {

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			err := s.sendEvent(ctx, session, taskID, event)
			return err
		},
	)
	return err
}

func (s *storageYDB) ClearEndedTasks(
	ctx context.Context,
	endedBefore time.Time,
	limit int,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.clearEndedTasks(ctx, session, endedBefore, limit)
		},
	)
}

func (s *storageYDB) ForceFinishTask(ctx context.Context, taskID string) error {
	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.forceFinishTask(ctx, session, taskID)
		},
	)
}

func (s *storageYDB) PauseTask(ctx context.Context, taskID string) error {
	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.pauseTask(ctx, session, taskID)
		},
	)
}

func (s *storageYDB) ResumeTask(ctx context.Context, taskID string) error {
	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.resumeTask(ctx, session, taskID)
		},
	)
}
