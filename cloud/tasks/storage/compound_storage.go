package storage

import (
	"context"
	"time"

	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

// Needed for smooth migration from legacy to new tables.
type compoundStorage struct {
	legacyStorageFolder string
	legacyStorage       Storage

	storageFolder string
	storage       Storage
}

func (s *compoundStorage) invoke(
	ctx context.Context,
	call func(Storage) error,
) error {

	err := call(s.legacyStorage)
	if err != nil {
		if errors.Is(err, errors.NewEmptyNotFoundError()) {
			return call(s.storage)
		}

		return err
	}

	return nil
}

func (s *compoundStorage) visit(
	ctx context.Context,
	call func(Storage) error,
) error {

	err := call(s.legacyStorage)
	if err != nil {
		return err
	}

	return call(s.storage)
}

func (s *compoundStorage) dispatch(
	ctx context.Context,
	storageFolder string,
	call func(Storage) error,
) error {

	if storageFolder == s.legacyStorageFolder {
		return call(s.legacyStorage)
	} else if storageFolder == s.storageFolder {
		return call(s.storage)
	}

	return errors.NewNonRetriableErrorf(
		"storageFolder=%v should be one of %v, %v",
		storageFolder,
		s.legacyStorageFolder,
		s.storageFolder,
	)
}

func (s *compoundStorage) CreateTask(
	ctx context.Context,
	state TaskState,
) (string, error) {

	if len(state.StorageFolder) == 0 {
		existing, err := s.legacyStorage.GetTaskByIdempotencyKey(
			ctx,
			state.IdempotencyKey,
			state.AccountID,
		)
		if err != nil {
			if errors.Is(err, errors.NewEmptyNotFoundError()) {
				return s.storage.CreateTask(ctx, state)
			}

			return "", err
		}

		return existing.ID, nil
	}

	var taskID string
	var err error

	err = s.dispatch(ctx, state.StorageFolder, func(storage Storage) error {
		taskID, err = storage.CreateTask(ctx, state)
		return err
	})
	return taskID, err
}

func (s *compoundStorage) CreateRegularTasks(
	ctx context.Context,
	state TaskState,
	schedule TaskSchedule,
) error {

	// Don't need to use legacyStorage here.
	return s.storage.CreateRegularTasks(ctx, state, schedule)
}

func (s *compoundStorage) GetTask(
	ctx context.Context,
	taskID string,
) (state TaskState, err error) {

	err = s.invoke(ctx, func(storage Storage) error {
		state, err = storage.GetTask(ctx, taskID)
		return err
	})
	return state, err
}

func (s *compoundStorage) GetTaskByIdempotencyKey(
	ctx context.Context,
	idempotencyKey string,
	accountID string,
) (state TaskState, err error) {

	err = s.invoke(ctx, func(storage Storage) error {
		state, err = storage.GetTaskByIdempotencyKey(
			ctx,
			idempotencyKey,
			accountID,
		)
		return err
	})
	return state, err
}

func (s *compoundStorage) ListTasksReadyToRun(
	ctx context.Context,
	limit uint64,
	taskTypeWhitelist []string,
) (taskInfos []TaskInfo, err error) {

	err = s.visit(ctx, func(storage Storage) error {
		values, err := storage.ListTasksReadyToRun(
			ctx,
			limit,
			taskTypeWhitelist,
		)
		taskInfos = append(taskInfos, values...)
		return err
	})
	return taskInfos, err
}

func (s *compoundStorage) ListTasksReadyToCancel(
	ctx context.Context,
	limit uint64,
	taskTypeWhitelist []string,
) (taskInfos []TaskInfo, err error) {

	err = s.visit(ctx, func(storage Storage) error {
		values, err := storage.ListTasksReadyToCancel(
			ctx,
			limit,
			taskTypeWhitelist,
		)
		taskInfos = append(taskInfos, values...)
		return err
	})
	return taskInfos, err
}

func (s *compoundStorage) ListTasksStallingWhileRunning(
	ctx context.Context,
	excludingHostname string,
	limit uint64,
	taskTypeWhitelist []string,
) (taskInfos []TaskInfo, err error) {

	err = s.visit(ctx, func(storage Storage) error {
		values, err := storage.ListTasksStallingWhileRunning(
			ctx,
			excludingHostname,
			limit,
			taskTypeWhitelist,
		)
		taskInfos = append(taskInfos, values...)
		return err
	})
	return taskInfos, err
}

func (s *compoundStorage) ListTasksStallingWhileCancelling(
	ctx context.Context,
	excludingHostname string,
	limit uint64,
	taskTypeWhitelist []string,
) (taskInfos []TaskInfo, err error) {

	err = s.visit(ctx, func(storage Storage) error {
		values, err := storage.ListTasksStallingWhileCancelling(
			ctx,
			excludingHostname,
			limit,
			taskTypeWhitelist,
		)
		taskInfos = append(taskInfos, values...)
		return err
	})
	return taskInfos, err
}

func (s *compoundStorage) ListTasksRunning(
	ctx context.Context,
	limit uint64,
) ([]TaskInfo, error) {

	tasks := []TaskInfo{}
	err := s.visit(ctx, func(storage Storage) error {
		values, err := storage.ListTasksRunning(ctx, limit)
		tasks = append(tasks, values...)
		return err
	})
	return tasks, err
}

func (s *compoundStorage) ListTasksCancelling(
	ctx context.Context,
	limit uint64,
) ([]TaskInfo, error) {

	tasks := []TaskInfo{}
	err := s.visit(ctx, func(storage Storage) error {
		values, err := storage.ListTasksCancelling(
			ctx,
			limit,
		)
		tasks = append(tasks, values...)
		return err
	})
	return tasks, err
}

func (s *compoundStorage) ListTasksWithStatus(
	ctx context.Context,
	status string,
) ([]TaskInfo, error) {

	tasks := []TaskInfo{}
	err := s.visit(ctx, func(storage Storage) error {
		values, err := storage.ListTasksWithStatus(
			ctx,
			status,
		)
		tasks = append(tasks, values...)
		return err
	})

	return tasks, err
}

func (s *compoundStorage) ListHangingTasks(
	ctx context.Context,
	limit uint64,
) ([]TaskInfo, error) {

	tasks := []TaskInfo{}
	err := s.visit(
		ctx,
		func(storage Storage) error {
			values, err := storage.ListHangingTasks(ctx, limit)
			tasks = append(tasks, values...)
			return err
		},
	)
	return tasks, err
}

func (s *compoundStorage) ListFailedTasks(
	ctx context.Context,
	since time.Time,
) ([]string, error) {

	tasks := []string{}
	err := s.visit(ctx, func(storage Storage) error {
		values, err := storage.ListFailedTasks(ctx, since)
		tasks = append(tasks, values...)
		return err
	})
	return tasks, err
}

func (s *compoundStorage) ListSlowTasks(
	ctx context.Context,
	since time.Time,
	estimateMiss time.Duration,
) ([]string, error) {

	tasks := []string{}
	err := s.visit(ctx, func(storage Storage) error {
		values, err := storage.ListSlowTasks(ctx, since, estimateMiss)
		tasks = append(tasks, values...)
		return err
	})
	return tasks, err
}

func (s *compoundStorage) LockTaskToRun(
	ctx context.Context,
	taskInfo TaskInfo,
	at time.Time,
	hostname string,
	runner string,
) (state TaskState, err error) {

	err = s.invoke(ctx, func(storage Storage) error {
		state, err = storage.LockTaskToRun(
			ctx,
			taskInfo,
			at,
			hostname,
			runner,
		)
		return err
	})
	return state, err
}

func (s *compoundStorage) LockTaskToCancel(
	ctx context.Context,
	taskInfo TaskInfo,
	at time.Time,
	hostname string,
	runner string,
) (state TaskState, err error) {

	err = s.invoke(ctx, func(storage Storage) error {
		state, err = storage.LockTaskToCancel(
			ctx,
			taskInfo,
			at,
			hostname,
			runner,
		)
		return err
	})
	return state, err
}

func (s *compoundStorage) MarkForCancellation(
	ctx context.Context,
	taskID string,
	now time.Time,
) (cancelling bool, err error) {

	err = s.invoke(ctx, func(storage Storage) error {
		cancelling, err = storage.MarkForCancellation(ctx, taskID, now)
		return err
	})
	return cancelling, err
}

func (s *compoundStorage) UpdateTaskWithPreparation(
	ctx context.Context,
	state TaskState,
	preparation func(context.Context, *persistence.Transaction) error,
) (res TaskState, err error) {

	err = s.invoke(ctx, func(storage Storage) error {
		res, err = storage.UpdateTaskWithPreparation(ctx, state, preparation)
		return err
	})
	return res, err
}

func (s *compoundStorage) UpdateTask(
	ctx context.Context,
	state TaskState,
) (res TaskState, err error) {

	err = s.invoke(ctx, func(storage Storage) error {
		res, err = storage.UpdateTask(ctx, state)
		return err
	})
	return res, err
}

func (s *compoundStorage) SendEvent(
	ctx context.Context,
	taskID string,
	event int64,
) error {

	err := s.invoke(ctx, func(storage Storage) error {
		err := storage.SendEvent(ctx, taskID, event)
		return err
	})
	return err
}

func (s *compoundStorage) ClearEndedTasks(
	ctx context.Context,
	endedBefore time.Time,
	limit int,
) error {

	// Don't need to use legacyStorage here.
	return s.storage.ClearEndedTasks(ctx, endedBefore, limit)
}

func (s *compoundStorage) ForceFinishTask(ctx context.Context, taskID string) error {
	return errors.NewNonRetriableErrorf("not implemented")
}

func (s *compoundStorage) PauseTask(ctx context.Context, taskID string) error {
	return errors.NewNonRetriableErrorf("not implemented")
}

func (s *compoundStorage) ResumeTask(ctx context.Context, taskID string) error {
	return errors.NewNonRetriableErrorf("not implemented")
}

func (s *compoundStorage) HeartbeatNode(
	ctx context.Context,
	host string,
	ts time.Time,
	inflightTaskCount uint32,
) error {

	return s.storage.HeartbeatNode(ctx, host, ts, inflightTaskCount)
}

func (s *compoundStorage) GetAliveNodes(ctx context.Context) ([]Node, error) {
	return s.storage.GetAliveNodes(ctx)
}

func (s *compoundStorage) GetNode(
	ctx context.Context,
	host string,
) (Node, error) {

	return s.storage.GetNode(ctx, host)
}

////////////////////////////////////////////////////////////////////////////////

func NewStorage(
	config *tasks_config.TasksConfig,
	metricsRegistry metrics.Registry,
	db *persistence.YDBClient,
) (Storage, error) {

	taskStallingTimeout, err := time.ParseDuration(config.GetTaskStallingTimeout())
	if err != nil {
		return nil, err
	}

	updateTaskTimeout, err := time.ParseDuration(config.GetUpdateTaskTimeout())
	if err != nil {
		return nil, err
	}

	livenessWindow, err := time.ParseDuration(config.GetLivenessWindow())
	if err != nil {
		return nil, err
	}

	hangingTaskTimeout, err := time.ParseDuration(
		config.GetHangingTaskTimeout(),
	)
	if err != nil {
		return nil, err
	}

	newStorage := func(storageFolder string, metrics storageMetrics) *storageYDB {
		return &storageYDB{
			db:                  db,
			folder:              storageFolder,
			tablesPath:          db.AbsolutePath(storageFolder),
			taskStallingTimeout: taskStallingTimeout,
			updateTaskTimeout:   updateTaskTimeout,
			livenessWindow:      livenessWindow,
			ZoneIDs:             config.GetZoneIds(),
			metrics:             metrics,

			exceptHangingTaskTypes:            config.GetExceptHangingTaskTypes(),
			hangingTaskTimeout:                hangingTaskTimeout,
			missedEstimatesUntilTaskIsHanging: config.GetMissedEstimatesUntilTaskIsHanging(),
		}
	}

	storage := newStorage(
		config.GetStorageFolder(),
		newStorageMetrics(metricsRegistry),
	)

	if len(config.GetLegacyStorageFolder()) == 0 {
		return storage, nil
	}

	// Ignore legacy metrics.
	legacyStorage := newStorage(
		config.GetLegacyStorageFolder(),
		newStorageMetrics(empty.NewRegistry()),
	)

	return &compoundStorage{
		legacyStorageFolder: config.GetLegacyStorageFolder(),
		legacyStorage:       legacyStorage,

		storageFolder: config.GetStorageFolder(),
		storage:       storage,
	}, nil
}
