package storage

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/common"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
	grpc_codes "google.golang.org/grpc/codes"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

func newYDB(ctx context.Context) (*persistence.YDBClient, error) {
	endpoint := os.Getenv("YDB_ENDPOINT")
	database := os.Getenv("YDB_DATABASE")
	rootPath := "tasks"

	return persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint: &endpoint,
			Database: &database,
			RootPath: &rootPath,
		},
		empty.NewRegistry(),
	)
}

func newStorage(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	config *tasks_config.TasksConfig,
	metricsRegistry metrics.Registry,
) (Storage, error) {

	folder := fmt.Sprintf("storage_ydb_test/%v", t.Name())
	config.StorageFolder = &folder

	err := CreateYDBTables(ctx, config, db, false /* dropUnusedColumns */)
	if err != nil {
		return nil, err
	}

	return NewStorage(config, metricsRegistry, db)
}

////////////////////////////////////////////////////////////////////////////////

var lastReqNumber int

func getIdempotencyKeyForTest(t *testing.T) string {
	lastReqNumber++
	return fmt.Sprintf("%v_%v", t.Name(), lastReqNumber)
}

////////////////////////////////////////////////////////////////////////////////

func TestStorageYDBCreateTask(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	taskState := TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      time.Now(),
		CreatedBy:      "some_user",
		ModifiedAt:     time.Now(),
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	}

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": taskState.TaskType},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, taskState)
	require.NoError(t, err)
	require.NotZero(t, taskID)
	metricsRegistry.AssertAllExpectations(t)
}

func TestStorageYDBCreateTaskIgnoresID(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	taskState := TaskState{
		ID:             "abc",
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      time.Now(),
		CreatedBy:      "some_user",
		ModifiedAt:     time.Now(),
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	}

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": taskState.TaskType},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, taskState)
	require.NoError(t, err)
	require.NotZero(t, taskID)
	require.NotEqual(t, taskID, taskState.ID)
	metricsRegistry.AssertAllExpectations(t)
}

func TestStorageYDBCreateTwoTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	taskState := TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      time.Now(),
		CreatedBy:      "some_user",
		ModifiedAt:     time.Now(),
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	}

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": taskState.TaskType},
	).On("Add", int64(1)).Once()

	taskID1, err := storage.CreateTask(ctx, taskState)
	require.NoError(t, err)
	require.NotZero(t, taskID1)
	metricsRegistry.AssertAllExpectations(t)

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": taskState.TaskType},
	).On("Add", int64(1)).Once()

	taskState.IdempotencyKey = getIdempotencyKeyForTest(t)
	taskID2, err := storage.CreateTask(ctx, taskState)
	require.NoError(t, err)
	require.NotZero(t, taskID2)
	require.NotEqual(t, taskID1, taskID2)
	metricsRegistry.AssertAllExpectations(t)
}

func TestStorageYDBCreateTwoTasksWithDifferentIdempotencyKeys(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)

	taskState := TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      time.Now(),
		CreatedBy:      "some_user",
		ModifiedAt:     time.Now(),
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	}

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": taskState.TaskType},
	).On("Add", int64(1)).Once()

	taskID1, err := storage.CreateTask(ctx, taskState)
	require.NoError(t, err)
	require.NotZero(t, taskID1)
	metricsRegistry.AssertAllExpectations(t)

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": taskState.TaskType},
	).On("Add", int64(1)).Once()

	taskState.IdempotencyKey = getIdempotencyKeyForTest(t)
	taskID2, err := storage.CreateTask(ctx, taskState)
	require.NoError(t, err)
	require.NotZero(t, taskID2)
	metricsRegistry.AssertAllExpectations(t)

	require.NotEqual(t, taskID1, taskID2)
}

func TestStorageYDBCreateTwoTasksWithSameIdempotencyKey(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)

	taskState := TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      time.Now(),
		CreatedBy:      "some_user",
		ModifiedAt:     time.Now(),
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	}

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": taskState.TaskType},
	).On("Add", int64(1)).Once()

	taskID1, err := storage.CreateTask(ctx, taskState)
	require.NoError(t, err)
	require.NotZero(t, taskID1)
	metricsRegistry.AssertAllExpectations(t)

	taskID2, err := storage.CreateTask(ctx, taskState)
	require.NoError(t, err)
	require.NotZero(t, taskID2)
	metricsRegistry.AssertAllExpectations(t)

	require.Equal(t, taskID1, taskID2)
}

func TestStorageYDBFailCreationOfTwoTasksWithSameIdempotencyKeyButDifferentTypesOrRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)

	taskState1 := TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      time.Now(),
		CreatedBy:      "some_user",
		ModifiedAt:     time.Now(),
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	}

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": taskState1.TaskType},
	).On("Add", int64(1)).Once()
	taskID1, err := storage.CreateTask(ctx, taskState1)
	require.NoError(t, err)
	require.NotZero(t, taskID1)
	metricsRegistry.AssertAllExpectations(t)

	taskState2 := taskState1
	taskState2.TaskType = "task2"
	_, err = storage.CreateTask(ctx, taskState2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot create task with type")

	taskState3 := taskState1
	taskState3.Request = []byte{'a'}
	_, err = storage.CreateTask(ctx, taskState3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot create task with request")
}

func TestStorageYDBGetTask(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	createdAt := time.Now()
	modifiedAt := createdAt.Add(time.Hour)

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey:   getIdempotencyKeyForTest(t),
		TaskType:         "task1",
		Description:      "Some task",
		CreatedAt:        createdAt,
		CreatedBy:        "some_user",
		ModifiedAt:       modifiedAt,
		GenerationID:     42,
		Status:           TaskStatusReadyToRun,
		State:            []byte{1, 2, 3},
		Dependencies:     common.NewStringSet(),
		ZoneID:           "zone",
		InflightDuration: 10 * time.Minute,
		WaitingDuration:  10 * time.Minute,
    StallingDuration: 10 * time.Minute,
	})
	require.NoError(t, err)

	taskState, err := storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.EqualValues(t, taskID, taskState.ID)
	require.EqualValues(t, "task1", taskState.TaskType)
	require.EqualValues(t, "Some task", taskState.Description)
	require.WithinDuration(t, time.Time(createdAt), time.Time(taskState.CreatedAt), time.Microsecond)
	require.EqualValues(t, "some_user", taskState.CreatedBy)
	require.WithinDuration(t, time.Time(modifiedAt), time.Time(taskState.ModifiedAt), time.Microsecond)
	require.EqualValues(t, 42, taskState.GenerationID)
	require.EqualValues(t, TaskStatusReadyToRun, taskState.Status)
	require.EqualValues(t, []byte{1, 2, 3}, taskState.State)
	require.EqualValues(t, common.NewStringSet(), taskState.Dependencies)
	require.WithinDuration(t, time.Time(createdAt), time.Time(taskState.ChangedStateAt), time.Microsecond)
	require.EqualValues(t, "zone", taskState.ZoneID)
	require.EqualValues(t, 10*time.Minute, taskState.InflightDuration)
	require.EqualValues(t, 10*time.Minute, taskState.StallingDuration)
	require.EqualValues(t, 10*time.Minute, taskState.WaitingDuration)
	metricsRegistry.AssertAllExpectations(t)
}

func TestStorageYDBGetTaskWithDependencies(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "dep_task"},
	).On("Add", int64(1)).Twice()
	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	createdAt := time.Now()
	modifiedAt := createdAt.Add(time.Hour)

	depID1, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "dep_task",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     modifiedAt,
		GenerationID:   42,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	depID2, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "dep_task",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     modifiedAt,
		GenerationID:   42,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey:   getIdempotencyKeyForTest(t),
		TaskType:         "task1",
		Description:      "Some task",
		CreatedAt:        createdAt,
		CreatedBy:        "some_user",
		ModifiedAt:       modifiedAt,
		GenerationID:     42,
		Status:           TaskStatusReadyToRun,
		State:            []byte{1, 2, 3},
		Dependencies:     common.NewStringSet(depID1, depID2),
		InflightDuration: 10 * time.Minute,
		StallingDuration: 10 * time.Minute,
		WaitingDuration:  10 * time.Minute,
	})
	require.NoError(t, err)

	taskState, err := storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.EqualValues(t, taskID, taskState.ID)
	require.EqualValues(t, "task1", taskState.TaskType)
	require.EqualValues(t, "Some task", taskState.Description)
	require.WithinDuration(t, time.Time(createdAt), time.Time(taskState.CreatedAt), time.Microsecond)
	require.EqualValues(t, "some_user", taskState.CreatedBy)
	require.WithinDuration(t, time.Time(modifiedAt), time.Time(taskState.ModifiedAt), time.Microsecond)
	require.EqualValues(t, 42, taskState.GenerationID)
	require.EqualValues(t, TaskStatusWaitingToRun, taskState.Status)
	require.EqualValues(t, []byte{1, 2, 3}, taskState.State)
	require.EqualValues(t, common.NewStringSet(depID1, depID2), taskState.Dependencies)
	require.WithinDuration(t, time.Time(createdAt), time.Time(taskState.ChangedStateAt), time.Microsecond)
	require.EqualValues(t, 10*time.Minute, taskState.InflightDuration)
	require.EqualValues(t, 10*time.Minute, taskState.StallingDuration)
	require.EqualValues(t, 10*time.Minute, taskState.WaitingDuration)
	metricsRegistry.AssertAllExpectations(t)
}

func TestStorageYDBGetTaskMissing(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)

	taskState, err := storage.GetTask(ctx, "abc")
	require.Error(t, err, "Got %v", taskState)

	taskState, err = storage.GetTask(ctx, "")
	require.Error(t, err, "Got %v", taskState)
}

func TestStorageYDBListTasksReadyToRun(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createTime := time.Now().Add(-time.Hour)
	freshTime := createTime.Add(2 * time.Hour)
	stallTime := createTime
	var generationID uint64 = 42

	taskIDReadyToRun, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDReadyToRunStalling, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDFinished, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusFinished,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDReadyToCancel, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelled,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToRun, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	taskIDReadyToRunWaited, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToRun, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	expectedTaskInfos := []TaskInfo{
		TaskInfo{
			ID:           taskIDReadyToRun,
			GenerationID: generationID,
			TaskType:     "task1",
		},
		TaskInfo{
			ID:           taskIDReadyToRunStalling,
			GenerationID: generationID,
			TaskType:     "task1",
		},
		TaskInfo{
			ID:           taskIDReadyToRunWaited,
			GenerationID: generationID,
			TaskType:     "task1",
		},
	}

	taskInfos, err := storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	// TODO: Maybe we need a proper order.
	require.ElementsMatch(t, expectedTaskInfos, taskInfos)
}

func TestStorageYDBListTasksReadyToCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createTime := time.Now().Add(-time.Hour)
	freshTime := createTime.Add(2 * time.Hour)
	stallTime := createTime
	var generationID uint64 = 42

	taskIDReadyToRun, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDFinished, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusFinished,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDReadyToCancel, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDReadyToCancelStalling, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelled,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToRun, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToRun, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	taskIDReadyToCancelWaited, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	expectedTaskInfos := []TaskInfo{
		TaskInfo{
			ID:           taskIDReadyToCancel,
			GenerationID: generationID,
			TaskType:     "task1",
		},
		TaskInfo{
			ID:           taskIDReadyToCancelStalling,
			GenerationID: generationID,
			TaskType:     "task1",
		},
		TaskInfo{
			ID:           taskIDReadyToCancelWaited,
			GenerationID: generationID,
			TaskType:     "task1",
		},
	}

	taskInfos, err := storage.ListTasksReadyToCancel(ctx, 100500, nil)
	require.NoError(t, err)
	// TODO: Maybe we need a proper order.
	require.ElementsMatch(t, expectedTaskInfos, taskInfos)
}

type hangingTaskTestFixture struct {
	t                  *testing.T
	storage            Storage
	ctx                context.Context
	hangingTaskTimeout time.Duration
	db                 *persistence.YDBClient
	cancel             context.CancelFunc
}

func (f hangingTaskTestFixture) createTask(
	taskType string,
	taskStatus TaskStatus,
	createdAt time.Time,
	estimatedDuration time.Duration,
) string {

	state := TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(f.t),
		TaskType:       taskType,
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     time.Now(),
		GenerationID:   10,
		Status:         taskStatus,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	}
	if estimatedDuration > 0 {
		state.EstimatedTime = createdAt.Add(estimatedDuration)
	}

	taskID, err := f.storage.CreateTask(f.ctx, state)
	require.NoError(f.t, err)
	logging.Info(
		f.ctx,
		"task with id=%s, created_at=%v, status %s, estimate=%v",
		taskID,
		createdAt,
		TaskStatusToString(taskStatus),
		state.EstimatedTime,
	)
	return taskID
}

func (f hangingTaskTestFixture) createHangingTaskNoEstimate(
	taskType string,
	taskStatus TaskStatus,
) string {

	return f.createTask(
		taskType,
		taskStatus,
		time.Now().Add(-f.hangingTaskTimeout).Add(-time.Minute),
		-1,
	)
}

func (f hangingTaskTestFixture) createHangingTaskWithEstimate(
	taskType string,
	taskStatus TaskStatus,
) string {

	return f.createTask(
		taskType,
		taskStatus,
		time.Now().Add(
			-(f.hangingTaskTimeout+time.Hour)*2,
		).Add(-time.Minute),
		time.Hour,
	)
}

func (f hangingTaskTestFixture) ListHangingTasksIDs() []string {
	hangingTasks, err := f.storage.ListHangingTasks(
		f.ctx,
		^uint64(0),
	)
	require.NoError(f.t, err)
	hangingTaskIDs := make([]string, 0, len(hangingTasks))
	for _, task := range hangingTasks {
		hangingTaskIDs = append(hangingTaskIDs, task.ID)
	}

	return hangingTaskIDs
}

func (f hangingTaskTestFixture) teardown() {
	err := f.db.Close(f.ctx)
	require.NoError(f.t, err)
	f.cancel()
}

func newHangingTaskTestFixture(
	t *testing.T,
	config *tasks_config.TasksConfig,
) hangingTaskTestFixture {

	hangingTaskTimeout, err := time.ParseDuration(
		config.GetHangingTaskTimeout(),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(newContext())
	fixture := hangingTaskTestFixture{
		t:                  t,
		hangingTaskTimeout: hangingTaskTimeout,
		ctx:                ctx,
		cancel:             cancel,
	}
	fixture.ctx = ctx
	fixture.cancel = cancel
	db, err := newYDB(ctx)
	require.NoError(t, err)

	fixture.db = db
	metricsRegistry := empty.NewRegistry()
	storage, err := newStorage(
		t,
		ctx,
		db,
		config,
		metricsRegistry,
	)
	require.NoError(t, err)

	fixture.storage = storage
	return fixture
}

func TestStorageYDBListHangingTasks(t *testing.T) {
	hangingTaskTimeout := 5 * time.Hour
	hangingTaskTimeoutString := hangingTaskTimeout.String()
	fixture := newHangingTaskTestFixture(t, &tasks_config.TasksConfig{
		HangingTaskTimeout: &hangingTaskTimeoutString,
	})
	defer fixture.teardown()

	expectedHangingTaskIDs := make([]string, 0, 10)
	hangingTasksStatuses := []TaskStatus{
		TaskStatusReadyToRun,
		TaskStatusRunning,
		TaskStatusCancelling,
		TaskStatusReadyToCancel,
	}

	for _, taskStatus := range hangingTasksStatuses {
		taskID := fixture.createHangingTaskNoEstimate("first", taskStatus)
		expectedHangingTaskIDs = append(expectedHangingTaskIDs, taskID)

		taskID = fixture.createHangingTaskWithEstimate("second", taskStatus)
		expectedHangingTaskIDs = append(expectedHangingTaskIDs, taskID)

		fixture.createTask("a", taskStatus, time.Now(), -1)
		fixture.createTask("a", taskStatus, time.Now(), time.Hour)
		// Estimate is missed, but task duration does not exceed x2 estimate duration
		fixture.createTask(
			"b",
			taskStatus,
			time.Now().Add(-719*time.Minute),
			hangingTaskTimeout+time.Hour,
		)
		// Estimate is missed but default estimate is not
		oneHourAgo := time.Now().Add(-time.Hour)
		fixture.createTask("c", taskStatus, oneHourAgo, time.Minute*15)
	}

	fixture.createTask(
		"d",
		TaskStatusFinished,
		time.Now().Add(-hangingTaskTimeout).Add(-time.Minute),
		-1,
	)
	fixture.createTask(
		"e",
		TaskStatusCancelled,
		time.Now().Add(-time.Hour*14),
		hangingTaskTimeout+time.Hour,
	)

	actualHangingTasks := fixture.ListHangingTasksIDs()
	require.ElementsMatch(t, expectedHangingTaskIDs, actualHangingTasks)
}

func TestStorageYDBListHangingTasksWithExceptions(t *testing.T) {
	hangingTaskTimeout := "5h"
	fixture := newHangingTaskTestFixture(t, &tasks_config.TasksConfig{
		ExceptHangingTaskTypes: []string{"exception"},
		HangingTaskTimeout:     &hangingTaskTimeout,
	})
	defer fixture.teardown()

	taskID := fixture.createHangingTaskNoEstimate("first", TaskStatusRunning)
	fixture.createHangingTaskNoEstimate("exception", TaskStatusRunning)
	require.ElementsMatch(
		t,
		[]string{taskID},
		fixture.ListHangingTasksIDs(),
	)
}

func TestStorageYDBListTasksRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createTime := time.Now().Add(-time.Hour)
	freshTime := createTime.Add(2 * time.Hour)
	stallTime := createTime
	var generationID uint64 = 42

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDRunning, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDStallingWhileRunning, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusFinished,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelled,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	expectedTasks := []TaskInfo{
		TaskInfo{
			ID:           taskIDRunning,
			GenerationID: generationID,
			TaskType:     "task1",
		},
		TaskInfo{
			ID:           taskIDStallingWhileRunning,
			GenerationID: generationID,
			TaskType:     "task1",
		},
	}

	tasks, err := storage.ListTasksRunning(ctx, 100500)
	require.NoError(t, err)
	require.ElementsMatch(t, expectedTasks, tasks)
}

func TestStorageYDBListTasksCancelling(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createTime := time.Now().Add(-time.Hour)
	freshTime := createTime.Add(2 * time.Hour)
	stallTime := createTime
	var generationID uint64 = 42

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusFinished,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDCancelling, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDStallingWhileCancelling, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelled,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	expectedTasks := []TaskInfo{
		TaskInfo{
			ID:           taskIDCancelling,
			GenerationID: generationID,
			TaskType:     "task1",
		},
		TaskInfo{
			ID:           taskIDStallingWhileCancelling,
			GenerationID: generationID,
			TaskType:     "task1",
		},
	}

	tasks, err := storage.ListTasksCancelling(ctx, 100500)
	require.NoError(t, err)
	require.ElementsMatch(t, expectedTasks, tasks)
}

func TestStorageYDBListFailedTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	storage, err := newStorage(
		t,
		ctx,
		db,
		&tasks_config.TasksConfig{
			TaskStallingTimeout: &taskStallingTimeout,
		},
		metricsRegistry,
	)
	require.NoError(t, err)
	createdAt := time.Now().Add(-time.Hour)
	modifiedAt := createdAt.Add(2 * time.Hour)
	var generationID uint64 = 42

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{0},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDWithSilentError, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{0},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	_, err = storage.UpdateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		ID:             taskIDWithSilentError,
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     modifiedAt,
		GenerationID:   generationID,
		Status:         TaskStatusCancelled,
		ErrorCode:      grpc_codes.InvalidArgument,
		ErrorMessage:   "invalid argument",
		ErrorSilent:    true,
		State:          []byte{1},
	})
	require.NoError(t, err)

	taskIDWithNonSilentError, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{0},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	_, err = storage.UpdateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		ID:             taskIDWithNonSilentError,
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     modifiedAt,
		GenerationID:   generationID,
		Status:         TaskStatusCancelled,
		ErrorCode:      grpc_codes.InvalidArgument,
		ErrorMessage:   "invalid argument",
		ErrorSilent:    false,
		State:          []byte{1},
	})
	require.NoError(t, err)

	expectedTasks, err := storage.ListFailedTasks(
		ctx,
		modifiedAt.Add(-10*time.Minute),
	)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]string{
			taskIDWithNonSilentError,
		},
		expectedTasks,
	)
}

func TestStorageYDBListSlowTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	storage, err := newStorage(
		t,
		ctx,
		db,
		&tasks_config.TasksConfig{
			TaskStallingTimeout: &taskStallingTimeout,
		},
		metricsRegistry,
	)
	require.NoError(t, err)
	createdAt := time.Now().Add(-3 * time.Hour)
	var generationID uint64 = 1

	createTask := func(created time.Time, estimated, durationMinutes int) TaskInfo {
		task := TaskState{
			IdempotencyKey: getIdempotencyKeyForTest(t),
			TaskType:       "task1",
			Description:    "some task",
			CreatedAt:      created,
			CreatedBy:      "some_user",
			ModifiedAt:     created,
			GenerationID:   generationID,
			Status:         TaskStatusFinished,
			State:          []byte{0},
			Dependencies:   common.NewStringSet(),
			EndedAt:        created.Add(time.Duration(durationMinutes) * time.Minute),
			EstimatedTime:  created.Add(time.Duration(estimated) * time.Minute),
		}
		id, err := storage.CreateTask(ctx, task)
		generationID += 1
		require.NoError(t, err)
		return TaskInfo{
			ID:           id,
			GenerationID: task.GenerationID,
			TaskType:     task.TaskType,
		}
	}
	createTask(createdAt, 5, 4) // Should not be presented in the result
	largeDelay := createTask(createdAt, 1, 61)
	mediumDelay := createTask(createdAt, 5, 64)
	smallDelay := createTask(createdAt, 60, 70)
	dayBefore := createTask(createdAt.Add(time.Duration(-24)*time.Hour), 5, 60)

	testCases := []struct {
		since        time.Time
		estimateMiss time.Duration
		expected     []string
		comment      string
	}{
		{createdAt.Add(-10 * time.Minute),
			200 * time.Minute,
			[]string{},
			"EstimateMiss is too big, no tasks expected",
		},
		{createdAt.Add(2 * time.Hour),
			10 * time.Minute,
			[]string{},
			"No tasks expected",
		},
		{createdAt,
			10 * time.Minute,
			[]string{largeDelay.ID, mediumDelay.ID, smallDelay.ID},
			"Since limited, 3 tasks expected",
		},
		{createdAt.Add(-10 * time.Minute),
			1 * time.Hour,
			[]string{largeDelay.ID},
			"EstimateMiss limited, one task expected",
		},
		{createdAt.Add(-25 * time.Hour),
			10 * time.Minute,
			[]string{largeDelay.ID, mediumDelay.ID, smallDelay.ID, dayBefore.ID},
			"All tasks fit, 4 expected",
		},
	}

	for _, tc := range testCases {
		tasks, err := storage.ListSlowTasks(ctx, tc.since, tc.estimateMiss)
		require.NoError(t, err)
		require.ElementsMatchf(t, tc.expected, tasks, tc.comment)
	}
}

func TestStorageYDBListTasksStallingWhileRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createTime := time.Now().Add(-time.Hour)
	freshTime := createTime.Add(2 * time.Hour)
	stallTime := createTime
	var generationID uint64 = 42

	taskIDReadyToRun, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDRunStalling, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastHost:       "other_host",
	})
	require.NoError(t, err)

	taskIDFinished, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusFinished,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDReadyToCancel, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelled,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToRun, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	taskIDRunStallingWaited, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToRun, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	expectedTaskInfos := []TaskInfo{
		TaskInfo{
			ID:           taskIDRunStalling,
			GenerationID: generationID,
			TaskType:     "task1",
		},
		TaskInfo{
			ID:           taskIDRunStallingWaited,
			GenerationID: generationID,
			TaskType:     "task1",
		},
	}

	taskInfos, err := storage.ListTasksStallingWhileRunning(
		ctx,
		"current_host",
		100500,
		nil, // taskTypeWhitelist
	)
	require.NoError(t, err)
	// TODO: Maybe we need a proper order.
	require.ElementsMatch(t, expectedTaskInfos, taskInfos)
}

func TestStorageYDBListTasksStallingWhileCancelling(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createTime := time.Now().Add(-time.Hour)
	freshTime := createTime.Add(2 * time.Hour)
	stallTime := createTime
	var generationID uint64 = 42

	taskIDReadyToRun, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDFinished, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusFinished,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDReadyToCancel, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDCancelStalling, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastHost:       "other_host",
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelled,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToRun, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToRun, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	taskIDCancelStallingWaited, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(taskIDFinished, taskIDReadyToCancel),
	})
	require.NoError(t, err)

	expectedTaskInfos := []TaskInfo{
		TaskInfo{
			ID:           taskIDCancelStalling,
			GenerationID: generationID,
			TaskType:     "task1",
		},
		TaskInfo{
			ID:           taskIDCancelStallingWaited,
			GenerationID: generationID,
			TaskType:     "task1",
		},
	}

	taskInfos, err := storage.ListTasksStallingWhileCancelling(
		ctx,
		"current_host",
		100500,
		nil, // taskTypeWhitelist
	)
	require.NoError(t, err)
	// TODO: Maybe we need a proper order.
	require.ElementsMatch(t, expectedTaskInfos, taskInfos)
}

func TestStorageYDBListTasksReadyToRunWithWhitelist(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createTime := time.Now().Add(-time.Hour)
	freshTime := createTime.Add(2 * time.Hour)
	var generationID uint64 = 42

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDReadyToRun2, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task2",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	expectedReadyToRunTaskInfos := []TaskInfo{
		{
			ID:           taskIDReadyToRun2,
			GenerationID: generationID,
			TaskType:     "task2",
		},
	}

	taskInfos, err := storage.ListTasksReadyToRun(
		ctx,
		100500,
		[]string{"task2"},
	)
	require.NoError(t, err)
	require.ElementsMatch(t, expectedReadyToRunTaskInfos, taskInfos)
}

func TestStorageYDBListTasksReadyToCancelWithWhitelist(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createTime := time.Now().Add(-time.Hour)
	freshTime := createTime.Add(2 * time.Hour)
	var generationID uint64 = 42

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDReadyToCancel2, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task2",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	expectedReadyToCancelTaskInfos := []TaskInfo{
		{
			ID:           taskIDReadyToCancel2,
			GenerationID: generationID,
			TaskType:     "task2",
		},
	}

	taskInfos, err := storage.ListTasksReadyToCancel(
		ctx,
		100500,
		[]string{"task2"},
	)
	require.NoError(t, err)
	require.ElementsMatch(t, expectedReadyToCancelTaskInfos, taskInfos)
}

func TestStorageYDBListTasksStallingWhileRunningWithWhitelist(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createTime := time.Now().Add(-time.Hour)
	stallTime := createTime
	var generationID uint64 = 42

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastHost:       "other_host",
	})
	require.NoError(t, err)

	taskIDRunStalling2, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task2",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastHost:       "other_host",
	})
	require.NoError(t, err)

	expectedStallingWhileRunningTaskInfos := []TaskInfo{
		{
			ID:           taskIDRunStalling2,
			GenerationID: generationID,
			TaskType:     "task2",
		},
	}

	taskInfos, err := storage.ListTasksStallingWhileRunning(
		ctx,
		"current_host",
		100500,
		[]string{"task2"},
	)
	require.NoError(t, err)
	require.ElementsMatch(t, expectedStallingWhileRunningTaskInfos, taskInfos)
}

func TestStorageYDBListTasksStallingWhileCancellingWithWhitelist(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createTime := time.Now().Add(-time.Hour)
	stallTime := createTime
	var generationID uint64 = 42

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastHost:       "other_host",
	})
	require.NoError(t, err)

	taskIDCancelStalling2, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task2",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastHost:       "other_host",
	})
	require.NoError(t, err)

	expectedStallingWhileCancellingTaskInfos := []TaskInfo{
		{
			ID:           taskIDCancelStalling2,
			GenerationID: generationID,
			TaskType:     "task2",
		},
	}

	taskInfos, err := storage.ListTasksStallingWhileCancelling(
		ctx,
		"current_host",
		100500,
		[]string{"task2"},
	)
	require.NoError(t, err)
	require.ElementsMatch(t, expectedStallingWhileCancellingTaskInfos, taskInfos)
}

func TestStorageYDBListTasksReadyToRunInCertainZone(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	zoneID := "zone1"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
		ZoneIds:             []string{zoneID},
	}, metricsRegistry)
	require.NoError(t, err)
	createTime := time.Now().Add(-time.Hour)
	freshTime := createTime.Add(2 * time.Hour)
	var generationID uint64 = 42

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		ZoneID:         "zone0",
	})
	require.NoError(t, err)

	taskIDReadyToRun, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		ZoneID:         "zone1",
	})
	require.NoError(t, err)

	expectedReadyToRunTaskInfos := []TaskInfo{
		{
			ID:           taskIDReadyToRun,
			GenerationID: generationID,
			TaskType:     "task1",
		},
	}

	taskInfos, err := storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, expectedReadyToRunTaskInfos, taskInfos)
}

func TestStorageYDBListTasksReadyToCancelInCertainZone(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	zoneID := "zone1"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
		ZoneIds:             []string{zoneID},
	}, metricsRegistry)
	require.NoError(t, err)
	createTime := time.Now().Add(-time.Hour)
	freshTime := createTime.Add(2 * time.Hour)
	var generationID uint64 = 42

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		ZoneID:         "zone0",
	})
	require.NoError(t, err)

	taskIDReadyToCancel, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     freshTime,
		GenerationID:   generationID,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		ZoneID:         "zone1",
	})
	require.NoError(t, err)

	expectedReadyToCancelTaskInfos := []TaskInfo{
		{
			ID:           taskIDReadyToCancel,
			GenerationID: generationID,
			TaskType:     "task1",
		},
	}

	taskInfos, err := storage.ListTasksReadyToCancel(ctx, 100500, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, expectedReadyToCancelTaskInfos, taskInfos)
}

func TestStorageYDBListTasksStallingWhileRunningInCertainZone(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	zoneID := "zone1"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
		ZoneIds:             []string{zoneID},
	}, metricsRegistry)
	require.NoError(t, err)
	createTime := time.Now().Add(-time.Hour)
	stallTime := createTime
	var generationID uint64 = 42

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastHost:       "other_host",
		ZoneID:         "zone0",
	})
	require.NoError(t, err)

	taskIDRunStalling, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastHost:       "other_host",
		ZoneID:         "zone1",
	})
	require.NoError(t, err)

	expectedStallingWhileRunningTaskInfos := []TaskInfo{
		{
			ID:           taskIDRunStalling,
			GenerationID: generationID,
			TaskType:     "task1",
		},
	}

	taskInfos, err := storage.ListTasksStallingWhileRunning(
		ctx,
		"current_host",
		100500,
		nil, // taskTypeWhitelist
	)
	require.NoError(t, err)
	require.ElementsMatch(t, expectedStallingWhileRunningTaskInfos, taskInfos)
}

func TestStorageYDBListTasksStallingWhileCancellingInCertainZone(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := empty.NewRegistry()

	taskStallingTimeout := "1s"
	zoneID := "zone1"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
		ZoneIds:             []string{zoneID},
	}, metricsRegistry)
	require.NoError(t, err)
	createTime := time.Now().Add(-time.Hour)
	stallTime := createTime
	var generationID uint64 = 42

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastHost:       "other_host",
		ZoneID:         "zone0",
	})
	require.NoError(t, err)

	taskIDCancelStalling, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createTime,
		CreatedBy:      "some_user",
		ModifiedAt:     stallTime,
		GenerationID:   generationID,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastHost:       "other_host",
		ZoneID:         "zone1",
	})
	require.NoError(t, err)

	expectedStallingWhileCancellingTaskInfos := []TaskInfo{
		{
			ID:           taskIDCancelStalling,
			GenerationID: generationID,
			TaskType:     "task1",
		},
	}

	taskInfos, err := storage.ListTasksStallingWhileCancelling(
		ctx,
		"current_host",
		100500,
		nil, // taskTypeWhitelist
	)
	require.NoError(t, err)
	require.ElementsMatch(t, expectedStallingWhileCancellingTaskInfos, taskInfos)
}

func TestStorageYDBLockTaskToRun(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	taskDuration := time.Minute

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastRunner:     "runner_42",
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Maybe().Panic("shouldn't create new task in LockTaskToRun")

	taskState, err := storage.LockTaskToRun(ctx, TaskInfo{
		ID:           taskID,
		GenerationID: 0,
	}, createdAt.Add(taskDuration), "host", "runner_43")
	require.NoError(t, err)
	require.EqualValues(t, taskState.GenerationID, 1)
	require.EqualValues(t, 0, taskState.StallingDuration)
	metricsRegistry.AssertAllExpectations(t)
}

func TestStorageYDBLockTaskToRunWrongGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	taskDuration := time.Minute

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   1,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	_, err = storage.LockTaskToRun(ctx, TaskInfo{
		ID:           taskID,
		GenerationID: 0,
	}, createdAt.Add(taskDuration), "host", "runner_42")
	require.Truef(t, errors.Is(err, errors.NewWrongGenerationError()), "Expected WrongGenerationError got %v", err)
	metricsRegistry.AssertAllExpectations(t)
}

func TestStorageYDBLockTaskToRunWrongState(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	taskDuration := time.Minute

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	_, err = storage.LockTaskToRun(ctx, TaskInfo{
		ID:           taskID,
		GenerationID: 0,
	}, createdAt.Add(taskDuration), "host", "runner_42")
	require.Error(t, err)
	require.Truef(t, !errors.Is(err, errors.NewWrongGenerationError()), "Unexpected WrongGenerationError")
	metricsRegistry.AssertAllExpectations(t)
}

func TestStorageYDBLockTaskToCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	taskDuration := time.Minute

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToCancel,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastRunner:     "runner_42",
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Maybe().Panic("shouldn't create new task in LockTaskToRun")

	taskState, err := storage.LockTaskToCancel(ctx, TaskInfo{
		ID:           taskID,
		GenerationID: 0,
	}, createdAt.Add(taskDuration), "host", "runner_43")
	require.NoError(t, err)
	require.EqualValues(t, taskState.GenerationID, 1)
	require.EqualValues(t, 0, taskState.StallingDuration)
	metricsRegistry.AssertAllExpectations(t)
}

func TestStorageYDBLockTaskToCancelWrongGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	taskDuration := time.Minute

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   1,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	_, err = storage.LockTaskToCancel(ctx, TaskInfo{
		ID:           taskID,
		GenerationID: 0,
	}, createdAt.Add(taskDuration), "host", "runner_42")
	require.Truef(t, errors.Is(err, errors.NewWrongGenerationError()), "Expected WrongGenerationError got %v", err)
	metricsRegistry.AssertAllExpectations(t)
}

func TestStorageYDBLockTaskToCancelWrongState(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	taskDuration := time.Minute

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	_, err = storage.LockTaskToCancel(ctx, TaskInfo{
		ID:           taskID,
		GenerationID: 0,
	}, createdAt.Add(taskDuration), "host", "runner_42")
	require.Error(t, err)
	require.Truef(t, !errors.Is(err, errors.NewWrongGenerationError()), "Unexpected WrongGenerationError")
	metricsRegistry.AssertAllExpectations(t)
}

func TestStorageYDBMarkForCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	taskDuration := time.Minute

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastRunner:     "runner_42",
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	cancelling, err := storage.MarkForCancellation(ctx, taskID, createdAt.Add(taskDuration))
	require.NoError(t, err)
	require.True(t, cancelling)
	metricsRegistry.AssertAllExpectations(t)

	taskState, err := storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.EqualValues(t, taskState.GenerationID, 1)
	require.EqualValues(t, taskState.Status, TaskStatusReadyToCancel)
	require.EqualValues(t, taskState.ErrorCode, grpc_codes.Canceled)
	require.EqualValues(t, taskState.ErrorMessage, "Cancelled by client")
	require.EqualValues(t, 0, taskState.WaitingDuration)
}

func TestStorageYDBMarkForCancellationIfAlreadyCancelling(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	taskDuration := time.Minute

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	cancelling, err := storage.MarkForCancellation(ctx, taskID, createdAt.Add(taskDuration))
	require.NoError(t, err)
	require.True(t, cancelling)

	metricsRegistry.AssertAllExpectations(t)

	taskState, err := storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.EqualValues(t, taskState.GenerationID, 0)
	require.Equal(t, taskState.Status, TaskStatusCancelling)
	require.EqualValues(t, 0, taskState.WaitingDuration)
}

func TestStorageYDBMarkForCancellationIfAlreadyFinished(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	taskDuration := time.Minute

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusFinished,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	cancelling, err := storage.MarkForCancellation(ctx, taskID, createdAt.Add(taskDuration))
	require.NoError(t, err)
	require.False(t, cancelling)
	metricsRegistry.AssertAllExpectations(t)

	taskState, err := storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.EqualValues(t, taskState.GenerationID, 0)
	require.Equal(t, taskState.Status, TaskStatusFinished)
	require.EqualValues(t, 0, taskState.WaitingDuration)
}

func TestStorageYDBMarkForCancellationIfAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	taskDuration := time.Minute

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusCancelled,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	cancelling, err := storage.MarkForCancellation(ctx, taskID, createdAt.Add(taskDuration))
	require.NoError(t, err)
	require.True(t, cancelling)
	metricsRegistry.AssertAllExpectations(t)

	taskState, err := storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.EqualValues(t, taskState.GenerationID, 0)
	require.Equal(t, taskState.Status, TaskStatusCancelled)
	require.EqualValues(t, 0, taskState.WaitingDuration)
}

func TestStorageYDBMarkForCancellationWhileWaitingToRun(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	taskDuration := time.Minute

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusWaitingToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	cancelling, err := storage.MarkForCancellation(ctx, taskID, createdAt.Add(taskDuration))
	require.NoError(t, err)
	require.True(t, cancelling)
	metricsRegistry.AssertAllExpectations(t)

	taskState, err := storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.EqualValues(t, taskState.GenerationID, 1)
	require.Equal(t, taskState.Status, TaskStatusReadyToCancel)
	require.Equal(t, taskDuration, taskState.WaitingDuration)
}

func TestStorageYDBUpdateTask(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	modifiedAt := createdAt.Add(time.Minute)

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Times(3)

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{0},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDDependent1, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskIDDependent2, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusFinished,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	_, err = storage.UpdateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		ID:             taskID,
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     modifiedAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		ErrorCode:      grpc_codes.InvalidArgument,
		ErrorMessage:   "invalid argument",
		State:          []byte{1},
		Dependencies:   common.NewStringSet(taskIDDependent1, taskIDDependent2),
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	taskState, err := storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.EqualValues(t, taskState.Status, TaskStatusReadyToRun)
	require.EqualValues(t, taskState.GenerationID, 0)
	require.WithinDuration(t, time.Time(taskState.CreatedAt), time.Time(createdAt), time.Microsecond)
	require.WithinDuration(t, time.Time(taskState.ModifiedAt), time.Time(modifiedAt), time.Microsecond)
	require.EqualValues(t, taskState.ErrorCode, grpc_codes.InvalidArgument)
	require.EqualValues(t, taskState.ErrorMessage, "invalid argument")
	require.EqualValues(t, taskState.State, []byte{1})
	require.ElementsMatch(t, taskState.Dependencies.List(), []string{taskIDDependent1})
	require.WithinDuration(t, time.Time(taskState.ChangedStateAt), time.Time(createdAt), time.Microsecond)

	// Updating with the same stuff shouldn't change a thing.
	_, err = storage.UpdateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		ID:             taskID,
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     modifiedAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		ErrorCode:      grpc_codes.InvalidArgument,
		ErrorMessage:   "invalid argument",
		State:          []byte{1},
		Dependencies:   common.NewStringSet(taskIDDependent1, taskIDDependent2),
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	taskState, err = storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.EqualValues(t, taskState.Status, TaskStatusReadyToRun)
	require.EqualValues(t, taskState.GenerationID, 0)
	require.WithinDuration(t, time.Time(taskState.CreatedAt), time.Time(createdAt), time.Microsecond)
	require.WithinDuration(t, time.Time(taskState.ModifiedAt), time.Time(modifiedAt), time.Microsecond)
	require.EqualValues(t, taskState.ErrorCode, grpc_codes.InvalidArgument)
	require.EqualValues(t, taskState.ErrorMessage, "invalid argument")
	require.EqualValues(t, taskState.State, []byte{1})
	require.ElementsMatch(t, taskState.Dependencies.List(), []string{taskIDDependent1})
	require.WithinDuration(t, time.Time(taskState.ChangedStateAt), time.Time(createdAt), time.Microsecond)
}

func TestStorageYDBUpdateTaskStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	firstUpdatePause := time.Minute
	secondUpdatePause := time.Hour

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{0},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	_, err = storage.UpdateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		ID:             taskID,
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt.Add(firstUpdatePause),
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{1},
		Dependencies:   common.NewStringSet(),
		LastRunner:     "runner_42",
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	taskState, err := storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.EqualValues(t, taskState.GenerationID, 0)
	require.Equal(t, taskState.Status, TaskStatusReadyToRun)
	require.WithinDuration(t, time.Time(taskState.CreatedAt), time.Time(taskState.ChangedStateAt), time.Microsecond)

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Maybe().Panic("shouldn't create new task in UpdateTask")

	_, err = storage.UpdateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		ID:             taskID,
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt.Add(firstUpdatePause + secondUpdatePause),
		GenerationID:   0,
		Status:         TaskStatusRunning,
		State:          []byte{2},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	taskState, err = storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.EqualValues(t, taskState.GenerationID, 1)
	require.Equal(t, taskState.Status, TaskStatusRunning)
	require.WithinDuration(t, time.Time(taskState.ModifiedAt), time.Time(taskState.ChangedStateAt), time.Microsecond)
}

func TestStorageYDBUpdateTaskWrongGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   2,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	_, err = storage.UpdateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		ID:             taskID,
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusRunning,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.Truef(t, errors.Is(err, errors.NewWrongGenerationError()), "Expected WrongGenerationError got %v", err)
	metricsRegistry.AssertAllExpectations(t)

	taskState, err := storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.EqualValues(t, taskState.GenerationID, 2)
	require.Equal(t, taskState.Status, TaskStatusReadyToRun)
}

func TestStorageYDBLockInParallel(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	taskDuration := time.Minute

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastRunner:     "runner_42",
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Maybe().Panic("shouldn't create new task in LockTaskToRun")

	var successCounter uint64
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := storage.LockTaskToRun(ctx, TaskInfo{
				ID:           taskID,
				GenerationID: 0,
			}, createdAt.Add(taskDuration), "host", "runner_43")
			if err == nil {
				atomic.AddUint64(&successCounter, 1)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, successCounter, 1)
	metricsRegistry.AssertAllExpectations(t)
}

func TestStorageYDBMarkForCancellationInParallel(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	taskDuration := time.Minute

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastRunner:     "runner_42",
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	var successCounter uint64
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := storage.MarkForCancellation(ctx, taskID, createdAt.Add(taskDuration))
			if err == nil {
				atomic.AddUint64(&successCounter, 1)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 100, successCounter)
	metricsRegistry.AssertAllExpectations(t)
}

func TestStorageYDBLockAlreadyCancellingTask(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()
	taskDuration := time.Minute

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	var taskState TaskState
	taskState, err = storage.LockTaskToCancel(ctx, TaskInfo{
		ID:           taskID,
		GenerationID: 0,
	}, createdAt.Add(taskDuration), "host", "runner_42")
	require.NoError(t, err)
	require.EqualValues(t, taskState.GenerationID, 1)

	taskState, err = storage.LockTaskToCancel(ctx, TaskInfo{
		ID:           taskID,
		GenerationID: 0,
	}, createdAt.Add(taskDuration), "host", "runner_42")
	require.Truef(t, errors.Is(err, errors.NewWrongGenerationError()), "Expected WrongGenerationError got %v", err)
}

func TestStorageYDBCheckStallingTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	_, err = storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusCancelling,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastHost:       "other_host",
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	time.Sleep(time.Second)

	tasks, err := storage.ListTasksStallingWhileCancelling(
		ctx,
		"current_host",
		100500,
		nil, // taskTypeWhitelist
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))
}

func TestStorageYDBCreateRegularTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)

	createdAt := time.Now()
	task := TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		LastRunner:     "runner",
		LastHost:       "host",
	}

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": task.TaskType},
	).On("Add", int64(2)).Once()

	schedule := TaskSchedule{
		ScheduleInterval: time.Second,
		MaxTasksInflight: 2,
	}

	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	taskInfos, err := storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(taskInfos))

	task1, err := storage.GetTask(ctx, taskInfos[0].ID)
	require.NoError(t, err)
	require.Equal(t, task1.TaskType, "task")
	require.True(t, task1.Regular)

	task2, err := storage.GetTask(ctx, taskInfos[1].ID)
	require.NoError(t, err)
	require.Equal(t, task2.TaskType, "task")
	require.True(t, task2.Regular)

	// Check idempotency.
	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)

	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(taskInfos))

	// Rewind time and check that new tasks were not created.
	task.CreatedAt = createdAt.Add(2 * time.Second)
	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)

	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(taskInfos))

	// Finish first task and check that new tasks were not created.
	task1.Status = TaskStatusFinished
	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": task.TaskType},
	).On("Add", int64(1)).Maybe().Panic("shouldn't create new task in UpdateTask")
	metricsRegistry.GetTimer(
		"time/total",
		map[string]string{"type": task.TaskType},
	).On("RecordDuration", mock.Anything).Once()

	_, err = storage.UpdateTask(ctx, task1)
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)

	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(taskInfos))

	task.CreatedAt = createdAt

	// Finish second task and check that new tasks were not created.
	task2.Status = TaskStatusFinished
	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": task.TaskType},
	).On("Add", int64(1)).Maybe().Panic("shouldn't create new task in UpdateTask")
	metricsRegistry.GetTimer(
		"time/total",
		map[string]string{"type": task.TaskType},
	).On("RecordDuration", mock.Anything).Once()

	_, err = storage.UpdateTask(ctx, task2)
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)

	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(taskInfos))

	// Rewind time and check that new tasks were created.
	task.CreatedAt = createdAt.Add(time.Second + time.Millisecond)

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": task.TaskType},
	).On("Add", int64(2)).Once()

	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(taskInfos))
}

func TestStorageYDBCreateRegularTasksUsingCrontab(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)

	day := 24 * time.Hour

	createdAt := time.Date(2024, 1, 26, 19, 41, 59, 0, time.Local)
	task := TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		LastRunner:     "runner",
		LastHost:       "host",
	}

	schedule := TaskSchedule{
		MaxTasksInflight: 2,

		UseCrontab: true,
		Hour:       19,
		Min:        42,
	}

	// No tasks should be created (the time has not come yet).
	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)

	taskInfos, err := storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(taskInfos))

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": task.TaskType},
	).On("Add", int64(2)).Once()

	// Rewind time and check that initial tasks were created.
	task.CreatedAt = createdAt.Add(2 * time.Second)
	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(taskInfos))

	task1, err := storage.GetTask(ctx, taskInfos[0].ID)
	require.NoError(t, err)
	require.Equal(t, task1.TaskType, "task")
	require.True(t, task1.Regular)

	task2, err := storage.GetTask(ctx, taskInfos[1].ID)
	require.NoError(t, err)
	require.Equal(t, task2.TaskType, "task")
	require.True(t, task2.Regular)

	// Check idempotency.
	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)

	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(taskInfos))

	// Rewind time and check that new tasks were not created.
	task.CreatedAt = createdAt.Add(day + 2*time.Second)
	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)

	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(taskInfos))

	// Finish first task and check that new tasks were not created.
	task1.Status = TaskStatusFinished
	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": task.TaskType},
	).On("Add", int64(1)).Maybe().Panic("shouldn't create new task in UpdateTask")
	metricsRegistry.GetTimer(
		"time/total",
		map[string]string{"type": task.TaskType},
	).On("RecordDuration", mock.Anything).Once()

	_, err = storage.UpdateTask(ctx, task1)
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)

	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(taskInfos))

	// Rewind time and check that new tasks were not created.
	task.CreatedAt = createdAt.Add(day - time.Second)

	// Finish second task and check that new tasks were not created.
	task2.Status = TaskStatusFinished
	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": task.TaskType},
	).On("Add", int64(1)).Maybe().Panic("shouldn't create new task in UpdateTask")
	metricsRegistry.GetTimer(
		"time/total",
		map[string]string{"type": task.TaskType},
	).On("RecordDuration", mock.Anything).Once()

	_, err = storage.UpdateTask(ctx, task2)
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)

	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(taskInfos))

	// Rewind time and check that new tasks were created.
	task.CreatedAt = createdAt.Add(day + time.Second)

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": task.TaskType},
	).On("Add", int64(2)).Once()

	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(taskInfos))
}

func TestStorageYDBCreateRegularTasksUsingCrontabInNewMonthOrYear(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)

	day := 24 * time.Hour

	createdAt := time.Date(2024, 2, 29, 19, 42, 0, 0, time.Local)
	task := TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		LastRunner:     "runner",
		LastHost:       "host",
	}

	schedule := TaskSchedule{
		MaxTasksInflight: 1,

		UseCrontab: true,
		Hour:       19,
		Min:        42,
	}

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": task.TaskType},
	).On("Add", int64(1)).Once()

	// Check that initial task was created.
	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	taskInfos, err := storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(taskInfos))

	actual, err := storage.GetTask(ctx, taskInfos[0].ID)
	require.NoError(t, err)

	// Finish task and check that new tasks were not created.
	actual.Status = TaskStatusFinished
	metricsRegistry.GetTimer(
		"time/total",
		map[string]string{"type": task.TaskType},
	).On("RecordDuration", mock.Anything).Once()

	_, err = storage.UpdateTask(ctx, actual)
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)

	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(taskInfos))

	// Rewind time by 1 day and check that new task was created.
	task.CreatedAt = createdAt.Add(day + time.Second)

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": task.TaskType},
	).On("Add", int64(1)).Once()

	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	// Next month has come, new task should be created.
	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(taskInfos))

	actual, err = storage.GetTask(ctx, taskInfos[0].ID)
	require.NoError(t, err)

	// Finish task and check that new tasks were not created.
	actual.Status = TaskStatusFinished
	metricsRegistry.GetTimer(
		"time/total",
		map[string]string{"type": task.TaskType},
	).On("RecordDuration", mock.Anything).Once()

	_, err = storage.UpdateTask(ctx, actual)
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(taskInfos))

	// Rewind time by 1 year and check that new task was created.
	task.CreatedAt = createdAt.Add(365 * day)

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": task.TaskType},
	).On("Add", int64(1)).Once()

	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	// Next year has come, new task should be created.
	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(taskInfos))
}

func TestStorageYDBRegularTaskShouldNotBeCreatedIfPreviousCrontabTaskIsNotFinished(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)

	day := 24 * time.Hour

	createdAt := time.Date(2024, 1, 26, 19, 41, 59, 0, time.Local)
	task := TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		LastRunner:     "runner",
		LastHost:       "host",
	}

	schedule := TaskSchedule{
		MaxTasksInflight: 1,

		UseCrontab: true,
		Hour:       19,
		Min:        42,
	}

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": task.TaskType},
	).On("Add", int64(1)).Once()

	// Rewind time and check that initial task was created.
	task.CreatedAt = createdAt.Add(2 * time.Second)
	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)

	taskInfos, err := storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(taskInfos))

	currTask, err := storage.GetTask(ctx, taskInfos[0].ID)
	require.NoError(t, err)

	// Rewind time and check that new tasks were not created.
	task.CreatedAt = createdAt.Add(day + 2*time.Second)
	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)

	taskInfos, err = storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(taskInfos))
	require.Equal(t, currTask.ID, taskInfos[0].ID)

	// Should be able to finish task successfully.
	currTask.Status = TaskStatusFinished
	metricsRegistry.GetTimer(
		"time/total",
		map[string]string{"type": task.TaskType},
	).On("RecordDuration", mock.Anything).Once()

	_, err = storage.UpdateTask(ctx, currTask)
	require.NoError(t, err)
}

func TestStorageYDBCancelRegularTask(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)

	createdAt := time.Now()
	task := TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		LastRunner:     "runner",
		LastHost:       "host",
	}

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": task.TaskType},
	).On("Add", int64(1)).Once()

	schedule := TaskSchedule{
		ScheduleInterval: time.Second,
		MaxTasksInflight: 1,
	}

	err = storage.CreateRegularTasks(ctx, task, schedule)
	require.NoError(t, err)

	taskInfos, err := storage.ListTasksReadyToRun(ctx, 100500, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(taskInfos))

	taskID := taskInfos[0].ID

	taskState, err := storage.GetTask(ctx, taskID)
	require.NoError(t, err)

	_, err = storage.MarkForCancellation(ctx, taskID, time.Now())
	require.NoError(t, err)

	taskState, err = storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, TaskStatusReadyToCancel, taskState.Status)

	generationID := taskState.GenerationID

	// Check idempotency, expect that generation and status are unchanged.
	_, err = storage.MarkForCancellation(ctx, taskID, time.Now())
	require.NoError(t, err)
	taskState, err = storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, TaskStatusReadyToCancel, taskState.Status)
	require.Equal(t, generationID, taskState.GenerationID)

	// Lock task for cancellation, expect that generation and status are changed.
	taskState, err = storage.LockTaskToCancel(ctx, TaskInfo{
		ID:           taskID,
		GenerationID: generationID,
	}, time.Now(), "host", "runner_43")
	require.NoError(t, err)
	require.EqualValues(t, generationID+1, taskState.GenerationID)

	// Check idempotency, expect that generation and status are unchanged.
	_, err = storage.MarkForCancellation(ctx, taskID, time.Now())
	require.NoError(t, err)
	taskState, err = storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, TaskStatusCancelling, taskState.Status)
	require.Equal(t, generationID+1, taskState.GenerationID)

	// End task, expect that generation and status are changed.
	taskState.Status = TaskStatusCancelled
	_, err = storage.UpdateTask(ctx, taskState)
	require.NoError(t, err)
	taskState, err = storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, TaskStatusCancelled, taskState.Status)
	require.Equal(t, generationID+2, taskState.GenerationID)

	// Check idempotency, expect that generation and status are unchanged.
	_, err = storage.MarkForCancellation(ctx, taskID, time.Now())
	require.NoError(t, err)
	taskState, err = storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, TaskStatusCancelled, taskState.Status)
	require.Equal(t, generationID+2, taskState.GenerationID)
}

func TestStorageYDBClearEndedTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task"},
	).On("Add", int64(1)).Times(4)
	metricsRegistry.GetTimer(
		"time/total",
		map[string]string{"type": "task"},
	).On("RecordDuration", mock.Anything).Twice()

	taskID1, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{0},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	task := TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	}
	taskID2, err := storage.CreateTask(ctx, task)
	require.NoError(t, err)
	task.ID = taskID2

	task.Status = TaskStatusCancelled
	_, err = storage.UpdateTask(ctx, task)
	require.NoError(t, err)

	task = TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	}

	taskID3, err := storage.CreateTask(ctx, task)
	require.NoError(t, err)
	task.ID = taskID3

	task.Status = TaskStatusFinished
	_, err = storage.UpdateTask(ctx, task)
	require.NoError(t, err)

	endedBefore := createdAt.Add(time.Microsecond)
	createdAt = createdAt.Add(2 * time.Microsecond)

	task = TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	}

	taskID4, err := storage.CreateTask(ctx, task)
	require.NoError(t, err)
	task.ID = taskID4

	task.Status = TaskStatusFinished
	_, err = storage.UpdateTask(ctx, task)
	require.NoError(t, err)

	metricsRegistry.AssertAllExpectations(t)

	err = storage.ClearEndedTasks(ctx, endedBefore, 100500)
	require.NoError(t, err)

	_, err = storage.GetTask(ctx, taskID1)
	require.NoError(t, err)

	_, err = storage.GetTask(ctx, taskID2)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.Contains(t, err.Error(), "No task")

	_, err = storage.GetTask(ctx, taskID3)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.Contains(t, err.Error(), "No task")

	_, err = storage.GetTask(ctx, taskID4)
	require.NoError(t, err)
}

func TestStorageYDBPauseResumeTask(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now()

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		LastRunner:     "runner_42",
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	err = storage.PauseTask(ctx, taskID)
	require.NoError(t, err)

	task, err := storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, task.Status, TaskStatusWaitingToRun)
	require.EqualValues(t, task.GenerationID, 1)

	// Check idempotency.
	err = storage.PauseTask(ctx, taskID)
	require.NoError(t, err)

	task, err = storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, task.Status, TaskStatusWaitingToRun)
	require.EqualValues(t, task.GenerationID, 1)

	err = storage.ResumeTask(ctx, taskID)
	require.NoError(t, err)

	// Check idempotency.
	err = storage.ResumeTask(ctx, taskID)
	require.NoError(t, err)

	task, err = storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, task.Status, TaskStatusReadyToRun)
	require.EqualValues(t, task.GenerationID, 2)

	task.Status = TaskStatusFinished
	metricsRegistry.GetTimer(
		"time/total",
		map[string]string{"type": task.TaskType},
	).On("RecordDuration", mock.Anything).Once()

	_, err = storage.UpdateTask(ctx, task)
	require.NoError(t, err)

	err = storage.PauseTask(ctx, taskID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid status")

	task, err = storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, task.Status, TaskStatusFinished)
	require.EqualValues(t, task.GenerationID, 3)

	err = storage.ResumeTask(ctx, taskID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid status")

	metricsRegistry.AssertAllExpectations(t)
}

// Test that the first hearbeat adds the node to the table.
func TestHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{}, empty.NewRegistry())
	require.NoError(t, err)

	err = storage.HeartbeatNode(ctx, "host-1", time.Now(), 1)
	require.NoError(t, err)

	node, err := storage.GetNode(ctx, "host-1")
	require.NoError(t, err)

	require.Equal(t, "host-1", node.Host)
	require.Equal(t, uint32(1), node.InflightTaskCount)
}

// Test that the following heartbeats overwrite the previous state.
func TestMultipleHeartbeats(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{}, empty.NewRegistry())
	require.NoError(t, err)

	err = storage.HeartbeatNode(ctx, "host-1", time.Now(), 1)
	require.NoError(t, err)

	initialNode, err := storage.GetNode(ctx, "host-1")
	require.NoError(t, err)

	err = storage.HeartbeatNode(ctx, "host-1", time.Now(), 2)
	require.NoError(t, err)

	node, err := storage.GetNode(ctx, "host-1")
	require.NoError(t, err)
	require.Equal(t, "host-1", node.Host)
	require.Equal(t, uint32(2), node.InflightTaskCount)
	require.True(t, node.LastHeartbeat.After(initialNode.LastHeartbeat))
}

// Test fetching of the nodes that have sent heartbeats within the interval.
func TestGetAliveNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{}, empty.NewRegistry())
	require.NoError(t, err)

	referenceTime := time.Now().UTC()

	registerNodeInThePast := func(host string, offset time.Duration) {
		err = storage.HeartbeatNode(ctx, host, referenceTime.Add(-offset), 1)
		require.NoError(t, err)
	}

	registerNodeInThePast("host-1", 5*time.Second)
	registerNodeInThePast("host-2", 10*time.Second)
	registerNodeInThePast("host-3", 35*time.Second)

	nodes, err := storage.GetAliveNodes(ctx)
	require.NoError(t, err)

	// Reformat timestamp to get the same format.
	for idx := range nodes {
		nodes[idx].LastHeartbeat = nodes[idx].LastHeartbeat.UTC().Round(time.Second)
	}

	require.ElementsMatch(t,
		[]Node{
			{
				Host:              "host-1",
				LastHeartbeat:     referenceTime.Add(-5 * time.Second).Round(time.Second),
				InflightTaskCount: 1,
			},
			{
				Host:              "host-2",
				LastHeartbeat:     referenceTime.Add(-10 * time.Second).Round(time.Second),
				InflightTaskCount: 1,
			},
		},
		nodes,
	)
}

func TestForceFinishTaskWithDependencies(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "dep_task"},
	).On("Add", int64(1)).Twice()
	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	initialWaitingDuration := 42 * time.Minute
	taskDuration := time.Hour
	createdAt := time.Now().Add(-taskDuration)

	depID1, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "dep_task",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   42,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	depID2, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey: getIdempotencyKeyForTest(t),
		TaskType:       "dep_task",
		Description:    "Some task",
		CreatedAt:      createdAt,
		CreatedBy:      "some_user",
		ModifiedAt:     createdAt,
		GenerationID:   42,
		Status:         TaskStatusReadyToRun,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey:  getIdempotencyKeyForTest(t),
		TaskType:        "task1",
		Description:     "Some task",
		CreatedAt:       createdAt,
		CreatedBy:       "some_user",
		ModifiedAt:      createdAt,
		GenerationID:    42,
		Status:          TaskStatusReadyToRun,
		State:           []byte{1, 2, 3},
		Dependencies:    common.NewStringSet(depID1, depID2),
		WaitingDuration: initialWaitingDuration,
	})
	require.NoError(t, err)

	err = storage.PauseTask(ctx, taskID)
	require.NoError(t, err)

	task, err := storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, task.Status, TaskStatusWaitingToRun)
	require.Equal(t, task.WaitingDuration, initialWaitingDuration)

	// Force finish first dependency and make sure task still sleeping
	err = storage.ForceFinishTask(ctx, depID1)
	require.NoError(t, err)

	task, err = storage.GetTask(ctx, depID1)
	require.NoError(t, err)
	require.Equal(t, task.Status, TaskStatusFinished)
	require.Equal(t, task.WaitingDuration, time.Duration(0))

	task, err = storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, task.Status, TaskStatusWaitingToRun)
	require.Equal(t, task.WaitingDuration, initialWaitingDuration)

	// Force finish second dependency and make sure task ready to run
	err = storage.ForceFinishTask(ctx, depID2)
	require.NoError(t, err)

	task, err = storage.GetTask(ctx, depID2)
	require.NoError(t, err)
	require.Equal(t, task.Status, TaskStatusFinished)
	require.Equal(t, task.WaitingDuration, time.Duration(0))

	task, err = storage.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, task.Status, TaskStatusReadyToRun)

	// Make sure that WaitingDuration is almost correct
	expectedDuration := initialWaitingDuration + taskDuration
	threshold := 2 * time.Second
	require.InDelta(t, expectedDuration, task.WaitingDuration, float64(threshold))
}

func testStallingDurationAccumulatesOnStalkerRun(t *testing.T, taskStatus TaskStatus) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry := mocks.NewRegistryMock()

	taskStallingTimeout := "1s"
	storage, err := newStorage(t, ctx, db, &tasks_config.TasksConfig{
		TaskStallingTimeout: &taskStallingTimeout,
	}, metricsRegistry)
	require.NoError(t, err)
	createdAt := time.Now().Truncate(time.Microsecond) // YDB truncates time up to 1 microsecond
	taskDuration := time.Minute
	stallingDuration := 42 * time.Second
	locksCount := 5

	metricsRegistry.GetCounter(
		"created",
		map[string]string{"type": "task1"},
	).On("Add", int64(1)).Once()

	taskID, err := storage.CreateTask(ctx, TaskState{
		IdempotencyKey:   getIdempotencyKeyForTest(t),
		TaskType:         "task1",
		Description:      "Some task",
		CreatedAt:        createdAt,
		CreatedBy:        "some_user",
		ModifiedAt:       createdAt,
		GenerationID:     0,
		Status:           taskStatus,
		State:            []byte{},
		Dependencies:     NewStringSet(),
		LastRunner:       "runner_42",
		StallingDuration: stallingDuration,
	})
	require.NoError(t, err)
	metricsRegistry.AssertAllExpectations(t)

	for i := 0; i < locksCount; i++ {
		function := storage.LockTaskToRun
		if taskStatus == TaskStatusCancelling {
			function = storage.LockTaskToCancel
		}
		taskState, err := function(ctx, TaskInfo{
			ID:           taskID,
			GenerationID: uint64(i),
		}, createdAt.Add(time.Duration(i+1)*taskDuration), "host", "runner_43")
		require.NoError(t, err)
		require.Equal(t,
			stallingDuration+time.Duration(i+1)*taskDuration,
			taskState.StallingDuration)
	}
}

func TestStallingDurationAccumulatesOnStalkerRun(t *testing.T) {
	testStallingDurationAccumulatesOnStalkerRun(t, TaskStatusRunning)
}

func TestStallingDurationAccumulatesOnStalkerCancel(t *testing.T) {
	testStallingDurationAccumulatesOnStalkerRun(t, TaskStatusCancelling)
}
