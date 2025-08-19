package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks"
	node_config "github.com/ydb-platform/nbs/cloud/tasks/acceptance_tests/recipe/node/config"
	recipe_tasks "github.com/ydb-platform/nbs/cloud/tasks/acceptance_tests/recipe/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/acceptance_tests/recipe/tasks/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	metrics_empty "github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	logger := logging.NewStderrLogger(logging.DebugLevel)
	return logging.SetLogger(ctx, logger), cancel
}

var lastReqNumber int

func getRequestContext(t *testing.T, ctx context.Context) context.Context {
	lastReqNumber++

	cookie := fmt.Sprintf("%v_%v", t.Name(), lastReqNumber)
	ctx = headers.SetIncomingIdempotencyKey(ctx, cookie)
	ctx = headers.SetIncomingRequestID(ctx, cookie)
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

type client struct {
	db        *persistence.YDBClient
	scheduler tasks.Scheduler
}

func newClient(ctx context.Context) (*client, error) {
	var config node_config.Config
	configString := os.Getenv("CLOUD_TASKS_ACCEPTANCE_TESTS_RECIPE_NODE0_CONFIG")

	if len(configString) == 0 {
		return nil, fmt.Errorf("node config should not be empty")
	}

	err := proto.UnmarshalText(configString, &config)
	if err != nil {
		return nil, err
	}

	db, err := persistence.NewYDBClient(
		ctx,
		config.PersistenceConfig,
		metrics_empty.NewRegistry(),
	)
	if err != nil {
		return nil, err
	}

	storage, err := tasks_storage.NewStorage(
		config.TasksConfig,
		metrics_empty.NewRegistry(),
		db,
	)
	if err != nil {
		return nil, err
	}

	registry := tasks.NewRegistry()

	scheduler, err := tasks.NewScheduler(
		ctx,
		registry,
		storage,
		config.TasksConfig,
		metrics_empty.NewRegistry(),
	)
	if err != nil {
		return nil, err
	}

	err = registry.RegisterForExecution("ChainTask", func() tasks.Task {
		return &recipe_tasks.ChainTask{}
	})
	if err != nil {
		return nil, err
	}

	return &client{
		db:        db,
		scheduler: scheduler,
	}, nil
}

func (c *client) Close(ctx context.Context) error {
	return c.db.Close(ctx)
}

func (c *client) execute(f func() error) error {
	for {
		err := f()
		if err != nil {
			if errors.CanRetry(err) {
				<-time.After(time.Second)
				continue
			}

			return err
		}

		return nil
	}
}

func (c *client) scheduleChain(
	t *testing.T,
	ctx context.Context,
	depth int,
) (string, error) {

	reqCtx := getRequestContext(t, ctx)

	var taskID string

	err := c.execute(func() error {
		var err error
		taskID, err = c.scheduler.ScheduleTask(
			reqCtx,
			"ChainTask",
			"",
			&protos.ChainTaskRequest{
				Depth: uint32(depth),
			},
		)
		return err
	})
	return taskID, err
}

func (c *client) cancelTask(
	ctx context.Context,
	taskID string,
) error {

	return c.execute(func() error {
		_, err := c.scheduler.CancelTask(ctx, taskID)
		return err
	})
}

////////////////////////////////////////////////////////////////////////////////

var defaultTimeout = 40 * time.Minute

func (c *client) waitTask(
	ctx context.Context,
	taskID string,
) error {

	_, err := c.scheduler.WaitTaskSync(ctx, taskID, defaultTimeout)
	return err
}

func (c *client) waitTaskEnded(
	ctx context.Context,
	taskID string,
) error {

	return c.execute(func() error {
		return c.scheduler.WaitTaskEnded(ctx, taskID)
	})
}

////////////////////////////////////////////////////////////////////////////////

func TestTasksAcceptanceRunChains(t *testing.T) {
	ctx, cancel := newContext()
	defer cancel()

	client, err := newClient(ctx)
	require.NoError(t, err)
	defer client.Close(ctx)

	tasks := make([]string, 0)

	for i := 0; i < 30; i++ {
		taskID, err := client.scheduleChain(t, ctx, 2)
		require.NoError(t, err)
		require.NotEmpty(t, taskID)

		tasks = append(tasks, taskID)
	}

	errs := make(chan error)

	for _, taskID := range tasks {
		go func(taskID string) {
			errs <- client.waitTask(ctx, taskID)
		}(taskID)
	}

	for range tasks {
		err := <-errs
		require.NoError(t, err)
	}
}

func TestTasksAcceptanceCancelChains(t *testing.T) {
	ctx, cancel := newContext()
	defer cancel()

	client, err := newClient(ctx)
	require.NoError(t, err)
	defer client.Close(ctx)

	tasks := make([]string, 0)

	for i := 0; i < 30; i++ {
		taskID, err := client.scheduleChain(t, ctx, 2)
		require.NoError(t, err)
		require.NotEmpty(t, taskID)

		tasks = append(tasks, taskID)
	}

	for _, taskID := range tasks {
		err := client.cancelTask(ctx, taskID)
		require.NoError(t, err)
	}

	errs := make(chan error)

	for _, taskID := range tasks {
		go func(taskID string) {
			errs <- client.waitTaskEnded(ctx, taskID)
		}(taskID)
	}

	for range tasks {
		err := <-errs
		require.NoError(t, err)
	}
}

func TestTasksAcceptanceHandlePanic(t *testing.T) {
	ctx, cancel := newContext()
	defer cancel()

	client, err := newClient(ctx)
	require.NoError(t, err)
	defer client.Close(ctx)

	var taskID string

	requestCtx := getRequestContext(t, ctx)
	err = client.execute(func() error {
		var err error
		taskID, err = client.scheduler.ScheduleTask(
			requestCtx,
			"PanicTask",
			"",
			&empty.Empty{},
		)
		return err
	})
	require.NoError(t, err)

	err = client.waitTask(ctx, taskID)

	require.Error(t, err)
	//TODO: unwrap error all the way down, instead of just checking the string?
	require.Contains(t, err.Error(), "panic: test panic")
}
