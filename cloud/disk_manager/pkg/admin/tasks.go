package admin

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/spf13/cobra"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/util"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

func toJSONWithDependencies(
	ctx context.Context,
	task *tasks_storage.TaskState,
	taskStorage tasks_storage.Storage,
	maxDepth uint64,
) (*util.TaskStateJSON, error) {

	jsonTask := util.TaskStateToJSON(task)

	if maxDepth != 0 {
		for i, depTask := range jsonTask.Dependencies {
			t, err := taskStorage.GetTask(ctx, depTask.ID)
			if err != nil {
				return nil, fmt.Errorf("failed to get task: %w", err)
			}

			stateJSON, err := toJSONWithDependencies(ctx, &t, taskStorage, maxDepth-1)
			if err != nil {
				return nil, fmt.Errorf("failed to expand dependencies: %w", err)
			}

			jsonTask.Dependencies[i] = stateJSON
		}
	}

	return jsonTask, nil
}

////////////////////////////////////////////////////////////////////////////////

type getTask struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	taskID       string
	maxDepth     uint64
}

func (c *getTask) run() error {
	ctx := newContext(c.clientConfig)

	taskStorage, db, err := newTaskStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	task, err := taskStorage.GetTask(ctx, c.taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}

	jsonTask, err := toJSONWithDependencies(ctx, &task, taskStorage, c.maxDepth)
	if err != nil {
		return err
	}

	json, err := jsonTask.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal task to json: %w", err)
	}

	fmt.Println(string(json))

	return nil
}

func newGetTaskCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &getTask{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "get",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.taskID, "id", "", "ID of task to get")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	cmd.Flags().Uint64Var(&c.maxDepth, "max-depth", 10, "Max depth of expanding dependencies")

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type cancelTask struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	taskID       string
}

func (c *cancelTask) run() error {
	ctx := newContext(c.clientConfig)

	taskStorage, db, err := newTaskStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	ok, err := taskStorage.MarkForCancellation(ctx, c.taskID, time.Now())
	if err != nil {
		return fmt.Errorf("failed to mark for cancellation task: %w", err)
	}

	if ok {
		fmt.Printf("Task %s has marked for cancellation\n", c.taskID)
	} else {
		fmt.Printf("Task %s hasn't marked for cancellation. Maybe it has already finished\n", c.taskID)
	}

	return nil
}

func newCancelTaskCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &cancelTask{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "cancel",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.taskID, "id", "", "ID of task to get")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type lister func(context.Context, tasks_storage.Storage, uint64) ([]string, error)

type listTasks struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	taskLister   lister
	limit        uint64
}

func (c *listTasks) run() error {
	ctx := newContext(c.clientConfig)

	taskStorage, db, err := newTaskStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	tasks, err := c.taskLister(ctx, taskStorage, c.limit)
	if err != nil {
		return fmt.Errorf("failed to list tasks: %w", err)
	}

	return printTasks(ctx, taskStorage, tasks)
}

func newListTasksCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
	command string,
	taskLister lister,
) *cobra.Command {

	c := &listTasks{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
		taskLister:   taskLister,
	}

	cmd := &cobra.Command{
		Use: command,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().Uint64Var(&c.limit, "limit", 1000, "limit for listing tasks")

	return cmd
}

func newListReadyToRunCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	return newListTasksCmd(
		clientConfig,
		serverConfig,
		"ready_to_run",
		func(ctx context.Context, storage tasks_storage.Storage, limit uint64) ([]string, error) {
			tasks, err := storage.ListTasksReadyToRun(
				ctx,
				limit,
				nil, // taskTypeWhitelist
			)
			if err != nil {
				return []string{}, err
			}

			return getTaskIDs(tasks), nil
		},
	)
}

func newListReadyToCancelCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	return newListTasksCmd(
		clientConfig,
		serverConfig,
		"ready_to_cancel",
		func(ctx context.Context, storage tasks_storage.Storage, limit uint64) ([]string, error) {
			tasks, err := storage.ListTasksReadyToCancel(
				ctx,
				limit,
				nil, // taskTypeWhitelist
			)
			if err != nil {
				return []string{}, err
			}

			return getTaskIDs(tasks), nil
		},
	)
}

func newListRunningCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	return newListTasksCmd(
		clientConfig,
		serverConfig,
		"running",
		func(ctx context.Context, storage tasks_storage.Storage, limit uint64) ([]string, error) {
			tasks, err := storage.ListTasksRunning(ctx, limit)
			if err != nil {
				return []string{}, err
			}

			return getTaskIDs(tasks), nil
		},
	)
}

func newListCancellingCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	return newListTasksCmd(
		clientConfig,
		serverConfig,
		"cancelling",
		func(ctx context.Context, storage tasks_storage.Storage, limit uint64) ([]string, error) {
			tasks, err := storage.ListTasksCancelling(ctx, limit)
			if err != nil {
				return []string{}, err
			}

			return getTaskIDs(tasks), nil
		},
	)
}

////////////////////////////////////////////////////////////////////////////////

type endedLister func(context.Context, tasks_storage.Storage, time.Time) ([]string, error)

type listEndedTasks struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	taskLister   endedLister
	since        string
}

func (c *listEndedTasks) run() error {
	ctx := newContext(c.clientConfig)

	sinceDuration, err := time.ParseDuration(c.since)
	if err != nil {
		return fmt.Errorf("failed to parse 'since' parameter: %w", err)
	}

	since := time.Now().Add(-sinceDuration)

	taskStorage, db, err := newTaskStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	tasks, err := c.taskLister(ctx, taskStorage, since)
	if err != nil {
		return fmt.Errorf("failed to list tasks: %w", err)
	}

	return printTasks(ctx, taskStorage, tasks)
}

func newListEndedTasksCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
	command string,
	taskLister endedLister,
) *cobra.Command {

	c := &listEndedTasks{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
		taskLister:   taskLister,
	}

	cmd := &cobra.Command{
		Use: command,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.since,
		"since",
		"24h",
		"list tasks that failed with fatal error since this duration ago. Example: 2h45m",
	)

	return cmd
}

func newListFailedCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	return newListEndedTasksCmd(
		clientConfig,
		serverConfig,
		"failed",
		func(ctx context.Context, storage tasks_storage.Storage, since time.Time) ([]string, error) {
			return storage.ListFailedTasks(ctx, since)
		},
	)
}

////////////////////////////////////////////////////////////////////////////////

func listHangingTasks(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) error {

	ctx := newContext(clientConfig)

	taskStorage, db, err := newTaskStorage(ctx, serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	taskInfos, err := taskStorage.ListHangingTasks(ctx, ^uint64(0))
	if err != nil {
		return err
	}

	tasks := getTaskIDs(taskInfos)
	return printTasks(ctx, taskStorage, tasks)
}

func newListHangingTasksCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	return &cobra.Command{
		Use: "hanging",
		RunE: func(cmd *cobra.Command, args []string) error {
			return listHangingTasks(clientConfig, serverConfig)
		},
	}
}

////////////////////////////////////////////////////////////////////////////////

type slowLister func(context.Context, tasks_storage.Storage, time.Time, time.Duration) ([]string, error)

type listSlowTasks struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	taskLister   slowLister
	since        string
	estimateMiss string
}

func (c *listSlowTasks) run() error {
	ctx := newContext(c.clientConfig)

	sinceDuration, err := time.ParseDuration(c.since)
	if err != nil {
		return fmt.Errorf("failed to parse 'since' parameter: %w", err)
	}

	since := time.Now().Add(-sinceDuration)

	estimateMiss, err := time.ParseDuration(c.estimateMiss)
	if err != nil {
		return fmt.Errorf("failed to parse 'estimateMiss' parameter: %w", err)
	}

	taskStorage, db, err := newTaskStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	tasks, err := c.taskLister(ctx, taskStorage, since, estimateMiss)
	if err != nil {
		return fmt.Errorf("failed to list slow tasks: %w", err)
	}

	return printTasks(ctx, taskStorage, tasks)
}

func newListSlowTasksCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
	command string,
	taskLister slowLister,
) *cobra.Command {

	c := &listSlowTasks{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
		taskLister:   taskLister,
	}

	cmd := &cobra.Command{
		Use: command,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.since,
		"since",
		"24h",
		"list slow tasks that ended since this duration ago. Example: 2h45m",
	)
	cmd.Flags().StringVar(
		&c.estimateMiss,
		"estimate-miss",
		"30m",
		"list tasks that exceeded their estimate by this duration. Example: 2h45m",
	)

	return cmd
}

func newListSlowCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	return newListSlowTasksCmd(
		clientConfig,
		serverConfig,
		"slow",
		func(ctx context.Context, storage tasks_storage.Storage, since time.Time, estimateMiss time.Duration) ([]string, error) {
			return storage.ListSlowTasks(ctx, since, estimateMiss)
		},
	)
}

// //////////////////////////////////////////////////////////////////////////////
func newListCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmd := &cobra.Command{
		Use: "list",
	}

	cmd.AddCommand(
		newListReadyToRunCmd(clientConfig, serverConfig),
		newListReadyToCancelCmd(clientConfig, serverConfig),
		newListRunningCmd(clientConfig, serverConfig),
		newListCancellingCmd(clientConfig, serverConfig),
		newListFailedCmd(clientConfig, serverConfig),
		newListSlowCmd(clientConfig, serverConfig),
		newListHangingTasksCmd(clientConfig, serverConfig),
	)

	return cmd
}

func getTaskIDs(taskInfos []tasks_storage.TaskInfo) []string {
	taskIDs := []string{}
	for _, task := range taskInfos {
		taskIDs = append(taskIDs, task.ID)
	}
	return taskIDs
}

func printTasks(
	ctx context.Context,
	taskStorage tasks_storage.Storage,
	taskIDs []string,
) error {

	for _, taskID := range taskIDs {
		task, err := taskStorage.GetTask(ctx, taskID)
		if err != nil {
			return fmt.Errorf("failed to get task: %w", err)
		}

		json, err := util.TaskStateToJSON(&task).Marshal()
		if err != nil {
			return fmt.Errorf("failed to marshal task to json: %w", err)
		}

		fmt.Println(string(json))
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

type forceFinishTask struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	taskID       string
}

func (c *forceFinishTask) run() error {
	ctx := newContext(c.clientConfig)

	taskStorage, db, err := newTaskStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	return taskStorage.ForceFinishTask(ctx, c.taskID)
}

func newForceFinishTaskCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &forceFinishTask{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use:   "force_finish",
		Short: "Sets task as finished successfully. Dangerous command, use it carefully",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.taskID, "id", "", "ID of task to finish")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type pauseTask struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	taskID       string
}

func (c *pauseTask) run() error {
	ctx := newContext(c.clientConfig)

	taskStorage, db, err := newTaskStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	return taskStorage.PauseTask(ctx, c.taskID)
}

func newPauseTaskCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &pauseTask{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use:   "pause",
		Short: "Pauses task until 'resume' is called",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.taskID, "id", "", "ID of task to pause")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type resumeTask struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	taskID       string
}

func (c *resumeTask) run() error {
	ctx := newContext(c.clientConfig)

	taskStorage, db, err := newTaskStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	return taskStorage.ResumeTask(ctx, c.taskID)
}

func newResumeTaskCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &resumeTask{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use:   "resume",
		Short: "Resumes task",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.taskID, "id", "", "ID of task to resume")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type scheduleBlankTask struct {
	config *client_config.ClientConfig
}

func (c *scheduleBlankTask) run() error {
	ctx := newContext(c.config)

	client, err := internal_client.NewPrivateClientForCLI(ctx, c.config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	resp, err := client.ScheduleBlankOperation(getRequestContext(ctx))
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newScheduleBlankTaskCmd(config *client_config.ClientConfig) *cobra.Command {
	c := &scheduleBlankTask{
		config: config,
	}

	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Schedule task",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

func newTasksCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmd := &cobra.Command{
		Use:     "tasks",
		Aliases: []string{"task"},
	}

	cmd.AddCommand(
		newGetTaskCmd(clientConfig, serverConfig),
		newCancelTaskCmd(clientConfig, serverConfig),
		newListCmd(clientConfig, serverConfig),
		newForceFinishTaskCmd(clientConfig, serverConfig),
		newPauseTaskCmd(clientConfig, serverConfig),
		newResumeTaskCmd(clientConfig, serverConfig),
		newScheduleBlankTaskCmd(clientConfig),
	)

	return cmd
}
