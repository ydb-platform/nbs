package tasks

import (
	"context"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/common"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/operation"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/tracing"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

func withComponentLoggingField(ctx context.Context) context.Context {
	return logging.WithComponent(ctx, logging.ComponentTaskScheduler)
}

////////////////////////////////////////////////////////////////////////////////

type scheduler struct {
	registry                      *Registry
	storage                       tasks_storage.Storage
	pollForTaskUpdatesPeriod      time.Duration
	taskWaitingTimeout            time.Duration
	scheduleRegularTasksPeriodMin time.Duration
	scheduleRegularTasksPeriodMax time.Duration
}

func (s *scheduler) ScheduleTask(
	ctx context.Context,
	taskType string,
	description string,
	request proto.Message,
) (string, error) {

	ctx = withComponentLoggingField(ctx)
	return s.ScheduleZonalTask(
		ctx,
		taskType,
		description,
		"", // zoneID
		request,
	)
}

func (s *scheduler) ScheduleZonalTask(
	ctx context.Context,
	taskType string,
	description string,
	zoneID string,
	request proto.Message,
) (string, error) {

	ctx = withComponentLoggingField(ctx)
	logging.Info(ctx, "scheduling task %v", taskType)

	ctx = tracing.SetTracingContext(ctx)

	marshalledRequest, err := proto.Marshal(request)
	if err != nil {
		logging.Warn(
			ctx,
			"failed to schedule task: marshal request of task %v: %v",
			taskType,
			err,
		)
		return "", err
	}

	createdAt := time.Now()
	metadata := tasks_storage.NewMetadata(headers.GetTracingHeaders(ctx))
	storageFolder := getStorageFolder(ctx)
	idempotencyKey := headers.GetIdempotencyKey(ctx)

	taskID, err := s.storage.CreateTask(ctx, tasks_storage.TaskState{
		ID:             "",
		IdempotencyKey: idempotencyKey,
		AccountID:      headers.GetAccountID(ctx),
		TaskType:       taskType,
		Description:    description,
		StorageFolder:  storageFolder,
		CreatedAt:      createdAt,
		CreatedBy:      headers.GetAccountID(ctx),
		ModifiedAt:     createdAt,
		GenerationID:   0,
		Status:         tasks_storage.TaskStatusReadyToRun,
		Request:        marshalledRequest,
		Metadata:       metadata,
		Dependencies:   common.NewStringSet(),
		ZoneID:         zoneID,
	})
	if err != nil {
		logging.Warn(ctx, "failed to persist task %v: %v", taskType, err)
		return "", err
	}

	logging.Info(
		ctx,
		"scheduled task %v with id %v, idempotencyKey %v, description %v",
		taskType,
		taskID,
		idempotencyKey,
		description,
	)
	return taskID, nil
}

func (s *scheduler) ScheduleRegularTasks(
	ctx context.Context,
	taskType string,
	schedule TaskSchedule,
) {

	ctx = withComponentLoggingField(ctx)

	// TODO: Don't schedule new goroutine for each regular task type.
	go func() {
		for {
			select {
			case <-ctx.Done():
				logging.Info(ctx, "scheduling regular task %v stopped", taskType)
				return
			case <-time.After(
				common.RandomDuration(
					s.scheduleRegularTasksPeriodMin,
					s.scheduleRegularTasksPeriodMax,
				)):
			}

			logging.Info(ctx, "scheduling %v iteration", taskType)

			createdAt := time.Now()

			requestID, err := uuid.NewV4()
			if err != nil {
				logging.Warn(ctx,
					"failed to generate x_request_id for task %v: %v",
					taskType,
					err,
				)
				continue
			}

			metadataCtx := headers.SetIncomingRequestID(ctx, requestID.String())

			// Parent span for tasks scheduled at this iteration.
			metadataCtx, span := tracing.StartSpan(
				metadataCtx,
				"ScheduleRegularTasks",
				tracing.WithAttributes(
					tracing.AttributeString("task_type", taskType),
					tracing.AttributeString("request_id", requestID.String()),
					tracing.AttributeInt(
						"max_tasks_inflight",
						schedule.MaxTasksInflight,
					),
					tracing.AttributeBool("use_crontab", schedule.UseCrontab),
				),
			)
			if schedule.UseCrontab {
				span.SetAttributes(
					tracing.AttributeInt("crontab_hour", schedule.Hour),
					tracing.AttributeInt("crontab_minute", schedule.Min),
				)
			} else {
				span.SetAttributes(
					tracing.AttributeString(
						"schedule_interval",
						schedule.ScheduleInterval.String(),
					),
				)
			}
			span.End()

			metadataCtx = tracing.SetTracingContext(metadataCtx)
			metadata := tasks_storage.NewMetadata(
				headers.GetTracingHeaders(metadataCtx),
			)

			schedule := tasks_storage.TaskSchedule{
				ScheduleInterval: schedule.ScheduleInterval,
				MaxTasksInflight: schedule.MaxTasksInflight,

				UseCrontab: schedule.UseCrontab,
				Hour:       schedule.Hour,
				Min:        schedule.Min,
			}

			err = s.storage.CreateRegularTasks(ctx, tasks_storage.TaskState{
				ID:           "",
				TaskType:     taskType,
				Description:  "",
				CreatedAt:    createdAt,
				CreatedBy:    headers.GetAccountID(ctx),
				ModifiedAt:   createdAt,
				GenerationID: 0,
				Status:       tasks_storage.TaskStatusReadyToRun,
				Metadata:     metadata,
				Dependencies: common.NewStringSet(),
			}, schedule)
			if err != nil {
				logging.Warn(ctx, "failed to persist task %v: %v", taskType, err)
			}
		}
	}()
}

func (s *scheduler) CancelTask(
	ctx context.Context,
	taskID string,
) (bool, error) {

	ctx = withComponentLoggingField(ctx)
	logging.Info(ctx, "cancelling task %v", taskID)

	cancelling, err := s.storage.MarkForCancellation(ctx, taskID, time.Now())
	if err != nil {
		logging.Info(ctx, "failed to cancel task %v", taskID)
		return false, err
	}

	return cancelling, nil
}

// Task is not finished yet, if err == nil and returned response == nil.
func (s *scheduler) getTaskResponse(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	taskState, err := s.storage.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}

	if taskState.ErrorCode != grpc_codes.OK {
		return nil, errors.NewDetailedErrorFull(
			grpc_status.Error(taskState.ErrorCode, taskState.ErrorMessage),
			taskState.ErrorDetails,
			taskState.ErrorSilent,
		)
	}

	if tasks_storage.HasResult(taskState.Status) {
		task, err := s.registry.NewTask(taskState.TaskType)
		if err != nil {
			return nil, err
		}

		err = task.Load(taskState.Request, taskState.State)
		if err != nil {
			return nil, err
		}

		return task.GetResponse(), nil
	}

	return nil, nil
}

func (s *scheduler) WaitTask(
	ctx context.Context,
	execCtx ExecutionContext,
	taskID string,
) (proto.Message, error) {

	ctx = withComponentLoggingField(ctx)
	dependantTaskID := execCtx.GetTaskID()

	logging.Info(ctx, "waiting task %v by %v", taskID, dependantTaskID)

	err := execCtx.AddTaskDependency(ctx, taskID)
	if err != nil {
		logError(ctx, err, "failed to add task dependency %v", taskID)
		return nil, err
	}

	return s.getTaskResponse(ctx, taskID)
}

func (s *scheduler) WaitAnyTasks(
	ctx context.Context,
	taskIDs []string,
) ([]string, error) {

	ctx = withComponentLoggingField(ctx)
	timeout := time.After(s.taskWaitingTimeout)

	for {
		var finishedTaskIDs []string

		for _, taskID := range taskIDs {
			response, err := s.getTaskResponse(ctx, taskID)
			if err != nil {
				return nil, err
			}

			if response != nil {
				// Task is finished.
				finishedTaskIDs = append(finishedTaskIDs, taskID)
			}
		}

		if len(finishedTaskIDs) != 0 {
			return finishedTaskIDs, nil
		}

		select {
		case <-ctx.Done():
			logging.Info(ctx, "waiting cancelled, taskIDs %v", taskIDs)
			return nil, ctx.Err()
		case <-timeout:
			logging.Warn(ctx, "waiting timed out, taskIDs %v", taskIDs)
			return nil, errors.NewInterruptExecutionError()
		case <-time.After(s.pollForTaskUpdatesPeriod):
		}
	}
}

func (s *scheduler) WaitTaskEnded(
	ctx context.Context,
	taskID string,
) error {

	ctx = withComponentLoggingField(ctx)
	timeout := time.After(s.taskWaitingTimeout)

	for {
		state, err := s.storage.GetTask(ctx, taskID)
		if err != nil {
			logging.Info(ctx, "wait iteration failed, taskID %v: %v", taskID, err)
			return err
		}

		if tasks_storage.IsEnded(state.Status) {
			return nil
		}

		select {
		case <-ctx.Done():
			logging.Info(ctx, "waiting cancelled, taskID %v", taskID)
			return ctx.Err()
		case <-timeout:
			logging.Warn(ctx, "waiting timed out, taskID %v", taskID)
			return errors.NewInterruptExecutionError()
		case <-time.After(s.pollForTaskUpdatesPeriod):
		}
	}
}

func (s *scheduler) GetTaskMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	ctx = withComponentLoggingField(ctx)
	logging.Info(ctx, "getting task metadata %v", taskID)

	taskState, err := s.storage.GetTask(ctx, taskID)
	if err != nil {
		logging.Info(ctx, "failed to get task %v from storage %v", taskID, err)
		return nil, err
	}

	task, err := s.registry.NewTask(taskState.TaskType)
	if err != nil {
		logging.Warn(
			ctx,
			"failed to construct task descriptor %v: %v",
			taskID,
			err,
		)

		return nil, err
	}

	err = task.Load(taskState.Request, taskState.State)
	if err != nil {
		logging.Warn(ctx, "failed to load task %v: %v", taskID, err)
		return nil, err
	}

	return task.GetMetadata(ctx)
}

func (s *scheduler) SendEvent(
	ctx context.Context,
	taskID string,
	event int64,
) error {

	ctx = withComponentLoggingField(ctx)
	return s.storage.SendEvent(ctx, taskID, event)
}

func (s *scheduler) GetOperation(
	ctx context.Context,
	taskID string,
) (*operation.Operation, error) {

	ctx = withComponentLoggingField(ctx)
	logging.Debug(ctx, "getting operation proto %v", taskID)

	taskState, err := s.storage.GetTask(ctx, taskID)
	if err != nil {
		logging.Warn(ctx, "failed to get task %v from storage %v", taskID, err)
		return nil, err
	}

	task, err := s.registry.NewTask(taskState.TaskType)
	if err != nil {
		logging.Warn(ctx, "failed to get task descriptor %v: %v", taskID, err)
		return nil, err
	}

	err = task.Load(taskState.Request, taskState.State)
	if err != nil {
		logging.Warn(ctx, "failed to load task %v: %v", taskID, err)
		return nil, err
	}

	createdAtProto, err := ptypes.TimestampProto(taskState.CreatedAt)
	if err != nil {
		logging.Warn(ctx, "failed to convert CreatedAt %v: %v", taskID, err)
		return nil, err
	}

	modifiedAtProto, err := ptypes.TimestampProto(taskState.ModifiedAt)
	if err != nil {
		logging.Warn(ctx, "failed to convert ModifiedAt %v: %v", taskID, err)
		return nil, err
	}

	metadata, err := task.GetMetadata(ctx)
	if err != nil {
		logging.Warn(ctx, "failed to get task metadata %v: %v", taskID, err)
		return nil, err
	}

	metadataAny, err := ptypes.MarshalAny(metadata)
	if err != nil {
		logging.Warn(ctx, "failed to convert metadata %v: %v", taskID, err)
		return nil, err
	}

	op := &operation.Operation{
		Id:          taskState.ID,
		Description: taskState.Description,
		CreatedAt:   createdAtProto,
		CreatedBy:   taskState.CreatedBy,
		ModifiedAt:  modifiedAtProto,
		Done:        tasks_storage.HasResult(taskState.Status),
		Metadata:    metadataAny,
	}

	if taskState.ErrorCode != grpc_codes.OK {
		status := grpc_status.New(taskState.ErrorCode, taskState.ErrorMessage)

		if taskState.ErrorDetails != nil {
			statusWithDetails, err := status.WithDetails(taskState.ErrorDetails)
			if err == nil {
				status = statusWithDetails
			} else {
				logging.Warn(ctx, "failed to attach error details: %v", err)
			}
		}

		op.Result = &operation.Operation_Error{
			Error: status.Proto(),
		}
	} else if tasks_storage.HasResult(taskState.Status) {
		responseAny, err := ptypes.MarshalAny(task.GetResponse())
		if err != nil {
			logging.Warn(ctx, "failed to convert response %v: %v", taskID, err)
			return nil, err
		}

		op.Result = &operation.Operation_Response{
			Response: responseAny,
		}
	}

	return op, nil
}

////////////////////////////////////////////////////////////////////////////////

// Used in tests.
func (s *scheduler) WaitTaskSync(
	ctx context.Context,
	taskID string,
	timeout time.Duration,
) (proto.Message, error) {

	ctx = withComponentLoggingField(ctx)
	timeoutChannel := time.After(timeout)
	iteration := 0

	wait := func() error {
		select {
		case <-ctx.Done():
			logging.Info(ctx, "waiting cancelled, taskID %v: %v", taskID, ctx.Err())
			return ctx.Err()
		case <-timeoutChannel:
			return errors.NewNonRetriableErrorf(
				"scheduler.WaitTaskSync timed out, taskID %v",
				taskID,
			)
		case <-time.After(s.pollForTaskUpdatesPeriod):
		}

		iteration++

		if iteration%20 == 0 {
			logging.Warn(ctx, "still waiting for task with id %v", taskID)
		}
		return nil
	}

	for {
		response, err := s.getTaskResponse(ctx, taskID)
		if err != nil {
			logging.Info(ctx, "wait iteration failed, taskID %v: %v", taskID, err)
			if errors.CanRetry(err) {
				err := wait()
				if err != nil {
					return nil, err
				}

				continue
			}

			return nil, err
		}

		if response != nil {
			return response, nil
		}

		err = wait()
		if err != nil {
			return nil, err
		}
	}
}

func (s *scheduler) ScheduleBlankTask(ctx context.Context) (string, error) {
	return s.ScheduleTask(ctx, "tasks.Blank", "", &empty.Empty{})
}

////////////////////////////////////////////////////////////////////////////////

func (s *scheduler) registerAndScheduleRegularSystemTasks(
	ctx context.Context,
	config *tasks_config.TasksConfig,
	metricsRegistry metrics.Registry,
) error {

	endedTaskExpirationTimeout, err := time.ParseDuration(
		config.GetEndedTaskExpirationTimeout(),
	)
	if err != nil {
		return err
	}

	clearEndedTasksTaskScheduleInterval, err := time.ParseDuration(
		config.GetClearEndedTasksTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	err = s.registry.RegisterForExecution(
		"tasks.ClearEndedTasks", func() Task {
			return &clearEndedTasksTask{
				storage:           s.storage,
				expirationTimeout: endedTaskExpirationTimeout,
				limit:             int(config.GetClearEndedTasksLimit()),
			}
		},
	)
	if err != nil {
		return err
	}

	s.ScheduleRegularTasks(
		ctx,
		"tasks.ClearEndedTasks",
		TaskSchedule{
			ScheduleInterval: clearEndedTasksTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	listerMetricsCollectionInterval, err := time.ParseDuration(
		config.GetListerMetricsCollectionInterval(),
	)
	if err != nil {
		return err
	}

	collectListerMetricsTaskScheduleInterval, err := time.ParseDuration(
		config.GetCollectListerMetricsTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	err = s.registry.RegisterForExecution(
		"tasks.CollectListerMetrics", func() Task {
			return &collectListerMetricsTask{
				registry:                  metricsRegistry,
				storage:                   s.storage,
				metricsCollectionInterval: listerMetricsCollectionInterval,

				taskTypes:                 s.registry.TaskTypes(),
				hangingTaskGaugesByID:     make(map[string]metrics.Gauge),
				maxHangingTaskIDsToReport: config.GetMaxHangingTaskIDsToReport(),
			}
		},
	)
	if err != nil {
		return err
	}

	s.ScheduleRegularTasks(
		ctx,
		"tasks.CollectListerMetrics",
		TaskSchedule{
			ScheduleInterval: collectListerMetricsTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func NewScheduler(
	ctx context.Context,
	registry *Registry,
	storage tasks_storage.Storage,
	config *tasks_config.TasksConfig,
	metricsRegistry metrics.Registry,
) (Scheduler, error) {

	pollForTaskUpdatesPeriod, err := time.ParseDuration(
		config.GetPollForTaskUpdatesPeriod())
	if err != nil {
		return nil, err
	}

	taskWaitingTimeout, err := time.ParseDuration(config.GetTaskWaitingTimeout())
	if err != nil {
		return nil, err
	}

	scheduleRegularTasksPeriodMin, err := time.ParseDuration(config.GetScheduleRegularTasksPeriodMin())
	if err != nil {
		return nil, err
	}

	scheduleRegularTasksPeriodMax, err := time.ParseDuration(config.GetScheduleRegularTasksPeriodMax())
	if err != nil {
		return nil, err
	}

	s := &scheduler{
		registry:                      registry,
		storage:                       storage,
		pollForTaskUpdatesPeriod:      pollForTaskUpdatesPeriod,
		taskWaitingTimeout:            taskWaitingTimeout,
		scheduleRegularTasksPeriodMin: scheduleRegularTasksPeriodMin,
		scheduleRegularTasksPeriodMax: scheduleRegularTasksPeriodMax,
	}

	err = registry.RegisterForExecution("tasks.Blank", func() Task {
		return &blankTask{}
	})
	if err != nil {
		return nil, err
	}

	if config.GetRegularSystemTasksEnabled() {
		err = s.registerAndScheduleRegularSystemTasks(
			ctx,
			config,
			metricsRegistry,
		)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}
