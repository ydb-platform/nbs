package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/common"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	grpc_codes "google.golang.org/grpc/codes"
)

////////////////////////////////////////////////////////////////////////////////

type stateTransition struct {
	lastState *TaskState
	newState  TaskState
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) getTaskID(
	ctx context.Context,
	session *persistence.Session,
	idempotencyKey string,
	accountID string,
) (string, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	// TODO: Return account_id check when NBS-1419 is resolved.
	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $idempotency_key as Utf8;

		select task_id
		from task_ids
		where idempotency_key = $idempotency_key
	`, s.tablesPath),
		persistence.ValueParam("$idempotency_key", persistence.UTF8Value(idempotencyKey)),
	)
	if err != nil {
		return "", err
	}
	defer res.Close()

	if res.NextResultSet(ctx) && res.NextRow() {
		err = tx.Commit(ctx)
		if err != nil {
			return "", err
		}

		var id string
		err = res.ScanNamed(
			persistence.OptionalWithDefault("task_id", &id),
		)
		if err != nil {
			return "", err
		}

		logging.Info(
			ctx,
			"Existing taskID %v for idempotencyKey %v accountID %v",
			id,
			idempotencyKey,
			accountID,
		)
		return id, nil
	}

	id := generateTaskID()

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $task_id as Utf8;
		declare $idempotency_key as Utf8;
		declare $account_id as Utf8;

		upsert into task_ids
			(task_id,
			idempotency_key,
			account_id)
		values
			($task_id,
			 $idempotency_key,
			 $account_id)
	`, s.tablesPath),
		persistence.ValueParam("$task_id", persistence.UTF8Value(id)),
		persistence.ValueParam("$idempotency_key", persistence.UTF8Value(idempotencyKey)),
		persistence.ValueParam("$account_id", persistence.UTF8Value(accountID)),
	)
	if err != nil {
		return "", err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return "", err
	}

	logging.Info(
		ctx,
		"New taskID %v for idempotencyKey %v accountID %v",
		id,
		idempotencyKey,
		accountID,
	)

	return id, nil
}

func (s *storageYDB) deleteFromTable(
	ctx context.Context,
	tx *persistence.Transaction,
	tableName string,
	id string,
) error {

	_, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		delete from %v
		where id = $id
	`, s.tablesPath, tableName),
		persistence.ValueParam("$id", persistence.UTF8Value(id)),
	)
	return err
}

func (s *storageYDB) updateReadyToExecute(
	ctx context.Context,
	tx *persistence.Transaction,
	tableName string,
	status TaskStatus,
	transitions []stateTransition,
) error {

	var values []persistence.Value

	for _, t := range transitions {
		if t.lastState != nil &&
			t.lastState.Status != t.newState.Status &&
			t.lastState.Status == status {

			values = append(values, persistence.StructValue(
				persistence.StructFieldValue("id", persistence.UTF8Value(t.lastState.ID)),
			))
		}
	}

	if len(values) != 0 {
		_, err := tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $values as List<Struct<id: Utf8>>;

			delete from %v on
			select *
			from AS_TABLE($values)
		`, s.tablesPath, tableName),
			persistence.ValueParam("$values", persistence.ListValue(values...)),
		)
		if err != nil {
			return err
		}
	}

	values = nil

	for _, t := range transitions {
		if t.newState.Status != status {
			// Table is not affected by transition.
			continue
		}

		if t.lastState != nil &&
			t.lastState.Status == t.newState.Status &&
			t.lastState.GenerationID == t.newState.GenerationID {

			// Nothing to update.
			continue
		}

		values = append(values, persistence.StructValue(
			persistence.StructFieldValue("id", persistence.UTF8Value(t.newState.ID)),
			persistence.StructFieldValue("generation_id", persistence.Uint64Value(t.newState.GenerationID)),
			persistence.StructFieldValue("task_type", persistence.UTF8Value(t.newState.TaskType)),
			persistence.StructFieldValue("zone_id", persistence.UTF8Value(t.newState.ZoneID)),
		))
	}

	if len(values) == 0 {
		return nil
	}

	_, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $values as List<%v>;

		upsert into %v
		select *
		from AS_TABLE($values)
	`, s.tablesPath, readyToExecuteStructTypeString(), tableName),
		persistence.ValueParam("$values", persistence.ListValue(values...)),
	)
	return err
}

func (s *storageYDB) updateExecuting(
	ctx context.Context,
	tx *persistence.Transaction,
	tableName string,
	status TaskStatus,
	transitions []stateTransition,
) error {

	var values []persistence.Value

	for _, t := range transitions {
		if t.lastState != nil &&
			t.lastState.Status != t.newState.Status &&
			t.lastState.Status == status {

			values = append(values, persistence.StructValue(
				persistence.StructFieldValue("id", persistence.UTF8Value(t.lastState.ID)),
			))
		}
	}

	if len(values) != 0 {
		_, err := tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $values as List<Struct<id: Utf8>>;

			delete from %v on
			select *
			from AS_TABLE($values)
		`, s.tablesPath, tableName),
			persistence.ValueParam("$values", persistence.ListValue(values...)),
		)
		if err != nil {
			return err
		}
	}

	values = nil

	for _, t := range transitions {
		if t.newState.Status != status {
			// Table is not affected by transition.
			continue
		}

		if t.lastState != nil &&
			t.lastState.Status == t.newState.Status &&
			t.lastState.GenerationID == t.newState.GenerationID &&
			t.lastState.ModifiedAt == t.newState.ModifiedAt {

			// Nothing to update.
			continue
		}

		values = append(values, persistence.StructValue(
			persistence.StructFieldValue("id", persistence.UTF8Value(t.newState.ID)),
			persistence.StructFieldValue("generation_id", persistence.Uint64Value(t.newState.GenerationID)),
			persistence.StructFieldValue("modified_at", persistence.TimestampValue(t.newState.ModifiedAt)),
			persistence.StructFieldValue("task_type", persistence.UTF8Value(t.newState.TaskType)),
			persistence.StructFieldValue("zone_id", persistence.UTF8Value(t.newState.ZoneID)),
		))
	}

	if len(values) == 0 {
		return nil
	}

	_, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $values as List<%v>;

		upsert into %v
		select *
		from AS_TABLE($values)
	`, s.tablesPath, executingStructTypeString(), tableName),
		persistence.ValueParam("$values", persistence.ListValue(values...)),
	)
	return err
}

func (s *storageYDB) updateEnded(
	ctx context.Context,
	tx *persistence.Transaction,
	transitions []stateTransition,
) error {

	// NOTE: deletion from 'ended' table is forbidden.

	var values []persistence.Value

	for _, t := range transitions {
		if !IsEnded(t.newState.Status) {
			// Table is not affected by transition.
			continue
		}

		if t.lastState != nil &&
			t.lastState.Status == t.newState.Status {
			// Nothing to update.
			continue
		}

		values = append(values, persistence.StructValue(
			persistence.StructFieldValue("ended_at", persistence.TimestampValue(t.newState.EndedAt)),
			persistence.StructFieldValue("id", persistence.UTF8Value(t.newState.ID)),
			persistence.StructFieldValue("idempotency_key", persistence.UTF8Value(t.newState.IdempotencyKey)),
			persistence.StructFieldValue("account_id", persistence.UTF8Value(t.newState.AccountID)),
		))
	}

	if len(values) == 0 {
		return nil
	}

	_, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $values as List<Struct<ended_at: Timestamp, id: Utf8, idempotency_key: Utf8, account_id: Utf8>>;

		upsert into ended
		select *
		from AS_TABLE($values)
	`, s.tablesPath),
		persistence.ValueParam("$values", persistence.ListValue(values...)),
	)
	return err
}

func (s *storageYDB) updateTaskStates(
	ctx context.Context,
	tx *persistence.Transaction,
	transitions []stateTransition,
) error {

	err := s.updateReadyToExecute(
		ctx,
		tx,
		"ready_to_run",
		TaskStatusReadyToRun,
		transitions,
	)
	if err != nil {
		return err
	}

	err = s.updateExecuting(
		ctx,
		tx,
		"running",
		TaskStatusRunning,
		transitions,
	)
	if err != nil {
		return err
	}

	err = s.updateReadyToExecute(
		ctx,
		tx,
		"ready_to_cancel",
		TaskStatusReadyToCancel,
		transitions,
	)
	if err != nil {
		return err
	}

	err = s.updateExecuting(
		ctx,
		tx,
		"cancelling",
		TaskStatusCancelling,
		transitions,
	)
	if err != nil {
		return err
	}

	err = s.updateEnded(ctx, tx, transitions)
	if err != nil {
		return err
	}

	values := make([]persistence.Value, 0, len(transitions))
	for _, t := range transitions {
		values = append(values, t.newState.structValue())
	}

	if len(values) == 0 {
		return nil
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $values as List<%v>;

		upsert into tasks
		select *
		from AS_TABLE($values)
	`, s.tablesPath, taskStateStructTypeString()),
		persistence.ValueParam("$values", persistence.ListValue(values...)),
	)
	return err
}

func (s *storageYDB) updateTaskEvents(
	ctx context.Context,
	tx *persistence.Transaction,
	taskID string,
	events []int64,
) error {

	_, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;
		declare $events as String;

		update tasks
		set events = $events
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(taskID)),
		persistence.ValueParam("$events", persistence.BytesValue(common.MarshalInts(events))),
	)
	return err
}

func (s *storageYDB) prepareUnfinishedDependencies(
	ctx context.Context,
	tx *persistence.Transaction,
	state *TaskState,
) ([]stateTransition, error) {

	var transitions []stateTransition

	dependencyIDs := state.Dependencies.List()
	for _, id := range dependencyIDs {
		res, err := tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $id as Utf8;
			declare $status as Int64;

			select *
			from tasks
			where id = $id and status < $status
		`, s.tablesPath),
			persistence.ValueParam("$id", persistence.UTF8Value(id)),
			persistence.ValueParam("$status", persistence.Int64Value(int64(TaskStatusFinished))),
		)
		if err != nil {
			return []stateTransition{}, err
		}
		defer res.Close()

		dependencies, err := s.scanTaskStates(ctx, res)
		if err != nil {
			return []stateTransition{}, err
		}

		if len(dependencies) == 0 {
			state.Dependencies.Remove(id)
		} else {
			newState := dependencies[0].DeepCopy()
			newState.dependants.Add(state.ID)

			transitions = append(transitions, stateTransition{
				lastState: &dependencies[0],
				newState:  newState,
			})
		}
	}

	return transitions, nil
}

func (s *storageYDB) prepareCreateTask(
	ctx context.Context,
	tx *persistence.Transaction,
	state *TaskState,
) ([]stateTransition, error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from tasks
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(state.ID)),
	)
	if err != nil {
		return []stateTransition{}, err
	}
	defer res.Close()

	existingStates, err := s.scanTaskStates(ctx, res)
	if err != nil {
		return []stateTransition{}, err
	}

	if len(existingStates) != 0 {
		logging.Info(
			ctx,
			"task with id=%v already exists",
			state.ID,
		)

		existingState := existingStates[0]
		if state.TaskType != existingState.TaskType {
			err = tx.Commit(ctx)
			if err != nil {
				return []stateTransition{}, err
			}

			return []stateTransition{}, errors.NewNonRetriableErrorf(
				`cannot create task with type=%v, because task with same id=%v,
				 but different type=%v already exists`,
				state.TaskType,
				state.ID,
				existingState.TaskType,
			)
		}

		if !bytes.Equal(state.Request, existingState.Request) {
			err = tx.Commit(ctx)
			if err != nil {
				return []stateTransition{}, err
			}

			return []stateTransition{}, errors.NewNonRetriableErrorf(
				`cannot create task with request=%s, because task with same id=%v,
				 but different request=%s already exists`,
				base64.StdEncoding.EncodeToString(state.Request),
				state.ID,
				base64.StdEncoding.EncodeToString(existingState.Request),
			)
		}

		return []stateTransition{}, nil
	}

	state.ChangedStateAt = state.CreatedAt

	transitions, err := s.prepareUnfinishedDependencies(ctx, tx, state)
	if err != nil {
		return []stateTransition{}, err
	}

	if len(transitions) != 0 {
		// Switch to "sleeping" state until dependencies are resolved.
		switch {
		case state.Status == TaskStatusReadyToRun || state.Status == TaskStatusRunning:
			state.Status = TaskStatusWaitingToRun

		case state.Status == TaskStatusReadyToCancel || state.Status == TaskStatusCancelling:
			state.Status = TaskStatusWaitingToCancel
		}
	}

	return append(transitions, stateTransition{
		lastState: nil,
		newState:  *state,
	}), nil
}

func (s *storageYDB) createTask(
	ctx context.Context,
	session *persistence.Session,
	state TaskState,
) (string, error) {

	if len(state.IdempotencyKey) == 0 {
		return "", errors.NewNonRetriableErrorf(
			"failed to create task without IdempotencyKey",
		)
	}

	// TODO: NBS-1419: Temporarily disable accountID check because we have
	// idempotency problem.
	state.AccountID = ""

	taskID, err := s.getTaskID(
		ctx,
		session,
		state.IdempotencyKey,
		state.AccountID,
	)
	if err != nil {
		return "", err
	}

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	state.ID = taskID

	transitions, err := s.prepareCreateTask(ctx, tx, &state)
	if err != nil {
		return "", err
	}

	created := false

	if len(transitions) != 0 {
		err = s.updateTaskStates(ctx, tx, transitions)
		if err != nil {
			return "", err
		}

		created = true
	}

	err = tx.Commit(ctx)
	if err != nil {
		return "", err
	}

	if created {
		s.metrics.OnTaskCreated(state, 1)
	}

	return taskID, nil
}

func (s *storageYDB) addRegularTasks(
	ctx context.Context,
	tx *persistence.Transaction,
	state TaskState,
	count int,
) error {

	var err error
	transitions := make([]stateTransition, 0, count)

	for i := 0; i < count; i++ {
		st := state
		st.ID = generateTaskID()

		var t []stateTransition
		t, err = s.prepareCreateTask(ctx, tx, &st)
		if err != nil {
			return err
		}

		transitions = append(transitions, t...)
	}

	if len(transitions) == 0 {
		return nil
	}

	return s.updateTaskStates(ctx, tx, transitions)
}

func (s *storageYDB) createRegularTasks(
	ctx context.Context,
	session *persistence.Session,
	state TaskState,
	schedule TaskSchedule,
) error {

	state.Regular = true

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $task_type as Utf8;

		select *
		from schedules
		where task_type = $task_type
	`, s.tablesPath),
		persistence.ValueParam("$task_type", persistence.UTF8Value(state.TaskType)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	var schState *scheduleState

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			schState = new(scheduleState)

			err = res.ScanNamed(
				persistence.OptionalWithDefault("task_type", &schState.taskType),
				persistence.OptionalWithDefault("scheduled_at", &schState.scheduledAt),
				persistence.OptionalWithDefault("tasks_inflight", &schState.tasksInflight),
			)
			if err != nil {
				return err
			}
		}
	}

	if schState != nil && schState.tasksInflight != 0 {
		// Some tasks are in-flight, nothing to do.
		return tx.Commit(ctx)
	}

	shouldSchedule := false

	if schedule.UseCrontab {
		year, month, day := state.CreatedAt.UTC().Date()
		hour, min, _ := state.CreatedAt.UTC().Clock()

		if schState != nil {
			lastRunYear, lastRunMonth, lastRunDay := schState.scheduledAt.UTC().Date()

			if year > lastRunYear || (year == lastRunYear && month > lastRunMonth) ||
				(year == lastRunYear && month == lastRunMonth && day > lastRunDay) {
				if hour >= schedule.Hour && min >= schedule.Min {
					shouldSchedule = true
				}
			}
		} else {
			if hour >= schedule.Hour && min >= schedule.Min {
				shouldSchedule = true
			}
		}
	} else {
		if schState != nil {
			schedulingTime := schState.scheduledAt.Add(schedule.ScheduleInterval)

			if state.CreatedAt.After(schedulingTime) {
				shouldSchedule = true
			}
		} else {
			shouldSchedule = true
		}
	}

	if shouldSchedule {
		err := s.addRegularTasks(ctx, tx, state, schedule.MaxTasksInflight)
		if err != nil {
			return err
		}

		newSchState := scheduleState{}
		newSchState.taskType = state.TaskType
		newSchState.tasksInflight = uint64(schedule.MaxTasksInflight)
		newSchState.scheduledAt = state.CreatedAt

		_, err = tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $task_type as Utf8;
			declare $scheduled_at as Timestamp;
			declare $tasks_inflight as Uint64;

			upsert into schedules (task_type, scheduled_at, tasks_inflight)
			values ($task_type, $scheduled_at, $tasks_inflight)
		`, s.tablesPath),
			persistence.ValueParam("$task_type", persistence.UTF8Value(newSchState.taskType)),
			persistence.ValueParam("$scheduled_at", persistence.TimestampValue(newSchState.scheduledAt)),
			persistence.ValueParam("$tasks_inflight", persistence.Uint64Value(newSchState.tasksInflight)),
		)
		if err != nil {
			return err
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	if shouldSchedule {
		s.metrics.OnTaskCreated(state, schedule.MaxTasksInflight)
	}

	return nil
}

func (s *storageYDB) getTask(
	ctx context.Context,
	session *persistence.Session,
	taskID string,
) (TaskState, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from tasks
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(taskID)),
	)
	if err != nil {
		return TaskState{}, err
	}
	defer res.Close()

	tasks, err := s.scanTaskStates(ctx, res)
	if err != nil {
		return TaskState{}, err
	}

	if len(tasks) == 0 {
		return TaskState{}, errors.NewNonRetriableError(
			errors.NewNotFoundErrorWithTaskID(taskID),
		)
	}

	return tasks[0], nil
}

func (s *storageYDB) getTaskByIdempotencyKey(
	ctx context.Context,
	session *persistence.Session,
	idempotencyKey string,
	accountID string,
) (TaskState, error) {

	if len(idempotencyKey) == 0 {
		return TaskState{}, errors.NewNonRetriableErrorf(
			"idempotencyKey should be defined",
		)
	}

	// TODO: Return account_id check when NBS-1419 is resolved.
	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $idempotency_key as Utf8;

		select task_id
		from task_ids
		where idempotency_key = $idempotency_key
	`, s.tablesPath),
		persistence.ValueParam("$idempotency_key", persistence.UTF8Value(idempotencyKey)),
	)
	if err != nil {
		return TaskState{}, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return TaskState{}, errors.NewNonRetriableError(
			errors.NewNotFoundErrorWithIdempotencyKey(idempotencyKey),
		)
	}

	var id string
	err = res.ScanNamed(
		persistence.OptionalWithDefault("task_id", &id),
	)
	if err != nil {
		return TaskState{}, err
	}

	return s.getTask(ctx, session, id)
}

func (s *storageYDB) listTasks(
	ctx context.Context,
	session *persistence.Session,
	tableName string,
	limit uint64,
	taskTypeWhitelist []string,
) ([]TaskInfo, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		pragma AnsiInForEmptyOrNullableItemsCollections;
		declare $limit as Uint64;
		declare $type_white_list as List<Utf8>;
		declare $zone_ids as List<Utf8>;

		select *
		from %v
		where
			(ListLength($type_white_list) == 0 or task_type in $type_white_list) and
			(Len(zone_id) == 0 or zone_id in $zone_ids)
		limit $limit
	`, s.tablesPath, tableName),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
		persistence.ValueParam("$type_white_list", strListValue(taskTypeWhitelist)),
		persistence.ValueParam("$zone_ids", strListValue(s.ZoneIDs)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	return scanTaskInfosStream(ctx, res)
}

func (s *storageYDB) listHangingTasks(
	ctx context.Context,
	session *persistence.Session,
	limit uint64,
) ([]TaskInfo, error) {

	now := time.Now()
	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		pragma AnsiInForEmptyOrNullableItemsCollections;
		declare $limit as Uint64;
		declare $except_task_types as List<Utf8>;
		declare $hanging_task_timeout as Interval;
		declare $missed_estimates_until_task_is_hanging as Uint64;
		declare $now as Timestamp;

		$task_ids = (
			select id from ready_to_run UNION ALL
			select id from running UNION ALL
			select id from ready_to_cancel UNION ALL
			select id from cancelling
		);
		select * from tasks
		where id in $task_ids and
		(
			(ListLength($except_task_types) == 0) or
			(task_type not in $except_task_types)
		)  and
		(
			(estimated_time == DateTime::FromSeconds(0) and $now >= created_at + $hanging_task_timeout) or
			(
				estimated_time > created_at and
				$now >= MAX_OF(
					created_at + (estimated_time - created_at) * $missed_estimates_until_task_is_hanging,
					created_at + $hanging_task_timeout
				)
			)
		) limit $limit;
	`, s.tablesPath),
		persistence.ValueParam("$limit", persistence.Uint64Value(limit)),
		persistence.ValueParam(
			"$except_task_types",
			strListValue(s.exceptHangingTaskTypes),
		),
		persistence.ValueParam(
			"$hanging_task_timeout",
			persistence.IntervalValue(s.hangingTaskTimeout),
		),
		persistence.ValueParam(
			"$missed_estimates_until_task_is_hanging",
			persistence.Uint64Value(s.missedEstimatesUntilTaskIsHanging),
		),
		persistence.ValueParam("$now", persistence.TimestampValue(now)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	return scanTaskInfosStream(ctx, res)
}

func (s *storageYDB) listTasksStallingWhileExecuting(
	ctx context.Context,
	session *persistence.Session,
	excludingHostname string,
	tableName string,
	limit uint64,
	taskTypeWhitelist []string,
) ([]TaskInfo, error) {

	stallingTime := time.Now().Add(-s.taskStallingTimeout)
	// TODO: Use excludingHostname
	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		pragma AnsiInForEmptyOrNullableItemsCollections;
		declare $limit as Uint64;
		declare $stalling_time as Timestamp;
		declare $type_white_list as List<Utf8>;
		declare $zone_ids as List<Utf8>;

		select * from %v
		where
			(modified_at < $stalling_time) and
			(ListLength($type_white_list) == 0 or task_type in $type_white_list) and
			(Len(zone_id) == 0 or zone_id in $zone_ids)
		limit $limit
	`, s.tablesPath, tableName),
		persistence.ValueParam("$limit", persistence.Uint64Value(limit)),
		persistence.ValueParam("$stalling_time", persistence.TimestampValue(stallingTime)),
		persistence.ValueParam("$type_white_list", strListValue(taskTypeWhitelist)),
		persistence.ValueParam("$zone_ids", strListValue(s.ZoneIDs)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	return scanTaskInfosStream(ctx, res)
}

func (s *storageYDB) listTaskIDs(
	ctx context.Context,
	session *persistence.Session,
	tableName string,
	limit uint64,
	taskTypeWhitelist []string,
) ([]string, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		pragma AnsiInForEmptyOrNullableItemsCollections;
		declare $limit as Uint64;
		declare $type_white_list as List<Utf8>;
		declare $zone_ids as List<Utf8>;

		select *
		from %v
		where
			(ListLength($type_white_list) == 0 or task_type in $type_white_list) and
			(Len(zone_id) == 0 or zone_id in $zone_ids)
		limit $limit
	`, s.tablesPath, tableName),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
		persistence.ValueParam("$type_white_list", strListValue(taskTypeWhitelist)),
		persistence.ValueParam("$zone_ids", strListValue(s.ZoneIDs)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	return scanTaskIDsStream(ctx, res)
}

func (s *storageYDB) listFailedTasks(
	ctx context.Context,
	session *persistence.Session,
	since time.Time,
) ([]string, error) {

	res, err := session.StreamExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		pragma AnsiInForEmptyOrNullableItemsCollections;
		declare $since as Timestamp;

		$task_ids = (select id from ended where ended_at >= $since);

		select *
		from tasks
		where id in $task_ids and error_message != "" and not error_silent
	`, s.tablesPath),
		persistence.ValueParam("$since", persistence.TimestampValue(since)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	return scanTaskIDsStream(ctx, res)
}

func (s *storageYDB) listSlowTasks(
	ctx context.Context,
	session *persistence.Session,
	since time.Time,
	estimateMiss time.Duration,
) ([]string, error) {

	res, err := session.StreamExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		pragma AnsiInForEmptyOrNullableItemsCollections;
		declare $since as Timestamp;
		declare $estimateMiss as Int64;

		$task_ids = (select id from ended where ended_at >= $since);

		select *
		  from tasks
		 where id in $task_ids
		   and estimated_time > created_at
		   and DateTime::ToMinutes(ended_at - estimated_time) >= $estimateMiss
		 order by DateTime::ToMinutes(ended_at - created_at) / DateTime::ToMinutes(estimated_time - created_at) desc
	`, s.tablesPath),
		persistence.ValueParam("$since", persistence.TimestampValue(since)),
		persistence.ValueParam("$estimateMiss", persistence.Int64Value(int64(estimateMiss.Minutes()))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	return scanTaskIDsStream(ctx, res)
}

func (s *storageYDB) lockTaskToExecute(
	ctx context.Context,
	session *persistence.Session,
	taskInfo TaskInfo,
	now time.Time,
	acceptableStatus func(TaskStatus) bool,
	newStatus TaskStatus,
	hostname string,
	runner string,
) (TaskState, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return TaskState{}, err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from tasks
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(taskInfo.ID)),
	)
	if err != nil {
		return TaskState{}, err
	}
	defer res.Close()

	states, err := s.scanTaskStates(ctx, res)
	if err != nil {
		return TaskState{}, err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return TaskState{}, err
		}

		return TaskState{}, errors.NewNonRetriableError(
			errors.NewNotFoundErrorWithTaskID(taskInfo.ID),
		)
	}

	state := states[0]
	if state.GenerationID != taskInfo.GenerationID {
		// NOTE: no need to commit here, because WrongGenerationError is
		// interpreted similar to RetriableError.
		return TaskState{}, errors.NewWrongGenerationError()
	}

	lastState := state.DeepCopy()

	if !acceptableStatus(lastState.Status) {
		err = tx.Commit(ctx)
		if err != nil {
			return TaskState{}, err
		}

		return TaskState{}, errors.NewNonRetriableErrorf(
			"invalid status %v",
			lastState.Status,
		)
	}

	state.Status = newStatus
	state.GenerationID++
	state.LastHost = hostname
	state.LastRunner = runner
	state.ChangedStateAt = lastState.ChangedStateAt
	if lastState.Status != state.Status {
		state.ChangedStateAt = now
	} else {
		// This task was in running/cancelling state
		// and was picked up by the stalking runner
		state.StallingDuration += now.Sub(state.ModifiedAt)
	}
	state.ModifiedAt = now

	transition := stateTransition{
		lastState: &lastState,
		newState:  state,
	}
	err = s.updateTaskStates(ctx, tx, []stateTransition{transition})
	if err != nil {
		return TaskState{}, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return TaskState{}, err
	}

	return state, nil
}

func (s *storageYDB) prepareDependenciesToClear(
	ctx context.Context,
	tx *persistence.Transaction,
	state *TaskState,
) ([]stateTransition, error) {

	var transitions []stateTransition

	dependencyIDs := state.Dependencies.List()
	for _, id := range dependencyIDs {
		res, err := tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $id as Utf8;

			select *
			from tasks
			where id = $id
		`, s.tablesPath),
			persistence.ValueParam("$id", persistence.UTF8Value(id)),
		)
		if err != nil {
			return []stateTransition{}, err
		}
		defer res.Close()

		dependencies, err := s.scanTaskStates(ctx, res)
		if err != nil {
			return []stateTransition{}, err
		}

		state.Dependencies.Remove(id)

		if len(dependencies) != 0 {
			newState := dependencies[0].DeepCopy()
			newState.dependants.Remove(state.ID)

			transitions = append(transitions, stateTransition{
				lastState: &dependencies[0],
				newState:  newState,
			})
		}
	}

	return transitions, nil
}

func (s *storageYDB) prepareDependantsToWakeup(
	ctx context.Context,
	tx *persistence.Transaction,
	state *TaskState,
) ([]stateTransition, error) {

	var ids []persistence.Value
	for _, id := range state.dependants.List() {
		ids = append(ids, persistence.UTF8Value(id))
	}

	if len(ids) == 0 {
		return []stateTransition{}, nil
	}

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $ids as List<Utf8>;

		select *
		from tasks
		where id in $ids
	`, s.tablesPath),
		persistence.ValueParam("$ids", persistence.ListValue(ids...)),
	)
	if err != nil {
		return []stateTransition{}, err
	}
	defer res.Close()

	dependants, err := s.scanTaskStates(ctx, res)
	if err != nil {
		return []stateTransition{}, err
	}

	var transitions []stateTransition

	for i := 0; i < len(dependants); i++ {
		newState := dependants[i].DeepCopy()
		newState.Dependencies.Remove(state.ID)

		if len(newState.Dependencies.Vals()) == 0 {
			// Return from "sleeping" state because dependencies are resolved.
			switch newState.Status {
			case TaskStatusWaitingToRun:
				newState.Status = TaskStatusReadyToRun
				newState.GenerationID++

			case TaskStatusWaitingToCancel:
				newState.Status = TaskStatusReadyToCancel
				newState.GenerationID++
			}
		}

		transitions = append(transitions, stateTransition{
			lastState: &dependants[i],
			newState:  newState,
		})

		state.dependants.Remove(dependants[i].ID)
	}

	return transitions, nil
}

func (s *storageYDB) markForCancellation(
	ctx context.Context,
	session *persistence.Session,
	taskID string,
	now time.Time,
) (bool, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from tasks
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(taskID)),
	)
	if err != nil {
		return false, err
	}
	defer res.Close()

	states, err := s.scanTaskStates(ctx, res)
	if err != nil {
		return false, err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return false, err
		}

		return false, errors.NewNonRetriableError(
			errors.NewNotFoundErrorWithTaskID(taskID),
		)
	}
	state := states[0]

	if IsCancellingOrCancelled(state.Status) {
		err = tx.Commit(ctx)
		if err != nil {
			return false, err
		}

		return true, nil
	}

	if state.Status == TaskStatusFinished {
		return false, tx.Commit(ctx)
	}

	lastState := state.DeepCopy()

	state.Status = TaskStatusReadyToCancel
	state.GenerationID++
	state.ModifiedAt = now
	state.ChangedStateAt = now
	state.ErrorMessage = "Cancelled by client"
	state.ErrorCode = grpc_codes.Canceled

	transitions, err := s.prepareDependenciesToClear(ctx, tx, &state)
	if err != nil {
		return false, err
	}

	wakeupTransitions, err := s.prepareDependantsToWakeup(ctx, tx, &state)
	if err != nil {
		return false, err
	}

	transitions = append(transitions, wakeupTransitions...)
	transitions = append(transitions, stateTransition{
		lastState: &lastState,
		newState:  state,
	})
	err = s.updateTaskStates(ctx, tx, transitions)
	if err != nil {
		return false, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *storageYDB) decrementRegularTasksInflight(
	ctx context.Context,
	tx *persistence.Transaction,
	taskType string,
) error {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $task_type as Utf8;

		select *
		from schedules
		where task_type = $task_type
	`, s.tablesPath),
		persistence.ValueParam("$task_type", persistence.UTF8Value(taskType)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	found := false
	schState := scheduleState{}

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			err = res.ScanNamed(
				persistence.OptionalWithDefault("task_type", &schState.taskType),
				persistence.OptionalWithDefault("scheduled_at", &schState.scheduledAt),
				persistence.OptionalWithDefault("tasks_inflight", &schState.tasksInflight),
			)
			if err != nil {
				return err
			}

			found = true
		}
	}

	if !found {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf("schedule %v is not found", taskType)
	}

	if schState.tasksInflight == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"schedule %v should have tasksInflight greater than zero",
			taskType,
		)
	}

	schState.tasksInflight--

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $task_type as Utf8;
		declare $scheduled_at as Timestamp;
		declare $tasks_inflight as Uint64;

		upsert into schedules (task_type, scheduled_at, tasks_inflight)
		values ($task_type, $scheduled_at, $tasks_inflight)
	`, s.tablesPath),
		persistence.ValueParam("$task_type", persistence.UTF8Value(schState.taskType)),
		persistence.ValueParam("$scheduled_at", persistence.TimestampValue(schState.scheduledAt)),
		persistence.ValueParam("$tasks_inflight", persistence.Uint64Value(schState.tasksInflight)),
	)
	return err
}

func (s *storageYDB) updateTaskTx(
	ctx context.Context,
	tx *persistence.Transaction,
	state TaskState,
) (TaskState, error) {

	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, s.updateTaskTimeout)
	defer cancel()

	logging.Info(ctx, "updating task with id %v", state.ID)
	defer func() {
		logging.Info(ctx, "finished updating task with id %v", state.ID)
	}()

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from tasks
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(state.ID)),
	)
	if err != nil {
		return TaskState{}, err
	}
	defer res.Close()

	states, err := s.scanTaskStates(ctx, res)
	if err != nil {
		return TaskState{}, err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return TaskState{}, err
		}

		return TaskState{}, errors.NewNonRetriableError(
			errors.NewNotFoundErrorWithTaskID(state.ID),
		)
	}

	lastState := states[0]

	if lastState.GenerationID != state.GenerationID {
		// NOTE: no need to commit here, because WrongGenerationError is
		// interpreted similar to RetriableError.
		return TaskState{}, errors.NewWrongGenerationError()
	}

	state.ChangedStateAt = lastState.ChangedStateAt
	state.EndedAt = lastState.EndedAt

	if lastState.Status != state.Status {
		state.ChangedStateAt = state.ModifiedAt

		if IsEnded(state.Status) {
			state.EndedAt = state.ModifiedAt
		}

		state.GenerationID++
	}
	// Always inherit dependants from previous state.
	state.dependants = lastState.dependants.DeepCopy()

	// It is forbidden to change Events via updateTask,
	// but we should return up-to-date Events value.
	state.Events = lastState.Events

	transitions, err := s.prepareUnfinishedDependencies(ctx, tx, &state)
	if err != nil {
		return TaskState{}, err
	}

	shouldInterruptTaskExecution := false

	if len(transitions) != 0 {
		// Switch to "sleeping" state until dependencies are resolved.
		switch state.Status {
		case TaskStatusRunning:
			state.Status = TaskStatusWaitingToRun
			state.GenerationID++
			shouldInterruptTaskExecution = true

		case TaskStatusCancelling:
			state.Status = TaskStatusWaitingToCancel
			state.GenerationID++
			shouldInterruptTaskExecution = true
		}
	}

	if HasResult(state.Status) {
		dependants, err := s.prepareDependantsToWakeup(ctx, tx, &state)
		if err != nil {
			return TaskState{}, err
		}

		transitions = append(transitions, dependants...)
	}

	if IsEnded(state.Status) && state.Regular {
		err := s.decrementRegularTasksInflight(ctx, tx, state.TaskType)
		if err != nil {
			return TaskState{}, err
		}
	}

	transitions = append(transitions, stateTransition{
		lastState: &lastState,
		newState:  state,
	})
	err = s.updateTaskStates(ctx, tx, transitions)
	if err != nil {
		return TaskState{}, err
	}

	if shouldInterruptTaskExecution {
		err = tx.Commit(ctx)
		if err != nil {
			return TaskState{}, err
		}

		// State has been updated but execution is not possible within current
		// generation.
		return TaskState{}, errors.NewInterruptExecutionError()
	}

	// Returned state should have old generation id, this is needed to protect
	// task from further updating within current execution.
	state.GenerationID = lastState.GenerationID

	return state, nil
}

func (s *storageYDB) updateTaskWithPreparation(
	ctx context.Context,
	session *persistence.Session,
	state TaskState,
	preparation func(context.Context, *persistence.Transaction) error,
) (TaskState, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return TaskState{}, err
	}
	defer tx.Rollback(ctx)

	err = preparation(ctx, tx)
	if err != nil {
		return TaskState{}, err
	}

	state, err = s.updateTaskTx(ctx, tx, state)
	if err != nil {
		return TaskState{}, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return TaskState{}, err
	}

	s.metrics.OnTaskUpdated(ctx, state)
	return state, nil
}

func (s *storageYDB) updateTask(
	ctx context.Context,
	session *persistence.Session,
	state TaskState,
) (TaskState, error) {

	return s.updateTaskWithPreparation(
		ctx,
		session,
		state,
		func(context.Context, *persistence.Transaction) error {
			return nil
		},
	)
}

func (s *storageYDB) sendEvent(
	ctx context.Context,
	session *persistence.Session,
	taskID string,
	event int64,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from tasks
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(taskID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := s.scanTaskStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableError(
			errors.NewNotFoundErrorWithTaskID(taskID),
		)
	}

	state := states[0]

	alreadyExists := false
	for _, elem := range state.Events {
		if elem == event {
			alreadyExists = true
			break
		}
	}

	if !alreadyExists {
		state.Events = append(state.Events, event)
	}

	err = s.updateTaskEvents(ctx, tx, taskID, state.Events)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) clearEndedTasks(
	ctx context.Context,
	session *persistence.Session,
	endedBefore time.Time,
	limit int,
) error {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $ended_before as Timestamp;
		declare $limit as Uint64;

		select *
		from ended
		where ended_at < $ended_before
		limit $limit
	`, s.tablesPath),
		persistence.ValueParam("$ended_before", persistence.TimestampValue(endedBefore)),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var (
				endedAt        time.Time
				taskID         string
				idempotencyKey string
				accountID      string
			)
			err = res.ScanNamed(
				persistence.OptionalWithDefault("ended_at", &endedAt),
				persistence.OptionalWithDefault("id", &taskID),
				persistence.OptionalWithDefault("idempotency_key", &idempotencyKey),
				persistence.OptionalWithDefault("account_id", &accountID),
			)
			if err != nil {
				return err
			}

			execute := func(deleteFromTaskIds string) error {
				_, err = session.ExecuteRW(ctx, fmt.Sprintf(`
					--!syntax_v1
					pragma TablePathPrefix = "%v";
					declare $ended_at as Timestamp;
					declare $task_id as Utf8;
					declare $idempotency_key as Utf8;
					declare $account_id as Utf8;
					declare $finished as Int64;
					declare $cancelled as Int64;

					delete from tasks
					where id = $task_id and (status = $finished or status = $cancelled);

					%v

					delete from ended
					where ended_at = $ended_at and id = $task_id
				`, s.tablesPath, deleteFromTaskIds),
					persistence.ValueParam("$ended_at", persistence.TimestampValue(endedAt)),
					persistence.ValueParam("$task_id", persistence.UTF8Value(taskID)),
					persistence.ValueParam("$idempotency_key", persistence.UTF8Value(idempotencyKey)),
					persistence.ValueParam("$account_id", persistence.UTF8Value(accountID)),
					persistence.ValueParam("$finished", persistence.Int64Value(int64(TaskStatusFinished))),
					persistence.ValueParam("$cancelled", persistence.Int64Value(int64(TaskStatusCancelled))),
				)
				return err
			}

			if len(idempotencyKey) == 0 {
				err = execute("")
			} else {
				err = execute(`
					delete from task_ids
					where idempotency_key = $idempotency_key and account_id = $account_id;
				`)
			}
			if err != nil {
				return err
			}

			logging.Info(
				ctx,
				"Cleared task with id %v, ended at %v",
				taskID,
				endedAt,
			)
		}
	}

	return nil
}

func (s *storageYDB) forceFinishTask(
	ctx context.Context,
	session *persistence.Session,
	taskID string,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from tasks
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(taskID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := s.scanTaskStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableError(
			errors.NewNotFoundErrorWithTaskID(taskID),
		)
	}

	state := states[0]
	lastState := state.DeepCopy()

	if lastState.Status == TaskStatusFinished {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	now := time.Now()

	state.Status = TaskStatusFinished
	state.ErrorCode = grpc_codes.OK
	state.GenerationID++
	state.ModifiedAt = now
	state.ChangedStateAt = now
	state.EndedAt = now

	transitions := []stateTransition{
		stateTransition{
			lastState: &lastState,
			newState:  state,
		},
	}

	wakeupTransitions, err := s.prepareDependantsToWakeup(ctx, tx, &state)
	if err != nil {
		return err
	}

	transitions = append(transitions, wakeupTransitions...)

	err = s.updateTaskStates(ctx, tx, transitions)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) pauseTask(
	ctx context.Context,
	session *persistence.Session,
	taskID string,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from tasks
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(taskID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := s.scanTaskStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableError(
			errors.NewNotFoundErrorWithTaskID(taskID),
		)
	}

	state := states[0]
	lastState := state.DeepCopy()

	if lastState.Status == TaskStatusWaitingToRun {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	if HasResult(lastState.Status) {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"invalid status %v",
			lastState.Status,
		)
	}

	now := time.Now()

	state.Status = TaskStatusWaitingToRun
	state.GenerationID++
	state.ModifiedAt = now
	state.ChangedStateAt = now

	transition := stateTransition{
		lastState: &lastState,
		newState:  state,
	}
	err = s.updateTaskStates(ctx, tx, []stateTransition{transition})
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) resumeTask(
	ctx context.Context,
	session *persistence.Session,
	taskID string,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from tasks
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(taskID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := s.scanTaskStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableError(
			errors.NewNotFoundErrorWithTaskID(taskID),
		)
	}

	state := states[0]
	lastState := state.DeepCopy()

	if HasResult(lastState.Status) {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"invalid status %v",
			lastState.Status,
		)
	}

	if lastState.Status != TaskStatusWaitingToRun {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	if lastState.Dependencies.Size() != 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"Task should not have unresolved dependencies",
		)
	}

	now := time.Now()

	state.Status = TaskStatusReadyToRun
	state.GenerationID++
	state.ModifiedAt = now
	state.ChangedStateAt = now

	transition := stateTransition{
		lastState: &lastState,
		newState:  state,
	}
	err = s.updateTaskStates(ctx, tx, []stateTransition{transition})
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
