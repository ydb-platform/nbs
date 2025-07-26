package storage

import (
	"context"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/tasks/common"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	grpc_codes "google.golang.org/grpc/codes"
)

////////////////////////////////////////////////////////////////////////////////

func generateTaskID() string {
	return uuid.Must(uuid.NewV4()).String()
}

////////////////////////////////////////////////////////////////////////////////

type scheduleState struct {
	taskType      string
	scheduledAt   time.Time
	tasksInflight uint64
}

////////////////////////////////////////////////////////////////////////////////

func strListValue(strings []string) persistence.Value {
	values := make([]persistence.Value, 0)
	for _, value := range strings {
		values = append(values, persistence.UTF8Value(value))
	}

	var result persistence.Value
	if len(values) == 0 {
		result = persistence.ZeroValue(persistence.List(persistence.TypeUTF8))
	} else {
		result = persistence.ListValue(values...)
	}

	return result
}

////////////////////////////////////////////////////////////////////////////////

func unmarshalErrorDetails(bytes []byte) (*errors.ErrorDetails, error) {
	if len(bytes) == 0 {
		return nil, nil
	}

	details := &errors.ErrorDetails{}

	err := proto.Unmarshal(bytes, details)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to unmarshal ErrorDetails: %w",
			err,
		)
	}

	return details, nil
}

func marshalErrorDetails(details *errors.ErrorDetails) []byte {
	if details == nil {
		return nil
	}

	bytes, err := proto.Marshal(details)
	if err != nil {
		// TODO: Throw an error.
		return nil
	}

	return bytes
}

////////////////////////////////////////////////////////////////////////////////

func scanTaskIDsStream(ctx context.Context, res persistence.Result) ([]string, error) {
	taskIDs := []string{}
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var id string
			err := res.ScanNamed(
				persistence.OptionalWithDefault("id", &id),
			)
			if err != nil {
				return []string{}, err
			}

			taskIDs = append(taskIDs, id)
		}
	}

	// NOTE: always check stream query result after iteration.
	err := res.Err()
	if err != nil {
		return []string{}, errors.NewRetriableError(err)
	}

	return taskIDs, nil
}

func scanTaskInfosStream(ctx context.Context, res persistence.Result) ([]TaskInfo, error) {
	taskInfos := []TaskInfo{}
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var info TaskInfo
			err := res.ScanNamed(
				persistence.OptionalWithDefault("id", &info.ID),
				persistence.OptionalWithDefault("generation_id", &info.GenerationID),
				persistence.OptionalWithDefault("task_type", &info.TaskType),
			)
			if err != nil {
				return []TaskInfo{}, err
			}

			taskInfos = append(taskInfos, info)
		}
	}

	// NOTE: always check stream query result after iteration.
	err := res.Err()
	if err != nil {
		return []TaskInfo{}, errors.NewRetriableError(err)
	}

	return taskInfos, nil
}

////////////////////////////////////////////////////////////////////////////////
// TaskState marshal/unmarshal routines.

func (s *TaskState) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("id", persistence.UTF8Value(s.ID)),
		persistence.StructFieldValue("idempotency_key", persistence.UTF8Value(s.IdempotencyKey)),
		persistence.StructFieldValue("account_id", persistence.UTF8Value(s.AccountID)),
		persistence.StructFieldValue("task_type", persistence.UTF8Value(s.TaskType)),
		persistence.StructFieldValue("regular", persistence.BoolValue(s.Regular)),
		persistence.StructFieldValue("description", persistence.UTF8Value(s.Description)),
		persistence.StructFieldValue("created_at", persistence.TimestampValue(s.CreatedAt)),
		persistence.StructFieldValue("created_by", persistence.UTF8Value(s.CreatedBy)),
		persistence.StructFieldValue("modified_at", persistence.TimestampValue(s.ModifiedAt)),
		persistence.StructFieldValue("generation_id", persistence.Uint64Value(s.GenerationID)),
		persistence.StructFieldValue("status", persistence.Int64Value(int64(s.Status))),
		persistence.StructFieldValue("error_code", persistence.Int64Value(int64(s.ErrorCode))),
		persistence.StructFieldValue("error_message", persistence.UTF8Value(s.ErrorMessage)),
		persistence.StructFieldValue("error_silent", persistence.BoolValue(s.ErrorSilent)),
		persistence.StructFieldValue("error_details", persistence.BytesValue(marshalErrorDetails(s.ErrorDetails))),
		persistence.StructFieldValue("retriable_error_count", persistence.Uint64Value(s.RetriableErrorCount)),
		persistence.StructFieldValue("request", persistence.BytesValue(s.Request)),
		persistence.StructFieldValue("state", persistence.BytesValue(s.State)),
		persistence.StructFieldValue("metadata", persistence.BytesValue(common.MarshalStringMap(s.Metadata.Vals()))),
		persistence.StructFieldValue("dependencies", persistence.BytesValue(common.MarshalStrings(s.Dependencies.List()))),
		persistence.StructFieldValue("changed_state_at", persistence.TimestampValue(s.ChangedStateAt)),
		persistence.StructFieldValue("ended_at", persistence.TimestampValue(s.EndedAt)),
		persistence.StructFieldValue("last_host", persistence.UTF8Value(s.LastHost)),
		persistence.StructFieldValue("last_runner", persistence.UTF8Value(s.LastRunner)),
		persistence.StructFieldValue("zone_id", persistence.UTF8Value(s.ZoneID)),
		persistence.StructFieldValue("estimated_time", persistence.TimestampValue(s.EstimatedTime)),
		persistence.StructFieldValue("inflight_duration", persistence.IntervalValue(s.InflightDuration)),
		persistence.StructFieldValue("stalling_duration", persistence.IntervalValue(s.StallingDuration)),
		persistence.StructFieldValue("waiting_duration", persistence.IntervalValue(s.WaitingDuration)),
		persistence.StructFieldValue("dependants", persistence.BytesValue(common.MarshalStrings(s.dependants.List()))),
		persistence.StructFieldValue("panic_count", persistence.Uint64Value(s.PanicCount)),
		// Exclude "events" field to avoid updating. Should update events only from sendEvent.
	)
}

func taskStateStructTypeString() string {
	return `Struct<
		id: Utf8,
		idempotency_key: Utf8,
		account_id: Utf8,
		task_type: Utf8,
		regular: Bool,
		description: Utf8,
		created_at: Timestamp,
		created_by: Utf8,
		modified_at: Timestamp,
		generation_id: Uint64,
		status: Int64,
		error_code: Int64,
		error_message: Utf8,
		error_silent: Bool,
		error_details: String,
		retriable_error_count: Uint64,
		request: String,
		state: String,
		metadata: String,
		dependencies: String,
		changed_state_at: Timestamp,
		ended_at: Timestamp,
		last_host: Utf8,
		last_runner: Utf8,
		zone_id: Utf8,
		estimated_time: Timestamp,
		inflight_duration: Interval,
		stalling_duration: Interval,
		waiting_duration: Interval,
		dependants: String,
		panic_count: Uint64>`
}

func taskStateTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("idempotency_key", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("account_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("task_type", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("regular", persistence.Optional(persistence.TypeBool)),
		persistence.WithColumn("description", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("created_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("created_by", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("modified_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("generation_id", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("status", persistence.Optional(persistence.TypeInt64)),
		persistence.WithColumn("error_code", persistence.Optional(persistence.TypeInt64)),
		persistence.WithColumn("error_message", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("error_silent", persistence.Optional(persistence.TypeBool)),
		persistence.WithColumn("error_details", persistence.Optional(persistence.TypeBytes)),
		persistence.WithColumn("retriable_error_count", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("request", persistence.Optional(persistence.TypeBytes)),
		persistence.WithColumn("state", persistence.Optional(persistence.TypeBytes)),
		persistence.WithColumn("metadata", persistence.Optional(persistence.TypeBytes)),
		persistence.WithColumn("dependencies", persistence.Optional(persistence.TypeBytes)),
		persistence.WithColumn("changed_state_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("ended_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("last_host", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("last_runner", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("cloud_id", persistence.Optional(persistence.TypeUTF8)),  // deprecated
		persistence.WithColumn("folder_id", persistence.Optional(persistence.TypeUTF8)), // deprecated
		persistence.WithColumn("estimated_time", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("inflight_duration", persistence.Optional(persistence.TypeInterval)),
		persistence.WithColumn("stalling_duration", persistence.Optional(persistence.TypeInterval)),
		persistence.WithColumn("waiting_duration", persistence.Optional(persistence.TypeInterval)),
		persistence.WithColumn("dependants", persistence.Optional(persistence.TypeBytes)),
		persistence.WithColumn("panic_count", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("events", persistence.Optional(persistence.TypeBytes)),
		persistence.WithPrimaryKeyColumn("id"),
	)
}

func readyToExecuteTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("generation_id", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("task_type", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),

		persistence.WithPrimaryKeyColumn("id"),
	)
}

func readyToRunTableDescription() persistence.CreateTableDescription {
	return readyToExecuteTableDescription()
}

func readyToCancelTableDescription() persistence.CreateTableDescription {
	return readyToExecuteTableDescription()
}

func readyToExecuteStructTypeString() string {
	return `Struct<
		id: Utf8,
		generation_id: Uint64,
		task_type: Utf8,
		zone_id: Utf8>`
}

func executingTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("generation_id", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("task_type", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),

		persistence.WithColumn("modified_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithPrimaryKeyColumn("id"),
	)
}

func runningTableDescription() persistence.CreateTableDescription {
	return executingTableDescription()
}

func cancellingTableDescription() persistence.CreateTableDescription {
	return executingTableDescription()
}

func executingStructTypeString() string {
	return `Struct<
		id: Utf8,
		generation_id: Uint64,
		modified_at: Timestamp,
		task_type: Utf8,
		zone_id: Utf8>`
}

func endedTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("ended_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("idempotency_key", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("account_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithPrimaryKeyColumn("ended_at", "id"),
	)
}

func schedulesTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("task_type", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("scheduled_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("tasks_inflight", persistence.Optional(persistence.TypeUint64)),
		persistence.WithPrimaryKeyColumn("task_type"),
	)
}

func (s *storageYDB) scanTaskState(res persistence.Result) (state TaskState, err error) {
	var (
		errorCode    int64
		errorDetails []byte
		metadata     []byte
		dependencies []byte
		dependants   []byte
		events       []byte
	)
	err = res.ScanNamed(
		persistence.OptionalWithDefault("id", &state.ID),
		persistence.OptionalWithDefault("idempotency_key", &state.IdempotencyKey),
		persistence.OptionalWithDefault("account_id", &state.AccountID),
		persistence.OptionalWithDefault("task_type", &state.TaskType),
		persistence.OptionalWithDefault("regular", &state.Regular),
		persistence.OptionalWithDefault("description", &state.Description),
		persistence.OptionalWithDefault("created_at", &state.CreatedAt),
		persistence.OptionalWithDefault("created_by", &state.CreatedBy),
		persistence.OptionalWithDefault("modified_at", &state.ModifiedAt),
		persistence.OptionalWithDefault("generation_id", &state.GenerationID),
		persistence.OptionalWithDefault("status", &state.Status),
		persistence.OptionalWithDefault("error_code", &errorCode),
		persistence.OptionalWithDefault("error_message", &state.ErrorMessage),
		persistence.OptionalWithDefault("error_silent", &state.ErrorSilent),
		persistence.OptionalWithDefault("error_details", &errorDetails),
		persistence.OptionalWithDefault("retriable_error_count", &state.RetriableErrorCount),
		persistence.OptionalWithDefault("request", &state.Request),
		persistence.OptionalWithDefault("state", &state.State),
		persistence.OptionalWithDefault("metadata", &metadata),
		persistence.OptionalWithDefault("dependencies", &dependencies),
		persistence.OptionalWithDefault("changed_state_at", &state.ChangedStateAt),
		persistence.OptionalWithDefault("ended_at", &state.EndedAt),
		persistence.OptionalWithDefault("last_host", &state.LastHost),
		persistence.OptionalWithDefault("last_runner", &state.LastRunner),
		persistence.OptionalWithDefault("zone_id", &state.ZoneID),
		persistence.OptionalWithDefault("estimated_time", &state.EstimatedTime),
		persistence.OptionalWithDefault("inflight_duration", &state.InflightDuration),
		persistence.OptionalWithDefault("stalling_duration", &state.StallingDuration),
		persistence.OptionalWithDefault("waiting_duration", &state.WaitingDuration),
		persistence.OptionalWithDefault("dependants", &dependants),
		persistence.OptionalWithDefault("panic_count", &state.PanicCount),
		persistence.OptionalWithDefault("events", &events),
	)
	if err != nil {
		return
	}

	state.StorageFolder = s.folder
	state.ErrorCode = grpc_codes.Code(errorCode)
	state.ErrorDetails, err = unmarshalErrorDetails(errorDetails)
	if err != nil {
		return
	}

	metadataValues, err := common.UnmarshalStringMap(metadata)
	if err != nil {
		return TaskState{}, errors.NewNonRetriableError(err)
	}

	state.Metadata = NewMetadata(metadataValues)

	depsValues, err := common.UnmarshalStrings(dependencies)
	if err != nil {
		return TaskState{}, errors.NewNonRetriableError(err)
	}

	state.Dependencies = common.NewStringSet(depsValues...)

	dependantValues, err := common.UnmarshalStrings(dependants)
	if err != nil {
		return TaskState{}, errors.NewNonRetriableError(err)
	}

	state.dependants = common.NewStringSet(dependantValues...)

	eventsValues, err := common.UnmarshalInts(events)
	if err != nil {
		return TaskState{}, errors.NewNonRetriableError(err)
	}

	state.Events = eventsValues
	return
}

func (s *storageYDB) scanTaskStates(ctx context.Context, res persistence.Result) ([]TaskState, error) {
	var states []TaskState
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			state, err := s.scanTaskState(res)
			if err != nil {
				return nil, err
			}

			states = append(states, state)
		}
	}

	return states, nil
}

////////////////////////////////////////////////////////////////////////////////

func CreateYDBTables(
	ctx context.Context,
	config *tasks_config.TasksConfig,
	db *persistence.YDBClient,
	dropUnusedColumns bool,
) error {

	logging.Info(ctx, "Creating tables for tasks in %v", db.AbsolutePath(config.GetStorageFolder()))

	err := db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"tasks",
		taskStateTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created tasks table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"task_ids",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("task_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("idempotency_key", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("account_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("idempotency_key", "account_id"),
			persistence.WithSecondaryKeyColumn("task_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created task_ids table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"ready_to_run",
		readyToRunTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created ready_to_run table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"ready_to_cancel",
		readyToCancelTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created ready_to_cancel table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"running",
		runningTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created running table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"cancelling",
		cancellingTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created cancelling table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"ended",
		endedTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created ended table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"schedules",
		schedulesTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created schedules table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"nodes",
		nodeTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created nodes table")

	err = persistence.CreateAvailabilityMonitoringYDBTables(
		ctx,
		config.GetStorageFolder(),
		db,
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created availability monitoring tables")

	logging.Info(ctx, "Created tables for tasks")

	return nil
}

func DropYDBTables(
	ctx context.Context,
	config *tasks_config.TasksConfig,
	db *persistence.YDBClient,
) error {

	logging.Info(ctx, "Dropping tables for tasks in %v", db.AbsolutePath(config.GetStorageFolder()))

	err := db.DropTable(ctx, config.GetStorageFolder(), "tasks")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped tasks table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "task_ids")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped task_ids table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "ready_to_run")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped ready_to_run table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "ready_to_cancel")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped ready_to_cancel table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "running")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped running table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "cancelling")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped cancelling table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "ended")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped ended table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "schedules")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped schedules table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "nodes")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped nodes table")

	err = persistence.DropAvailabilityMonitoringYDBTables(
		ctx,
		config.GetStorageFolder(),
		db,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped availability monitoring tables")

	logging.Info(ctx, "Dropped tables for tasks")

	return nil
}
