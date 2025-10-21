package resources

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type filesystemBackupStatus uint32

func (s *filesystemBackupStatus) UnmarshalYDB(res persistence.RawValue) error {
	*s = filesystemBackupStatus(res.Int64())
	return nil
}

// NOTE: These values are stored in DB, do not shuffle them around.
const (
	filesystemBackupStatusCreating filesystemBackupStatus = iota
	filesystemBackupStatusReady
	filesystemBackupStatusDeleting
	filesystemBackupStatusDeleted
)

func filesystemBackupStatusToString(status filesystemBackupStatus) string {
	switch status {
	case filesystemBackupStatusCreating:
		return "creating"
	case filesystemBackupStatusReady:
		return "ready"
	case filesystemBackupStatusDeleting:
		return "deleting"
	case filesystemBackupStatusDeleted:
		return "deleted"
	}

	return fmt.Sprintf("unknown_%v", status)
}

////////////////////////////////////////////////////////////////////////////////

// This is mapped into a DB row. If you change this struct, make sure to update
// the mapping code.
type filesystemBackupState struct {
	id                string
	folderID          string
	zoneID            string
	filesystemID      string
	checkpointID      string
	createRequest     []byte
	createTaskID      string
	creatingAt        time.Time
	createdAt         time.Time
	createdBy         string
	deleteTaskID      string
	deletingAt        time.Time
	deletedAt         time.Time
	useDataplaneTasks bool
	size              uint64
	storageSize       uint64

	status filesystemBackupStatus
}

func (s *filesystemBackupState) toFilesystemBackupMeta() *FilesystemBackupMeta {
	return &FilesystemBackupMeta{
		ID:       s.id,
		FolderID: s.folderID,
		Filesystem: &types.Filesystem{
			ZoneId:       s.zoneID,
			FilesystemId: s.filesystemID,
		},
		CheckpointID:      s.checkpointID,
		CreateTaskID:      s.createTaskID,
		CreatingAt:        s.creatingAt,
		CreatedBy:         s.createdBy,
		DeleteTaskID:      s.deleteTaskID,
		UseDataplaneTasks: s.useDataplaneTasks,
		Size:              s.size,
		StorageSize:       s.storageSize,
		Ready:             s.status == filesystemBackupStatusReady,
	}
}

func (s *filesystemBackupState) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("id", persistence.UTF8Value(s.id)),
		persistence.StructFieldValue("folder_id", persistence.UTF8Value(s.folderID)),
		persistence.StructFieldValue("zone_id", persistence.UTF8Value(s.zoneID)),
		persistence.StructFieldValue("filesystem_id", persistence.UTF8Value(s.filesystemID)),
		persistence.StructFieldValue("checkpoint_id", persistence.UTF8Value(s.checkpointID)),
		persistence.StructFieldValue("create_request", persistence.StringValue(s.createRequest)),
		persistence.StructFieldValue("create_task_id", persistence.UTF8Value(s.createTaskID)),
		persistence.StructFieldValue("creating_at", persistence.TimestampValue(s.creatingAt)),
		persistence.StructFieldValue("created_at", persistence.TimestampValue(s.createdAt)),
		persistence.StructFieldValue("created_by", persistence.UTF8Value(s.createdBy)),
		persistence.StructFieldValue("delete_task_id", persistence.UTF8Value(s.deleteTaskID)),
		persistence.StructFieldValue("deleting_at", persistence.TimestampValue(s.deletingAt)),
		persistence.StructFieldValue("deleted_at", persistence.TimestampValue(s.deletedAt)),
		persistence.StructFieldValue("use_dataplane_tasks", persistence.BoolValue(s.useDataplaneTasks)),
		persistence.StructFieldValue("size", persistence.Uint64Value(s.size)),
		persistence.StructFieldValue("storage_size", persistence.Uint64Value(s.storageSize)),
		persistence.StructFieldValue("status", persistence.Int64Value(int64(s.status))),
	)
}

func scanFilesystemBackupState(res persistence.Result) (state filesystemBackupState, err error) {
	err = res.ScanNamed(
		persistence.OptionalWithDefault("id", &state.id),
		persistence.OptionalWithDefault("folder_id", &state.folderID),
		persistence.OptionalWithDefault("zone_id", &state.zoneID),
		persistence.OptionalWithDefault("filesystem_id", &state.filesystemID),
		persistence.OptionalWithDefault("checkpoint_id", &state.checkpointID),
		persistence.OptionalWithDefault("create_request", &state.createRequest),
		persistence.OptionalWithDefault("create_task_id", &state.createTaskID),
		persistence.OptionalWithDefault("creating_at", &state.creatingAt),
		persistence.OptionalWithDefault("created_at", &state.createdAt),
		persistence.OptionalWithDefault("created_by", &state.createdBy),
		persistence.OptionalWithDefault("delete_task_id", &state.deleteTaskID),
		persistence.OptionalWithDefault("deleting_at", &state.deletingAt),
		persistence.OptionalWithDefault("deleted_at", &state.deletedAt),
		persistence.OptionalWithDefault("use_dataplane_tasks", &state.useDataplaneTasks),
		persistence.OptionalWithDefault("size", &state.size),
		persistence.OptionalWithDefault("storage_size", &state.storageSize),
		persistence.OptionalWithDefault("status", &state.status),
	)
	return
}

func scanFilesystemBackupStates(
	ctx context.Context,
	res persistence.Result,
) ([]filesystemBackupState, error) {

	var states []filesystemBackupState
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			state, err := scanFilesystemBackupState(res)
			if err != nil {
				return nil, err
			}

			states = append(states, state)
		}
	}

	return states, nil
}

func filesystemBackupStateStructTypeString() string {
	return `Struct<
		id: Utf8,
		folder_id: Utf8,
		zone_id: Utf8,
		filesystem_id: Utf8,
		checkpoint_id: Utf8,
		create_request: String,
		create_task_id: Utf8,
		creating_at: Timestamp,
		created_at: Timestamp,
		created_by: Utf8,
		delete_task_id: Utf8,
		deleting_at: Timestamp,
		deleted_at: Timestamp,
		use_dataplane_tasks: Bool,
		size: Uint64,
		storage_size: Uint64,
		status: Int64>`
}

func filesystemBackupStateTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("folder_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("filesystem_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("checkpoint_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("create_request", persistence.Optional(persistence.TypeString)),
		persistence.WithColumn("create_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("creating_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("created_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("created_by", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("delete_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("deleting_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("deleted_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("use_dataplane_tasks", persistence.Optional(persistence.TypeBool)),
		persistence.WithColumn("size", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("storage_size", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("status", persistence.Optional(persistence.TypeInt64)),
		persistence.WithPrimaryKeyColumn("id"),
	)
}
