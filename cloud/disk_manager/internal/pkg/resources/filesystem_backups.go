package resources

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
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

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) filesystemBackupExists(
	ctx context.Context,
	tx *persistence.Transaction,
	backupID string,
) (bool, error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select count(*)
		from filesystem_backups
		where id = $id
	`, s.filesystemBackupsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(backupID)),
	)
	if err != nil {
		return false, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return false, nil
	}

	var count uint64
	err = res.ScanWithDefaults(&count)
	if err != nil {
		return false, err
	}

	return count != 0, nil
}

func (s *storageYDB) getFilesystemBackupMeta(
	ctx context.Context,
	session *persistence.Session,
	backupID string,
) (*FilesystemBackupMeta, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from filesystem_backups
		where id = $id
	`, s.filesystemBackupsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(backupID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	states, err := scanFilesystemBackupStates(ctx, res)
	if err != nil {
		return nil, err
	}

	if len(states) != 0 {
		return states[0].toFilesystemBackupMeta(), nil
	}
	return nil, nil
}

func (s *storageYDB) createFilesystemBackup(
	ctx context.Context,
	session *persistence.Session,
	backup FilesystemBackupMeta,
) (FilesystemBackupMeta, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return FilesystemBackupMeta{}, err
	}
	defer tx.Rollback(ctx)

	createRequest, err := proto.Marshal(backup.CreateRequest)
	if err != nil {
		return FilesystemBackupMeta{}, errors.NewNonRetriableErrorf(
			"failed to marshal create request for filesystem backup with id %v: %w",
			backup.ID,
			err,
		)
	}

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from filesystem_backups
		where id = $id
	`, s.filesystemBackupsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(backup.ID)),
	)
	if err != nil {
		return FilesystemBackupMeta{}, err
	}
	defer res.Close()

	states, err := scanFilesystemBackupStates(ctx, res)
	if err != nil {
		return FilesystemBackupMeta{}, err
	}

	if len(states) != 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return FilesystemBackupMeta{}, err
		}

		state := states[0]

		if state.status >= filesystemBackupStatusDeleting {
			logging.Info(ctx, "can't create already deleting/deleted filesystem backup with id %v", backup.ID)
			return FilesystemBackupMeta{}, errors.NewSilentNonRetriableErrorf(
				"can't create already deleting/deleted filesystem backup with id %v",
				backup.ID,
			)
		}

		// Check idempotency.
		if bytes.Equal(state.createRequest, createRequest) &&
			state.createTaskID == backup.CreateTaskID &&
			state.createdBy == backup.CreatedBy {

			return *state.toFilesystemBackupMeta(), nil
		}

		return FilesystemBackupMeta{}, errors.NewNonCancellableErrorf(
			"filesystem backup with different params already exists, old=%v, new=%v",
			state,
			backup,
		)
	}

	state := filesystemBackupState{
		id:                backup.ID,
		folderID:          backup.FolderID,
		zoneID:            backup.Filesystem.ZoneId,
		filesystemID:      backup.Filesystem.FilesystemId,
		createRequest:     createRequest,
		createTaskID:      backup.CreateTaskID,
		creatingAt:        backup.CreatingAt,
		createdBy:         backup.CreatedBy,
		useDataplaneTasks: backup.UseDataplaneTasks,
		status:            filesystemBackupStatusCreating,
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into filesystem_backups
		select *
		from AS_TABLE($states)
	`, s.filesystemBackupsPath, filesystemBackupStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return FilesystemBackupMeta{}, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return FilesystemBackupMeta{}, err
	}

	return *state.toFilesystemBackupMeta(), nil
}

func (s *storageYDB) filesystemBackupCreated(
	ctx context.Context,
	session *persistence.Session,
	backupID string,
	checkpointID string,
	createdAt time.Time,
	backupSize uint64,
	backupStorageSize uint64,
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
		from filesystem_backups
		where id = $id
	`, s.filesystemBackupsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(backupID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanFilesystemBackupStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"filesystem backup with id %v is not found",
			backupID,
		)
	}

	state := states[0]

	if state.status == filesystemBackupStatusReady {
		if state.checkpointID != checkpointID {
			return errors.NewNonRetriableErrorf(
				"filesystem backup with id %v and checkpoint id %v can't be created, "+
					"because filesystem backup with the same id and another "+
					"checkpoint id %v already exists",
				backupID,
				checkpointID,
				state.checkpointID,
			)
		}

		// Nothing to do.
		return tx.Commit(ctx)
	}

	if state.status != filesystemBackupStatusCreating {
		return errors.NewSilentNonRetriableErrorf(
			"filesystem backup with id %v and status %v can't be created",
			backupID,
			filesystemBackupStatusToString(state.status),
		)
	}

	state.status = filesystemBackupStatusReady
	state.checkpointID = checkpointID
	state.createdAt = createdAt
	state.size = backupSize
	state.storageSize = backupStorageSize

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into filesystem_backups
		select *
		from AS_TABLE($states)
	`, s.filesystemBackupsPath, filesystemBackupStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) deleteFilesystemBackup(
	ctx context.Context,
	session *persistence.Session,
	backupID string,
	taskID string,
	deletingAt time.Time,
) (*FilesystemBackupMeta, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from filesystem_backups
		where id = $id
	`, s.filesystemBackupsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(backupID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	states, err := scanFilesystemBackupStates(ctx, res)
	if err != nil {
		return nil, err
	}

	if len(states) == 0 {
		// Should be idempotent.
		return nil, nil
	}

	state := states[0]

	if state.status >= filesystemBackupStatusDeleting {
		// Filesystem backup already marked as deleting/deleted.
		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}

		return state.toFilesystemBackupMeta(), nil
	}

	state.id = backupID
	state.status = filesystemBackupStatusDeleting
	state.deleteTaskID = taskID
	state.deletingAt = deletingAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into filesystem_backups
		select *
		from AS_TABLE($states)
	`, s.filesystemBackupsPath, filesystemBackupStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return state.toFilesystemBackupMeta(), nil
}

func (s *storageYDB) filesystemBackupDeleted(
	ctx context.Context,
	session *persistence.Session,
	backupID string,
	deletedAt time.Time,
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
		from filesystem_backups
		where id = $id
	`, s.filesystemBackupsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(backupID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanFilesystemBackupStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		// It's possible that filesystem backup is already collected.
		return tx.Commit(ctx)
	}

	state := states[0]

	if state.status == filesystemBackupStatusDeleted {
		// Nothing to do.
		return tx.Commit(ctx)
	}

	if state.status != filesystemBackupStatusDeleting {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"filesystem backup with id %v and status %v can't be deleted",
			backupID,
			filesystemBackupStatusToString(state.status),
		)
	}

	state.status = filesystemBackupStatusDeleted
	state.deletedAt = deletedAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;
		upsert into filesystem_backups

		select *
		from AS_TABLE($states)
	`, s.filesystemBackupsPath, filesystemBackupStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return err
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $deleted_at as Timestamp;
		declare $filesystem_backup_id as Utf8;

		upsert into deleted_filesystem_backups (deleted_at, filesystem_backup_id)
		values ($deleted_at, $filesystem_backup_id)
	`, s.filesystemBackupsPath),
		persistence.ValueParam("$deleted_at", persistence.TimestampValue(deletedAt)),
		persistence.ValueParam("$filesystem_backup_id", persistence.UTF8Value(backupID)),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) clearDeletedFilesystemBackups(
	ctx context.Context,
	session *persistence.Session,
	deletedBefore time.Time,
	limit int,
) error {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $deleted_before as Timestamp;
		declare $limit as Uint64;

		select *
		from deleted_filesystem_backups
		where deleted_at < $deleted_before
		limit $limit
	`, s.filesystemBackupsPath),
		persistence.ValueParam("$deleted_before", persistence.TimestampValue(deletedBefore)),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var (
				deletedAt          time.Time
				filesystemBackupID string
			)
			err = res.ScanNamed(
				persistence.OptionalWithDefault("deleted_at", &deletedAt),
				persistence.OptionalWithDefault("filesystem_backup_id", &filesystemBackupID),
			)
			if err != nil {
				return err
			}

			_, err = session.ExecuteRW(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				declare $deleted_at as Timestamp;
				declare $filesystem_backup_id as Utf8;
				declare $status as Int64;

				delete from filesystem_backups
				where id = $filesystem_backup_id and status = $status;

				delete from deleted_filesystem_backups
				where deleted_at = $deleted_at and filesystem_backup_id = $filesystem_backup_id
			`, s.filesystemBackupsPath),
				persistence.ValueParam("$deleted_at", persistence.TimestampValue(deletedAt)),
				persistence.ValueParam("$filesystem_backup_id", persistence.UTF8Value(filesystemBackupID)),
				persistence.ValueParam("$status", persistence.Int64Value(int64(filesystemBackupStatusDeleted))),
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *storageYDB) listFilesystemBackups(
	ctx context.Context,
	session *persistence.Session,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	return listResources(
		ctx,
		session,
		s.filesystemBackupsPath,
		"filesystem_backups",
		folderID,
		creatingBefore,
	)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) CreateFilesystemBackup(
	ctx context.Context,
	backup FilesystemBackupMeta,
) (FilesystemBackupMeta, error) {

	var created FilesystemBackupMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			created, err = s.createFilesystemBackup(ctx, session, backup)
			return err
		},
	)
	return created, err
}

func (s *storageYDB) FilesystemBackupCreated(
	ctx context.Context,
	backupID string,
	checkpointID string,
	createdAt time.Time,
	backupSize uint64,
	backupStorageSize uint64,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.filesystemBackupCreated(
				ctx,
				session,
				backupID,
				checkpointID,
				createdAt,
				backupSize,
				backupStorageSize,
			)
		},
	)
}

func (s *storageYDB) GetFilesystemBackupMeta(
	ctx context.Context,
	backupID string,
) (*FilesystemBackupMeta, error) {

	var backup *FilesystemBackupMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			backup, err = s.getFilesystemBackupMeta(ctx, session, backupID)
			return err
		},
	)
	return backup, err
}

func (s *storageYDB) DeleteFilesystemBackup(
	ctx context.Context,
	backupID string,
	taskID string,
	deletingAt time.Time,
) (*FilesystemBackupMeta, error) {

	var backup *FilesystemBackupMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			backup, err = s.deleteFilesystemBackup(ctx, session, backupID, taskID, deletingAt)
			return err
		},
	)
	return backup, err
}

func (s *storageYDB) FilesystemBackupDeleted(
	ctx context.Context,
	backupID string,
	deletedAt time.Time,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.filesystemBackupDeleted(ctx, session, backupID, deletedAt)
		},
	)
}

func (s *storageYDB) ClearDeletedFilesystemBackups(
	ctx context.Context,
	deletedBefore time.Time,
	limit int,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.clearDeletedFilesystemBackups(ctx, session, deletedBefore, limit)
		},
	)
}

func (s *storageYDB) ListFilesystemBackups(
	ctx context.Context,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	var ids []string

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			ids, err = s.listFilesystemBackups(ctx, session, folderID, creatingBefore)
			return err
		},
	)
	return ids, err
}

////////////////////////////////////////////////////////////////////////////////

func createFilesystemBackupsYDBTables(
	ctx context.Context,
	folder string,
	db *persistence.YDBClient,
	dropUnusedColumns bool,
) error {

	logging.Info(ctx, "Creating tables for filesystem backups in %v", db.AbsolutePath(folder))

	err := db.CreateOrAlterTable(
		ctx,
		folder,
		"filesystem_backups",
		filesystemBackupStateTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created filesystem_backups table")

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		"deleted_filesystem_backups",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("deleted_at", persistence.Optional(persistence.TypeTimestamp)),
			persistence.WithColumn("filesystem_backup_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("deleted_at", "filesystem_backup_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created deleted_filesystem_backups table")

	logging.Info(ctx, "Created tables for filesystem backups")

	return nil
}

func dropFilesystemBackupsYDBTables(
	ctx context.Context,
	folder string,
	db *persistence.YDBClient,
) error {

	logging.Info(ctx, "Dropping tables for filesystem backups in %v", db.AbsolutePath(folder))

	err := db.DropTable(ctx, folder, "filesystem_backups")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped filesystem_backups table")

	err = db.DropTable(ctx, folder, "deleted_filesystem_backups")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped deleted_filesystem_backups table")

	logging.Info(ctx, "Dropped tables for filesystem backups")

	return nil
}
