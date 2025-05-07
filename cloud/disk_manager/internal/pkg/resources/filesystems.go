package resources

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type filesystemStatus uint32

func (s *filesystemStatus) UnmarshalYDB(res persistence.RawValue) error {
	*s = filesystemStatus(res.Int64())
	return nil
}

// NOTE: These values are stored in DB, do not shuffle them around.
const (
	filesystemStatusCreating filesystemStatus = iota
	filesystemStatusReady    filesystemStatus = iota
	filesystemStatusDeleting filesystemStatus = iota
	filesystemStatusDeleted  filesystemStatus = iota
)

func filesystemStatusToString(status filesystemStatus) string {
	switch status {
	case filesystemStatusCreating:
		return "creating"
	case filesystemStatusReady:
		return "ready"
	case filesystemStatusDeleting:
		return "deleting"
	case filesystemStatusDeleted:
		return "deleted"
	}

	return fmt.Sprintf("unknown_%v", status)
}

////////////////////////////////////////////////////////////////////////////////

// This is mapped into a DB row. If you change this struct, make sure to update
// the mapping code.
type filesystemState struct {
	id          string
	zoneID      string
	blocksCount uint64
	blockSize   uint32
	kind        string
	cloudID     string
	folderID    string

	createRequest []byte
	createTaskID  string
	creatingAt    time.Time
	createdAt     time.Time
	createdBy     string
	deleteTaskID  string
	deletingAt    time.Time
	deletedAt     time.Time

	status filesystemStatus

	isExternal bool
}

func (s *filesystemState) toFilesystemMeta() *FilesystemMeta {
	return &FilesystemMeta{
		ID:           s.id,
		ZoneID:       s.zoneID,
		BlocksCount:  s.blocksCount,
		BlockSize:    s.blockSize,
		Kind:         s.kind,
		CloudID:      s.cloudID,
		FolderID:     s.folderID,
		CreateTaskID: s.createTaskID,
		CreatingAt:   s.creatingAt,
		CreatedAt:    s.createdAt,
		CreatedBy:    s.createdBy,
		DeleteTaskID: s.deleteTaskID,
		IsExternal:   s.isExternal,
	}
}

func (s *filesystemState) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("id", persistence.UTF8Value(s.id)),
		persistence.StructFieldValue("zone_id", persistence.UTF8Value(s.zoneID)),
		persistence.StructFieldValue("blocks_count", persistence.Uint64Value(s.blocksCount)),
		persistence.StructFieldValue("block_size", persistence.Uint32Value(s.blockSize)),
		persistence.StructFieldValue("kind", persistence.UTF8Value(s.kind)),
		persistence.StructFieldValue("cloud_id", persistence.UTF8Value(s.cloudID)),
		persistence.StructFieldValue("folder_id", persistence.UTF8Value(s.folderID)),
		persistence.StructFieldValue("create_request", persistence.StringValue(s.createRequest)),
		persistence.StructFieldValue("create_task_id", persistence.UTF8Value(s.createTaskID)),
		persistence.StructFieldValue("creating_at", persistence.TimestampValue(s.creatingAt)),
		persistence.StructFieldValue("created_at", persistence.TimestampValue(s.createdAt)),
		persistence.StructFieldValue("created_by", persistence.UTF8Value(s.createdBy)),
		persistence.StructFieldValue("delete_task_id", persistence.UTF8Value(s.deleteTaskID)),
		persistence.StructFieldValue("deleting_at", persistence.TimestampValue(s.deletingAt)),
		persistence.StructFieldValue("deleted_at", persistence.TimestampValue(s.deletedAt)),

		persistence.StructFieldValue("status", persistence.Int64Value(int64(s.status))),

		persistence.StructFieldValue("is_external", persistence.BoolValue(s.isExternal)),
	)
}

func scanFilesystemState(res persistence.Result) (state filesystemState, err error) {
	err = res.ScanNamed(
		persistence.OptionalWithDefault("id", &state.id),
		persistence.OptionalWithDefault("zone_id", &state.zoneID),
		persistence.OptionalWithDefault("blocks_count", &state.blocksCount),
		persistence.OptionalWithDefault("block_size", &state.blockSize),
		persistence.OptionalWithDefault("kind", &state.kind),
		persistence.OptionalWithDefault("cloud_id", &state.cloudID),
		persistence.OptionalWithDefault("folder_id", &state.folderID),
		persistence.OptionalWithDefault("create_request", &state.createRequest),
		persistence.OptionalWithDefault("create_task_id", &state.createTaskID),
		persistence.OptionalWithDefault("creating_at", &state.creatingAt),
		persistence.OptionalWithDefault("created_at", &state.createdAt),
		persistence.OptionalWithDefault("created_by", &state.createdBy),
		persistence.OptionalWithDefault("delete_task_id", &state.deleteTaskID),
		persistence.OptionalWithDefault("deleting_at", &state.deletingAt),
		persistence.OptionalWithDefault("deleted_at", &state.deletedAt),
		persistence.OptionalWithDefault("status", &state.status),
		persistence.OptionalWithDefault("is_external", &state.isExternal),
	)
	return
}

func scanFilesystemStates(
	ctx context.Context,
	res persistence.Result,
) ([]filesystemState, error) {

	var states []filesystemState
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			state, err := scanFilesystemState(res)
			if err != nil {
				return nil, err
			}

			states = append(states, state)
		}
	}

	return states, nil
}

func filesystemStateStructTypeString() string {
	return `Struct<
		id: Utf8,
		zone_id: Utf8,
		blocks_count: Uint64,
		block_size: Uint32,
		kind: Utf8,
		cloud_id: Utf8,
		folder_id: Utf8,
		create_request: String,
		create_task_id: Utf8,
		creating_at: Timestamp,
		created_at: Timestamp,
		created_by: Utf8,
		delete_task_id: Utf8,
		deleting_at: Timestamp,
		deleted_at: Timestamp,
		status: Int64,
		is_external: Bool>`
}

func filesystemStateTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("blocks_count", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("block_size", persistence.Optional(persistence.TypeUint32)),
		persistence.WithColumn("kind", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("cloud_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("folder_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("create_request", persistence.Optional(persistence.TypeString)),
		persistence.WithColumn("create_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("creating_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("created_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("created_by", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("delete_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("deleting_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("deleted_at", persistence.Optional(persistence.TypeTimestamp)),

		persistence.WithColumn("status", persistence.Optional(persistence.TypeInt64)),

		persistence.WithColumn("is_external", persistence.Optional(persistence.TypeBool)),

		persistence.WithPrimaryKeyColumn("id"),
	)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) getFilesystemMeta(
	ctx context.Context,
	session *persistence.Session,
	filesystemID string,
) (*FilesystemMeta, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from filesystems
		where id = $id
	`, s.filesystemsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(filesystemID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	states, err := scanFilesystemStates(ctx, res)
	if err != nil {
		return nil, err
	}

	if len(states) != 0 {
		return states[0].toFilesystemMeta(), nil
	} else {
		return nil, nil
	}
}

func (s *storageYDB) createFilesystem(
	ctx context.Context,
	session *persistence.Session,
	filesystem FilesystemMeta,
) (*FilesystemMeta, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	createRequest, err := proto.Marshal(filesystem.CreateRequest)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to marshal create request for filesystem with id %v: %w",
			filesystem.ID,
			err,
		)
	}

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from filesystems
		where id = $id
	`, s.filesystemsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(filesystem.ID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	states, err := scanFilesystemStates(ctx, res)
	if err != nil {
		return nil, err
	}

	if len(states) != 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}

		state := states[0]

		if state.status >= filesystemStatusDeleting {
			logging.Info(ctx, "can't create already deleting/deleted filesystem with id %v", filesystem.ID)
			return nil, errors.NewSilentNonRetriableErrorf(
				"can't create already deleting/deleted filesystem with id %v",
				filesystem.ID,
			)
		}

		// Check idempotency.
		if bytes.Equal(state.createRequest, createRequest) &&
			state.createTaskID == filesystem.CreateTaskID &&
			state.createdBy == filesystem.CreatedBy {

			return state.toFilesystemMeta(), nil
		}

		logging.Info(ctx, "filesystem with different params already exists, old=%v, new=%v", state, filesystem)
		return nil, nil
	}

	state := filesystemState{
		id:            filesystem.ID,
		zoneID:        filesystem.ZoneID,
		blocksCount:   filesystem.BlocksCount,
		blockSize:     filesystem.BlockSize,
		kind:          filesystem.Kind,
		cloudID:       filesystem.CloudID,
		folderID:      filesystem.FolderID,
		createRequest: createRequest,
		createTaskID:  filesystem.CreateTaskID,
		creatingAt:    filesystem.CreatingAt,
		createdBy:     filesystem.CreatedBy,

		status: filesystemStatusCreating,

		isExternal: filesystem.IsExternal,
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into filesystems
		select *
		from AS_TABLE($states)
	`, s.filesystemsPath, filesystemStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return state.toFilesystemMeta(), nil
}

func (s *storageYDB) filesystemCreated(
	ctx context.Context,
	session *persistence.Session,
	filesystem FilesystemMeta,
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
		from filesystems
		where id = $id
	`, s.filesystemsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(filesystem.ID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanFilesystemStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"filesystem with id %v is not found",
			filesystem.ID,
		)
	}

	state := states[0]

	if state.status == filesystemStatusReady {
		// Nothing to do.
		return tx.Commit(ctx)
	}

	if state.status != filesystemStatusCreating {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"filesystem with id %v and status %v can't be created",
			filesystem.ID,
			filesystemStatusToString(state.status),
		)
	}

	state.status = filesystemStatusReady
	state.createdAt = filesystem.CreatedAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into filesystems
		select *
		from AS_TABLE($states)
	`, s.filesystemsPath, filesystemStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) deleteFilesystem(
	ctx context.Context,
	session *persistence.Session,
	filesystemID string,
	taskID string,
	deletingAt time.Time,
) (*FilesystemMeta, error) {

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
		from filesystems
		where id = $id
	`, s.filesystemsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(filesystemID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	states, err := scanFilesystemStates(ctx, res)
	if err != nil {
		return nil, err
	}

	var state filesystemState

	if len(states) != 0 {
		state = states[0]

		if state.status >= filesystemStatusDeleting {
			// Filesystem already marked as deleting/deleted.

			err = tx.Commit(ctx)
			if err != nil {
				return nil, err
			}

			return state.toFilesystemMeta(), nil
		}
	}

	state.id = filesystemID
	state.status = filesystemStatusDeleting
	state.deleteTaskID = taskID
	state.deletingAt = deletingAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into filesystems
		select *
		from AS_TABLE($states)
	`, s.filesystemsPath, filesystemStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return state.toFilesystemMeta(), nil
}

func (s *storageYDB) filesystemDeleted(
	ctx context.Context,
	session *persistence.Session,
	filesystemID string,
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
		from filesystems
		where id = $id
	`, s.filesystemsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(filesystemID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanFilesystemStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		// It's possible that filesystem is already collected.
		return tx.Commit(ctx)
	}

	state := states[0]

	if state.status == filesystemStatusDeleted {
		// Nothing to do.
		return tx.Commit(ctx)
	}

	if state.status != filesystemStatusDeleting {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"filesystem with id %v and status %v can't be deleted",
			filesystemID,
			filesystemStatusToString(state.status),
		)
	}

	state.status = filesystemStatusDeleted
	state.deletedAt = deletedAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into filesystems
		select *
		from AS_TABLE($states)
	`, s.filesystemsPath, filesystemStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return err
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $deleted_at as Timestamp;
		declare $filesystem_id as Utf8;

		upsert into deleted (deleted_at, filesystem_id)
		values ($deleted_at, $filesystem_id)
	`, s.filesystemsPath),
		persistence.ValueParam("$deleted_at", persistence.TimestampValue(deletedAt)),
		persistence.ValueParam("$filesystem_id", persistence.UTF8Value(filesystemID)),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) clearDeletedFilesystems(
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
		from deleted
		where deleted_at < $deleted_before
		limit $limit
	`, s.filesystemsPath),
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
				deletedAt    time.Time
				filesystemID string
			)
			err = res.ScanNamed(
				persistence.OptionalWithDefault("deleted_at", &deletedAt),
				persistence.OptionalWithDefault("filesystem_id", &filesystemID),
			)
			if err != nil {
				return err
			}

			_, err = session.ExecuteRW(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				declare $deleted_at as Timestamp;
				declare $filesystem_id as Utf8;
				declare $status as Int64;

				delete from filesystems
				where id = $filesystem_id and status = $status;

				delete from deleted
				where deleted_at = $deleted_at and filesystem_id = $filesystem_id
			`, s.filesystemsPath),
				persistence.ValueParam("$deleted_at", persistence.TimestampValue(deletedAt)),
				persistence.ValueParam("$filesystem_id", persistence.UTF8Value(filesystemID)),
				persistence.ValueParam("$status", persistence.Int64Value(int64(filesystemStatusDeleted))),
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *storageYDB) listFilesystems(
	ctx context.Context,
	session *persistence.Session,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	return listResources(
		ctx,
		session,
		s.filesystemsPath,
		"filesystems",
		folderID,
		creatingBefore,
	)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) CreateFilesystem(
	ctx context.Context,
	filesystem FilesystemMeta,
) (*FilesystemMeta, error) {

	var created *FilesystemMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			created, err = s.createFilesystem(ctx, session, filesystem)
			return err
		},
	)
	return created, err
}

func (s *storageYDB) FilesystemCreated(
	ctx context.Context,
	filesystem FilesystemMeta,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.filesystemCreated(ctx, session, filesystem)
		},
	)
}

func (s *storageYDB) GetFilesystemMeta(
	ctx context.Context,
	filesystemID string,
) (*FilesystemMeta, error) {

	var filesystem *FilesystemMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			filesystem, err = s.getFilesystemMeta(ctx, session, filesystemID)
			return err
		},
	)
	return filesystem, err
}

func (s *storageYDB) DeleteFilesystem(
	ctx context.Context,
	filesystemID string,
	taskID string,
	deletingAt time.Time,
) (*FilesystemMeta, error) {

	var filesystem *FilesystemMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			filesystem, err = s.deleteFilesystem(ctx, session, filesystemID, taskID, deletingAt)
			return err
		},
	)
	return filesystem, err
}

func (s *storageYDB) FilesystemDeleted(
	ctx context.Context,
	filesystemID string,
	deletedAt time.Time,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.filesystemDeleted(ctx, session, filesystemID, deletedAt)
		},
	)
}

func (s *storageYDB) ClearDeletedFilesystems(
	ctx context.Context,
	deletedBefore time.Time,
	limit int,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.clearDeletedFilesystems(ctx, session, deletedBefore, limit)
		},
	)
}

func (s *storageYDB) ListFilesystems(
	ctx context.Context,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	var ids []string

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			ids, err = s.listFilesystems(ctx, session, folderID, creatingBefore)
			return err
		},
	)
	return ids, err
}

////////////////////////////////////////////////////////////////////////////////

func createFilesystemsYDBTables(
	ctx context.Context,
	folder string,
	db *persistence.YDBClient,
	dropUnusedColumns bool,
) error {

	logging.Info(ctx, "Creating tables for filesystems in %v", db.AbsolutePath(folder))

	err := db.CreateOrAlterTable(
		ctx,
		folder,
		"filesystems",
		filesystemStateTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created filesystems table")

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		"deleted",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("deleted_at", persistence.Optional(persistence.TypeTimestamp)),
			persistence.WithColumn("filesystem_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("deleted_at", "filesystem_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created deleted table")

	logging.Info(ctx, "Created tables for filesystems")

	return nil
}

func dropFilesystemsYDBTables(
	ctx context.Context,
	folder string,
	db *persistence.YDBClient,
) error {

	logging.Info(ctx, "Dropping tables for filesystems in %v", db.AbsolutePath(folder))

	err := db.DropTable(ctx, folder, "filesystems")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped filesystems table")

	err = db.DropTable(ctx, folder, "deleted")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped deleted table")

	logging.Info(ctx, "Dropped tables for filesystems")

	return nil
}
