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

type filesystemSnapshotStatus uint32

func (s *filesystemSnapshotStatus) UnmarshalYDB(
	res persistence.RawValue,
) error {

	*s = filesystemSnapshotStatus(res.Int64())
	return nil
}

// NOTE: These values are stored in DB, do not shuffle them around.
const (
	filesystemSnapshotStatusCreating filesystemSnapshotStatus = iota
	filesystemSnapshotStatusReady
	filesystemSnapshotStatusDeleting
	filesystemSnapshotStatusDeleted
)

func filesystemSnapshotStatusToString(status filesystemSnapshotStatus) string {
	switch status {
	case filesystemSnapshotStatusCreating:
		return "creating"
	case filesystemSnapshotStatusReady:
		return "ready"
	case filesystemSnapshotStatusDeleting:
		return "deleting"
	case filesystemSnapshotStatusDeleted:
		return "deleted"
	}

	return fmt.Sprintf("unknown_%v", status)
}

////////////////////////////////////////////////////////////////////////////////

// This is mapped into a DB row. If you change this struct, make sure to update
// the mapping code.
type filesystemSnapshotState struct {
	id            string
	folderID      string
	zoneID        string
	filesystemID  string
	createRequest []byte
	createTaskID  string
	creatingAt    time.Time
	createdAt     time.Time
	deleteTaskID  string
	deletingAt    time.Time
	deletedAt     time.Time
	size          uint64
	storageSize   uint64

	status filesystemSnapshotStatus
}

func (s *filesystemSnapshotState) toFilesystemSnapshotMeta() *FilesystemSnapshotMeta {
	return &FilesystemSnapshotMeta{
		ID:       s.id,
		FolderID: s.folderID,
		Filesystem: &types.Filesystem{
			ZoneId:       s.zoneID,
			FilesystemId: s.filesystemID,
		},
		CreateTaskID: s.createTaskID,
		CreatingAt:   s.creatingAt,
		DeleteTaskID: s.deleteTaskID,
		Size:         s.size,
		StorageSize:  s.storageSize,
		Ready:        s.status == filesystemSnapshotStatusReady,
	}
}

func (s *filesystemSnapshotState) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("id", persistence.UTF8Value(s.id)),
		persistence.StructFieldValue("folder_id", persistence.UTF8Value(s.folderID)),
		persistence.StructFieldValue("zone_id", persistence.UTF8Value(s.zoneID)),
		persistence.StructFieldValue("filesystem_id", persistence.UTF8Value(s.filesystemID)),
		persistence.StructFieldValue("create_request", persistence.StringValue(s.createRequest)),
		persistence.StructFieldValue("create_task_id", persistence.UTF8Value(s.createTaskID)),
		persistence.StructFieldValue("creating_at", persistence.TimestampValue(s.creatingAt)),
		persistence.StructFieldValue("created_at", persistence.TimestampValue(s.createdAt)),
		persistence.StructFieldValue("delete_task_id", persistence.UTF8Value(s.deleteTaskID)),
		persistence.StructFieldValue("deleting_at", persistence.TimestampValue(s.deletingAt)),
		persistence.StructFieldValue("deleted_at", persistence.TimestampValue(s.deletedAt)),
		persistence.StructFieldValue("size", persistence.Uint64Value(s.size)),
		persistence.StructFieldValue("storage_size", persistence.Uint64Value(s.storageSize)),
		persistence.StructFieldValue("status", persistence.Int64Value(int64(s.status))),
	)
}

func scanFilesystemSnapshotState(
	res persistence.Result,
) (state filesystemSnapshotState, err error) {

	err = res.ScanNamed(
		persistence.OptionalWithDefault("id", &state.id),
		persistence.OptionalWithDefault("folder_id", &state.folderID),
		persistence.OptionalWithDefault("zone_id", &state.zoneID),
		persistence.OptionalWithDefault("filesystem_id", &state.filesystemID),
		persistence.OptionalWithDefault("create_request", &state.createRequest),
		persistence.OptionalWithDefault("create_task_id", &state.createTaskID),
		persistence.OptionalWithDefault("creating_at", &state.creatingAt),
		persistence.OptionalWithDefault("created_at", &state.createdAt),
		persistence.OptionalWithDefault("delete_task_id", &state.deleteTaskID),
		persistence.OptionalWithDefault("deleting_at", &state.deletingAt),
		persistence.OptionalWithDefault("deleted_at", &state.deletedAt),
		persistence.OptionalWithDefault("size", &state.size),
		persistence.OptionalWithDefault("storage_size", &state.storageSize),
		persistence.OptionalWithDefault("status", &state.status),
	)
	return
}

func scanFilesystemSnapshotStates(
	ctx context.Context,
	res persistence.Result,
) ([]filesystemSnapshotState, error) {

	var states []filesystemSnapshotState
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			state, err := scanFilesystemSnapshotState(res)
			if err != nil {
				return nil, err
			}

			states = append(states, state)
		}
	}

	return states, nil
}

func filesystemSnapshotStateStructTypeString() string {
	return `Struct<
		id: Utf8,
		folder_id: Utf8,
		zone_id: Utf8,
		filesystem_id: Utf8,
		create_request: String,
		create_task_id: Utf8,
		creating_at: Timestamp,
		created_at: Timestamp,
		delete_task_id: Utf8,
		deleting_at: Timestamp,
		deleted_at: Timestamp,
		size: Uint64,
		storage_size: Uint64,
		status: Int64>`
}

func filesystemSnapshotStateTableDescription() persistence.CreateTableDescription {

	return persistence.NewCreateTableDescription(
		persistence.WithColumn("id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("folder_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("filesystem_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("create_request", persistence.Optional(persistence.TypeString)),
		persistence.WithColumn("create_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("creating_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("created_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("delete_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("deleting_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("deleted_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("size", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("storage_size", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("status", persistence.Optional(persistence.TypeInt64)),
		persistence.WithPrimaryKeyColumn("id"),
	)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) getFilesystemSnapshotMeta(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
) (*FilesystemSnapshotMeta, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from filesystem_snapshots
		where id = $id
	`, s.filesystemSnapshotsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	states, err := scanFilesystemSnapshotStates(ctx, res)
	if err != nil {
		return nil, err
	}

	if len(states) == 0 {
		return nil, nil
	}

	return states[0].toFilesystemSnapshotMeta(), nil
}

func (s *storageYDB) createFilesystemSnapshot(
	ctx context.Context,
	session *persistence.Session,
	snapshot FilesystemSnapshotMeta,
) (*FilesystemSnapshotMeta, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	createRequest, err := proto.Marshal(snapshot.CreateRequest)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to marshal create request for filesystem snapshot with id %v: %w",
			snapshot.ID,
			err,
		)
	}

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from filesystem_snapshots
		where id = $id
	`, s.filesystemSnapshotsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshot.ID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	states, err := scanFilesystemSnapshotStates(ctx, res)
	if err != nil {
		return nil, err
	}

	if len(states) != 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}

		state := states[0]

		if state.status >= filesystemSnapshotStatusDeleting {
			logging.Info(ctx, "can't create already deleting/deleted filesystem snapshot with id %v", snapshot.ID)
			return nil, errors.NewSilentNonRetriableErrorf(
				"can't create already deleting/deleted filesystem snapshot with id %v",
				snapshot.ID,
			)
		}

		// Check idempotency.
		if bytes.Equal(state.createRequest, createRequest) &&
			state.createTaskID == snapshot.CreateTaskID {
			return state.toFilesystemSnapshotMeta(), nil
		}

		return nil, errors.NewNonCancellableErrorf(
			"filesystem snapshot with different params already exists, old=%v, new=%v",
			state,
			snapshot,
		)
	}

	state := filesystemSnapshotState{
		id:            snapshot.ID,
		folderID:      snapshot.FolderID,
		zoneID:        snapshot.Filesystem.ZoneId,
		filesystemID:  snapshot.Filesystem.FilesystemId,
		createRequest: createRequest,
		createTaskID:  snapshot.CreateTaskID,
		creatingAt:    snapshot.CreatingAt,
		status:        filesystemSnapshotStatusCreating,
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into filesystem_snapshots
		select *
		from AS_TABLE($states)
	`, s.filesystemSnapshotsPath, filesystemSnapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return state.toFilesystemSnapshotMeta(), nil
}

func (s *storageYDB) filesystemSnapshotCreated(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	createdAt time.Time,
	snapshotSize uint64,
	snapshotStorageSize uint64,
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
		from filesystem_snapshots
		where id = $id
	`, s.filesystemSnapshotsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanFilesystemSnapshotStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"filesystem snapshot with id %v is not found",
			snapshotID,
		)
	}

	state := states[0]

	if state.status == filesystemSnapshotStatusReady {
		// Nothing to do.
		return tx.Commit(ctx)
	}

	if state.status != filesystemSnapshotStatusCreating {
		return errors.NewSilentNonRetriableErrorf(
			"filesystem snapshot with id %v and status %v can't be created",
			snapshotID,
			filesystemSnapshotStatusToString(state.status),
		)
	}

	state.status = filesystemSnapshotStatusReady
	state.createdAt = createdAt
	state.size = snapshotSize
	state.storageSize = snapshotStorageSize

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into filesystem_snapshots
		select *
		from AS_TABLE($states)
	`, s.filesystemSnapshotsPath, filesystemSnapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) deleteFilesystemSnapshot(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	taskID string,
	deletingAt time.Time,
) (*FilesystemSnapshotMeta, error) {

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
		from filesystem_snapshots
		where id = $id
	`, s.filesystemSnapshotsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	states, err := scanFilesystemSnapshotStates(ctx, res)
	if err != nil {
		return nil, err
	}

	if len(states) == 0 {
		// Should be idempotent.
		return nil, nil
	}

	state := states[0]

	if state.status >= filesystemSnapshotStatusDeleting {
		// Filesystem snapshot already marked as deleting/deleted.
		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}

		return state.toFilesystemSnapshotMeta(), nil
	}

	state.status = filesystemSnapshotStatusDeleting
	state.deleteTaskID = taskID
	state.deletingAt = deletingAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into filesystem_snapshots
		select *
		from AS_TABLE($states)
	`, s.filesystemSnapshotsPath, filesystemSnapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return state.toFilesystemSnapshotMeta(), nil
}

func (s *storageYDB) filesystemSnapshotDeleted(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
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
		from filesystem_snapshots
		where id = $id
	`, s.filesystemSnapshotsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanFilesystemSnapshotStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		// It's possible that filesystem snapshot is already collected.
		return tx.Commit(ctx)
	}

	state := states[0]

	if state.status == filesystemSnapshotStatusDeleted {
		// Nothing to do.
		return tx.Commit(ctx)
	}

	if state.status != filesystemSnapshotStatusDeleting {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"filesystem snapshot with id %v and status %v can't be deleted",
			snapshotID,
			filesystemSnapshotStatusToString(state.status),
		)
	}

	state.status = filesystemSnapshotStatusDeleted
	state.deletedAt = deletedAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;
		upsert into filesystem_snapshots

		select *
		from AS_TABLE($states)
	`, s.filesystemSnapshotsPath, filesystemSnapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return err
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $deleted_at as Timestamp;
		declare $filesystem_snapshot_id as Utf8;

		upsert into deleted (deleted_at, filesystem_snapshot_id)
		values ($deleted_at, $filesystem_snapshot_id)
	`, s.filesystemSnapshotsPath),
		persistence.ValueParam("$deleted_at", persistence.TimestampValue(deletedAt)),
		persistence.ValueParam("$filesystem_snapshot_id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) clearDeletedFilesystemSnapshots(
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
	`, s.filesystemSnapshotsPath),
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
				deletedAt            time.Time
				filesystemSnapshotID string
			)
			err = res.ScanNamed(
				persistence.OptionalWithDefault("deleted_at", &deletedAt),
				persistence.OptionalWithDefault("filesystem_snapshot_id", &filesystemSnapshotID),
			)
			if err != nil {
				return err
			}

			_, err = session.ExecuteRW(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				declare $deleted_at as Timestamp;
				declare $filesystem_snapshot_id as Utf8;
				declare $status as Int64;

				delete from filesystem_snapshots
				where id = $filesystem_snapshot_id and status = $status;

				delete from deleted
				where deleted_at = $deleted_at and filesystem_snapshot_id = $filesystem_snapshot_id
			`, s.filesystemSnapshotsPath),
				persistence.ValueParam("$deleted_at", persistence.TimestampValue(deletedAt)),
				persistence.ValueParam("$filesystem_snapshot_id", persistence.UTF8Value(filesystemSnapshotID)),
				persistence.ValueParam("$status", persistence.Int64Value(int64(filesystemSnapshotStatusDeleted))),
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *storageYDB) listFilesystemSnapshots(
	ctx context.Context,
	session *persistence.Session,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	return listResources(
		ctx,
		session,
		s.filesystemSnapshotsPath,
		"filesystem_snapshots",
		folderID,
		creatingBefore,
	)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) CreateFilesystemSnapshot(
	ctx context.Context,
	snapshot FilesystemSnapshotMeta,
) (*FilesystemSnapshotMeta, error) {

	var created *FilesystemSnapshotMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			created, err = s.createFilesystemSnapshot(ctx, session, snapshot)
			return err
		},
	)
	return created, err
}

func (s *storageYDB) FilesystemSnapshotCreated(
	ctx context.Context,
	snapshotID string,
	createdAt time.Time,
	snapshotSize uint64,
	snapshotStorageSize uint64,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.filesystemSnapshotCreated(
				ctx,
				session,
				snapshotID,
				createdAt,
				snapshotSize,
				snapshotStorageSize,
			)
		},
	)
}

func (s *storageYDB) GetFilesystemSnapshotMeta(
	ctx context.Context,
	snapshotID string,
) (*FilesystemSnapshotMeta, error) {

	var snapshot *FilesystemSnapshotMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			snapshot, err = s.getFilesystemSnapshotMeta(ctx, session, snapshotID)
			return err
		},
	)
	return snapshot, err
}

func (s *storageYDB) DeleteFilesystemSnapshot(
	ctx context.Context,
	snapshotID string,
	taskID string,
	deletingAt time.Time,
) (*FilesystemSnapshotMeta, error) {

	var snapshot *FilesystemSnapshotMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			snapshot, err = s.deleteFilesystemSnapshot(
				ctx,
				session,
				snapshotID,
				taskID,
				deletingAt,
			)
			return err
		},
	)
	return snapshot, err
}

func (s *storageYDB) FilesystemSnapshotDeleted(
	ctx context.Context,
	snapshotID string,
	deletedAt time.Time,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.filesystemSnapshotDeleted(ctx, session, snapshotID, deletedAt)
		},
	)
}

func (s *storageYDB) ClearDeletedFilesystemSnapshots(
	ctx context.Context,
	deletedBefore time.Time,
	limit int,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.clearDeletedFilesystemSnapshots(ctx, session, deletedBefore, limit)
		},
	)
}

func (s *storageYDB) ListFilesystemSnapshots(
	ctx context.Context,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	var ids []string

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			ids, err = s.listFilesystemSnapshots(ctx, session, folderID, creatingBefore)
			return err
		},
	)
	return ids, err
}

////////////////////////////////////////////////////////////////////////////////

func createFilesystemSnapshotsYDBTables(
	ctx context.Context,
	folder string,
	db *persistence.YDBClient,
	dropUnusedColumns bool,
) error {

	logging.Info(ctx, "Creating tables for filesystem snapshots in %v", db.AbsolutePath(folder))

	err := db.CreateOrAlterTable(
		ctx,
		folder,
		"filesystem_snapshots",
		filesystemSnapshotStateTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}

	logging.Info(ctx, "Created filesystem_snapshots table")

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		"deleted",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("deleted_at", persistence.Optional(persistence.TypeTimestamp)),
			persistence.WithColumn("filesystem_snapshot_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("deleted_at", "filesystem_snapshot_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}

	logging.Info(ctx, "Created deleted table")

	logging.Info(ctx, "Created tables for filesystem snapshots")

	return nil
}

func dropFilesystemSnapshotsYDBTables(
	ctx context.Context,
	folder string,
	db *persistence.YDBClient,
) error {

	logging.Info(ctx, "Dropping tables for filesystem snapshots in %v", db.AbsolutePath(folder))
	err := db.DropTable(ctx, folder, "filesystem_snapshots")
	if err != nil {
		return err
	}

	logging.Info(ctx, "Dropped filesystem_snapshots table")
	err = db.DropTable(ctx, folder, "deleted")
	if err != nil {
		return err
	}

	logging.Info(ctx, "Dropped deleted table")
	logging.Info(ctx, "Dropped tables for filesystem snapshots")

	return nil
}
