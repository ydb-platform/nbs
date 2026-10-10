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

type snapshotStatus uint32

func (s *snapshotStatus) UnmarshalYDB(res persistence.RawValue) error {
	*s = snapshotStatus(res.Int64())
	return nil
}

// NOTE: These values are stored in DB, do not shuffle them around.
const (
	snapshotStatusCreating snapshotStatus = iota
	snapshotStatusReady    snapshotStatus = iota
	snapshotStatusDeleting snapshotStatus = iota
	snapshotStatusDeleted  snapshotStatus = iota
)

func snapshotStatusToString(status snapshotStatus) string {
	switch status {
	case snapshotStatusCreating:
		return "creating"
	case snapshotStatusReady:
		return "ready"
	case snapshotStatusDeleting:
		return "deleting"
	case snapshotStatusDeleted:
		return "deleted"
	}

	return fmt.Sprintf("unknown_%v", status)
}

////////////////////////////////////////////////////////////////////////////////

// This is mapped into a DB row. If you change this struct, make sure to update
// the mapping code.
type snapshotState struct {
	id                string
	folderID          string
	zoneID            string
	diskID            string
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
	backupCompleted   bool
	encryptionMode    uint32
	encryptionKeyHash []byte

	status snapshotStatus
}

func (s *snapshotState) toSnapshotMeta() *SnapshotMeta {
	// TODO: Snapshot.CreateRequest should be []byte, because we can't unmarshal
	// it here, without knowing particular protobuf message type.

	return &SnapshotMeta{
		ID:       s.id,
		FolderID: s.folderID,
		Disk: &types.Disk{
			ZoneId: s.zoneID,
			DiskId: s.diskID,
		},
		CheckpointID:      s.checkpointID,
		CreateTaskID:      s.createTaskID,
		CreatingAt:        s.creatingAt,
		CreatedBy:         s.createdBy,
		DeleteTaskID:      s.deleteTaskID,
		UseDataplaneTasks: s.useDataplaneTasks,
		Size:              s.size,
		StorageSize:       s.storageSize,
		Encryption: &types.EncryptionDesc{
			Mode: types.EncryptionMode(s.encryptionMode),
			Key: &types.EncryptionDesc_KeyHash{
				KeyHash: s.encryptionKeyHash,
			},
		},
		Ready:           s.status == snapshotStatusReady,
		BackupCompleted: s.backupCompleted,
	}
}

func (s *snapshotState) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("id", persistence.UTF8Value(s.id)),
		persistence.StructFieldValue("folder_id", persistence.UTF8Value(s.folderID)),
		persistence.StructFieldValue("zone_id", persistence.UTF8Value(s.zoneID)),
		persistence.StructFieldValue("disk_id", persistence.UTF8Value(s.diskID)),
		persistence.StructFieldValue("checkpoint_id", persistence.UTF8Value(s.checkpointID)),
		persistence.StructFieldValue("create_request", persistence.StringValue(s.createRequest)),
		persistence.StructFieldValue("create_task_id", persistence.UTF8Value(s.createTaskID)),
		persistence.StructFieldValue("creating_at", persistence.TimestampValue(s.creatingAt)),
		persistence.StructFieldValue("created_at", persistence.TimestampValue(s.createdAt)),
		persistence.StructFieldValue("created_by", persistence.UTF8Value(s.createdBy)),
		persistence.StructFieldValue("delete_task_id", persistence.UTF8Value(s.deleteTaskID)),
		persistence.StructFieldValue("deleting_at", persistence.TimestampValue(s.deletingAt)),
		persistence.StructFieldValue("deleted_at", persistence.TimestampValue(s.deletedAt)),
		persistence.StructFieldValue("incremental", persistence.BoolValue(true)),         // deprecated
		persistence.StructFieldValue("use_dataplane_tasks", persistence.BoolValue(true)), // legacy
		persistence.StructFieldValue("size", persistence.Uint64Value(s.size)),
		persistence.StructFieldValue("storage_size", persistence.Uint64Value(s.storageSize)),
		persistence.StructFieldValue("encryption_mode", persistence.Uint32Value(s.encryptionMode)),
		persistence.StructFieldValue("encryption_keyhash", persistence.StringValue(s.encryptionKeyHash)),
		persistence.StructFieldValue(
			"backup_completed",
			persistence.BoolValue(s.backupCompleted),
		),
		persistence.StructFieldValue("status", persistence.Int64Value(int64(s.status))),
	)
}

func scanSnapshotState(res persistence.Result) (state snapshotState, err error) {
	err = res.ScanNamed(
		persistence.OptionalWithDefault("id", &state.id),
		persistence.OptionalWithDefault("folder_id", &state.folderID),
		persistence.OptionalWithDefault("zone_id", &state.zoneID),
		persistence.OptionalWithDefault("disk_id", &state.diskID),
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
		persistence.OptionalWithDefault("encryption_mode", &state.encryptionMode),
		persistence.OptionalWithDefault("encryption_keyhash", &state.encryptionKeyHash),
		persistence.OptionalWithDefault(
			"backup_completed",
			&state.backupCompleted,
		),
		persistence.OptionalWithDefault("status", &state.status),
	)
	return
}

func scanSnapshotStates(
	ctx context.Context,
	res persistence.Result,
) ([]snapshotState, error) {

	var states []snapshotState
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			state, err := scanSnapshotState(res)
			if err != nil {
				return nil, err
			}

			states = append(states, state)
		}
	}

	return states, nil
}

func snapshotStateStructTypeString() string {
	return `Struct<
		id: Utf8,
		folder_id: Utf8,
		zone_id: Utf8,
		disk_id: Utf8,
		checkpoint_id: Utf8,
		create_request: String,
		create_task_id: Utf8,
		creating_at: Timestamp,
		created_at: Timestamp,
		created_by: Utf8,
		delete_task_id: Utf8,
		deleting_at: Timestamp,
		deleted_at: Timestamp,
		incremental: Bool, /* deprecated */
		use_dataplane_tasks: Bool, /* legacy */
		size: Uint64,
		storage_size: Uint64,
		encryption_mode: Uint32,
		encryption_keyhash: String,
		backup_completed: Bool,
		status: Int64>`
}

func snapshotStateTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("folder_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("disk_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("checkpoint_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("create_request", persistence.Optional(persistence.TypeString)),
		persistence.WithColumn("create_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("creating_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("created_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("created_by", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("delete_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("deleting_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("deleted_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("incremental", persistence.Optional(persistence.TypeBool)),         // deprecated
		persistence.WithColumn("use_dataplane_tasks", persistence.Optional(persistence.TypeBool)), // legacy
		persistence.WithColumn("size", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("storage_size", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("encryption_mode", persistence.Optional(persistence.TypeUint32)),
		persistence.WithColumn("encryption_keyhash", persistence.Optional(persistence.TypeString)),
		persistence.WithColumn(
			"backup_completed",
			persistence.Optional(persistence.TypeBool),
		),
		persistence.WithColumn("status", persistence.Optional(persistence.TypeInt64)),
		persistence.WithPrimaryKeyColumn("id"),
	)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) snapshotExists(
	ctx context.Context,
	tx *persistence.Transaction,
	snapshotID string,
) (bool, error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select count(*)
		from snapshots
		where id = $id
	`, s.snapshotsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)),
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

func (s *storageYDB) getSnapshotMeta(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
) (*SnapshotMeta, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from snapshots
		where id = $id
	`, s.snapshotsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	states, err := scanSnapshotStates(ctx, res)
	if err != nil {
		return nil, err
	}

	if len(states) != 0 {
		return states[0].toSnapshotMeta(), nil
	} else {
		return nil, nil
	}
}

func (s *storageYDB) createSnapshot(
	ctx context.Context,
	session *persistence.Session,
	snapshot SnapshotMeta,
) (SnapshotMeta, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return SnapshotMeta{}, err
	}
	defer tx.Rollback(ctx)

	// HACK: see NBS-974 for details.
	imageExists, err := s.imageExists(ctx, tx, snapshot.ID)
	if err != nil {
		return SnapshotMeta{}, err
	}

	if imageExists {
		err = tx.Commit(ctx)
		if err != nil {
			return SnapshotMeta{}, err
		}

		return SnapshotMeta{}, errors.NewNonCancellableErrorf(
			"snapshot with id %v can't be created, because image with id %v already exists",
			snapshot.ID,
			snapshot.ID,
		)
	}

	createRequest, err := proto.Marshal(snapshot.CreateRequest)
	if err != nil {
		return SnapshotMeta{}, errors.NewNonRetriableErrorf(
			"failed to marshal create request for snapshot with id %v: %w",
			snapshot.ID,
			err,
		)
	}

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from snapshots
		where id = $id
	`, s.snapshotsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshot.ID)),
	)
	if err != nil {
		return SnapshotMeta{}, err
	}
	defer res.Close()

	states, err := scanSnapshotStates(ctx, res)
	if err != nil {
		return SnapshotMeta{}, err
	}

	if len(states) != 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return SnapshotMeta{}, err
		}

		state := states[0]

		if state.status >= snapshotStatusDeleting {
			logging.Info(ctx, "can't create already deleting/deleted snapshot with id %v", snapshot.ID)
			return SnapshotMeta{}, errors.NewSilentNonRetriableErrorf(
				"can't create already deleting/deleted snapshot with id %v",
				snapshot.ID,
			)
		}

		// Check idempotency.
		if bytes.Equal(state.createRequest, createRequest) &&
			state.createTaskID == snapshot.CreateTaskID &&
			state.createdBy == snapshot.CreatedBy {

			return *state.toSnapshotMeta(), nil
		}

		return SnapshotMeta{}, errors.NewNonCancellableErrorf(
			"snapshot with different params already exists, old=%v, new=%v",
			state,
			snapshot,
		)
	}

	state := snapshotState{
		id:                snapshot.ID,
		folderID:          snapshot.FolderID,
		zoneID:            snapshot.Disk.ZoneId,
		diskID:            snapshot.Disk.DiskId,
		createRequest:     createRequest,
		createTaskID:      snapshot.CreateTaskID,
		creatingAt:        snapshot.CreatingAt,
		createdBy:         snapshot.CreatedBy,
		useDataplaneTasks: true,

		status: snapshotStatusCreating,
	}

	encryptionMode, encryptionKeyHash, err := GetEncryptionModeAndKeyHash(
		snapshot.Encryption,
	)
	if err != nil {
		return SnapshotMeta{}, err
	}

	state.encryptionMode = uint32(encryptionMode)
	state.encryptionKeyHash = encryptionKeyHash

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into snapshots
		select *
		from AS_TABLE($states)
	`, s.snapshotsPath, snapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return SnapshotMeta{}, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return SnapshotMeta{}, err
	}

	return *state.toSnapshotMeta(), nil
}

func (s *storageYDB) snapshotCreated(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	checkpointID string,
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
		from snapshots
		where id = $id
	`, s.snapshotsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanSnapshotStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"snapshot with id %v is not found",
			snapshotID,
		)
	}

	state := states[0]

	if state.status == snapshotStatusReady {
		if state.checkpointID != checkpointID {
			return errors.NewNonRetriableErrorf(
				"snapshot with id %v and checkpoint id %v can't be created, "+
					"because snapshot with the same id and another "+
					"checkpoint id %v already exists",
				snapshotID,
				checkpointID,
				state.checkpointID,
			)
		}

		// Nothing to do.
		return tx.Commit(ctx)
	}

	if state.status != snapshotStatusCreating {
		return errors.NewSilentNonRetriableErrorf(
			"snapshot with id %v and status %v can't be created",
			snapshotID,
			snapshotStatusToString(state.status),
		)
	}

	state.status = snapshotStatusReady
	state.checkpointID = checkpointID
	state.createdAt = createdAt
	state.size = snapshotSize
	state.storageSize = snapshotStorageSize

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into snapshots
		select *
		from AS_TABLE($states)
	`, s.snapshotsPath, snapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return err
	}

	// A new ready snapshot of a configured folder is queued in the same
	// transaction. Other snapshots are queued by EnqueueSnapshotBackup. The
	// snapshot becomes ready once, so its create task identifies the attempt.
	_, backupFolder := s.backupFolderIDs[state.folderID]
	if s.backupEnabled && backupFolder {
		_, err = tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $snapshot_id as Utf8;
			declare $backup_id as Utf8;
			declare $enqueued_at as Timestamp;

			upsert into backup_queue
				(snapshot_id, backup_id, task_id, enqueued_at)
			values ($snapshot_id, $backup_id, "", $enqueued_at)
		`, s.snapshotsPath),
			persistence.ValueParam(
				"$snapshot_id",
				persistence.UTF8Value(snapshotID),
			),
			persistence.ValueParam(
				"$backup_id",
				persistence.UTF8Value(state.createTaskID),
			),
			persistence.ValueParam(
				"$enqueued_at",
				persistence.TimestampValue(time.Now()),
			),
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) deleteSnapshot(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	taskID string,
	deletingAt time.Time,
) (*SnapshotMeta, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// HACK: see NBS-974 for details.
	imageExists, err := s.imageExists(ctx, tx, snapshotID)
	if err != nil {
		return nil, err
	}

	if imageExists {
		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}

		return nil, errors.NewNonCancellableErrorf(
			"snapshot with id %v can't be deleted, because image with id %v already exists",
			snapshotID,
			snapshotID,
		)
	}

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from snapshots
		where id = $id
	`, s.snapshotsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	states, err := scanSnapshotStates(ctx, res)
	if err != nil {
		return nil, err
	}

	if len(states) == 0 {
		// Should be idempotent.
		return nil, nil
	}

	state := states[0]

	if state.status >= snapshotStatusDeleting {
		// Snapshot already marked as deleting/deleted.

		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}

		return state.toSnapshotMeta(), nil
	}

	state.id = snapshotID
	state.status = snapshotStatusDeleting
	state.deleteTaskID = taskID
	state.deletingAt = deletingAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into snapshots
		select *
		from AS_TABLE($states)
	`, s.snapshotsPath, snapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return nil, err
	}

	if s.backupEnabled {
		// A queued backup is not started anymore. A running one holds the
		// snapshot, so the deletion waits for it.
		_, err = tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $snapshot_id as Utf8;

			delete from backup_queue
			where snapshot_id = $snapshot_id
		`, s.snapshotsPath),
			persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		)
		if err != nil {
			return nil, err
		}

		if len(state.diskID) == 0 {
			return nil, errors.NewNonRetriableErrorf(
				"snapshot %v has no disk id",
				snapshotID,
			)
		}

		_, err = tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $snapshot_id as Utf8;
			declare $disk_id as Utf8;

			upsert into backup_delete_queue (snapshot_id, disk_id)
			values ($snapshot_id, $disk_id)
		`, s.snapshotsPath),
			persistence.ValueParam(
				"$snapshot_id",
				persistence.UTF8Value(snapshotID),
			),
			persistence.ValueParam(
				"$disk_id",
				persistence.UTF8Value(state.diskID),
			),
		)
		if err != nil {
			return nil, err
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return state.toSnapshotMeta(), nil
}

func (s *storageYDB) snapshotDeleted(
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
		from snapshots
		where id = $id
	`, s.snapshotsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanSnapshotStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		// It's possible that snapshot is already collected.
		return tx.Commit(ctx)
	}

	state := states[0]

	if state.status == snapshotStatusDeleted {
		// Nothing to do.
		return tx.Commit(ctx)
	}

	if state.status != snapshotStatusDeleting {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"snapshot with id %v and status %v can't be deleted",
			snapshotID,
			snapshotStatusToString(state.status),
		)
	}

	state.status = snapshotStatusDeleted
	state.deletedAt = deletedAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;
		upsert into snapshots

		select *
		from AS_TABLE($states)
	`, s.snapshotsPath, snapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return err
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $deleted_at as Timestamp;
		declare $snapshot_id as Utf8;

		upsert into deleted (deleted_at, snapshot_id)
		values ($deleted_at, $snapshot_id)
	`, s.snapshotsPath),
		persistence.ValueParam("$deleted_at", persistence.TimestampValue(deletedAt)),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) clearDeletedSnapshots(
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
	`, s.snapshotsPath),
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
				deletedAt  time.Time
				snapshotID string
			)
			err = res.ScanNamed(
				persistence.OptionalWithDefault("deleted_at", &deletedAt),
				persistence.OptionalWithDefault("snapshot_id", &snapshotID),
			)
			if err != nil {
				return err
			}

			_, err = session.ExecuteRW(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				declare $deleted_at as Timestamp;
				declare $snapshot_id as Utf8;
				declare $status as Int64;

				delete from snapshots
				where id = $snapshot_id and status = $status;

				delete from deleted
				where deleted_at = $deleted_at and snapshot_id = $snapshot_id
			`, s.snapshotsPath),
				persistence.ValueParam("$deleted_at", persistence.TimestampValue(deletedAt)),
				persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
				persistence.ValueParam("$status", persistence.Int64Value(int64(snapshotStatusDeleted))),
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *storageYDB) listSnapshots(
	ctx context.Context,
	session *persistence.Session,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	return listResources(
		ctx,
		session,
		s.snapshotsPath,
		"snapshots",
		folderID,
		creatingBefore,
	)
}

func (s *storageYDB) listSnapshotsToBackup(
	ctx context.Context,
	session *persistence.Session,
	limit int,
) ([]SnapshotBackupRequest, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $limit as Uint64;

		select snapshot_id, backup_id
		from backup_queue
		where task_id is null or task_id = ""
		order by enqueued_at
		limit $limit
	`, s.snapshotsPath),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var backups []SnapshotBackupRequest
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var backup SnapshotBackupRequest
			err = res.ScanNamed(
				persistence.OptionalWithDefault(
					"snapshot_id",
					&backup.SnapshotID,
				),
				persistence.OptionalWithDefault("backup_id", &backup.BackupID),
			)
			if err != nil {
				return nil, err
			}
			backups = append(backups, backup)
		}
	}
	return backups, res.Err()
}

func (s *storageYDB) removeSnapshotFromBackupQueue(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	backupID string,
) error {

	_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $backup_id as Utf8;

		delete from backup_queue
		where snapshot_id = $snapshot_id and backup_id = $backup_id
	`, s.snapshotsPath),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$backup_id", persistence.UTF8Value(backupID)),
	)
	return err
}

func (s *storageYDB) snapshotBackupScheduled(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	backupID string,
	taskID string,
) (time.Time, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $backup_id as Utf8;

		select enqueued_at
		from backup_queue
		where snapshot_id = $snapshot_id and backup_id = $backup_id
	`, s.snapshotsPath),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$backup_id", persistence.UTF8Value(backupID)),
	)
	if err != nil {
		return time.Time{}, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		// The attempt left the queue meanwhile: nothing to mark.
		err = res.Err()
		if err != nil {
			return time.Time{}, err
		}

		return time.Time{}, tx.Commit(ctx)
	}

	var enqueuedAt time.Time
	err = res.ScanNamed(
		persistence.OptionalWithDefault("enqueued_at", &enqueuedAt),
	)
	if err != nil {
		return time.Time{}, err
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $backup_id as Utf8;
		declare $task_id as Utf8;

		update backup_queue
		set task_id = $task_id
		where snapshot_id = $snapshot_id and backup_id = $backup_id
	`, s.snapshotsPath),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$backup_id", persistence.UTF8Value(backupID)),
		persistence.ValueParam("$task_id", persistence.UTF8Value(taskID)),
	)
	if err != nil {
		return time.Time{}, err
	}

	return enqueuedAt, tx.Commit(ctx)
}

func (s *storageYDB) listScheduledSnapshotBackups(
	ctx context.Context,
	session *persistence.Session,
	limit int,
) ([]ScheduledSnapshotBackup, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $limit as Uint64;

		select snapshot_id, backup_id, task_id
		from backup_queue
		where task_id is not null and task_id != ""
		limit $limit
	`, s.snapshotsPath),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var backups []ScheduledSnapshotBackup
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var backup ScheduledSnapshotBackup
			err = res.ScanNamed(
				persistence.OptionalWithDefault(
					"snapshot_id",
					&backup.SnapshotID,
				),
				persistence.OptionalWithDefault("backup_id", &backup.BackupID),
				persistence.OptionalWithDefault("task_id", &backup.TaskID),
			)
			if err != nil {
				return nil, err
			}
			backups = append(backups, backup)
		}
	}
	return backups, res.Err()
}

func (s *storageYDB) getSnapshotBackupQueueStats(
	ctx context.Context,
	session *persistence.Session,
) (SnapshotBackupQueueStats, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";

		select
			count_if(task_id is null or task_id = "") as queued,
			count_if(task_id is not null and task_id != "") as scheduled
		from backup_queue
	`, s.snapshotsPath))
	if err != nil {
		return SnapshotBackupQueueStats{}, err
	}
	defer res.Close()

	var stats SnapshotBackupQueueStats
	if !res.NextResultSet(ctx) || !res.NextRow() {
		return stats, res.Err()
	}

	err = res.ScanNamed(
		persistence.OptionalWithDefault("queued", &stats.Queued),
		persistence.OptionalWithDefault("scheduled", &stats.Scheduled),
	)
	if err != nil {
		return SnapshotBackupQueueStats{}, err
	}
	return stats, res.Err()
}

func (s *storageYDB) snapshotBackupCompleted(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
) error {

	// A deleting snapshot is not marked: its copy may have found the
	// snapshot already deleted and copied nothing.
	_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;
		declare $ready as Int64;

		update snapshots
		set backup_completed = true
		where id = $id and status = $ready
	`, s.snapshotsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam(
			"$ready",
			persistence.Int64Value(int64(snapshotStatusReady)),
		),
	)
	return err
}

func (s *storageYDB) listSnapshotBackupIDsForDeletion(
	ctx context.Context,
	session *persistence.Session,
	limit int,
) ([]SnapshotBackupID, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $limit as Uint64;

		select snapshot_id, disk_id
		from backup_delete_queue
		limit $limit
	`, s.snapshotsPath),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var snapshotBackupIDsForDeletion []SnapshotBackupID

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var snapshotBackupID SnapshotBackupID
			err = res.ScanNamed(
				persistence.OptionalWithDefault(
					"snapshot_id",
					&snapshotBackupID.SnapshotID,
				),
				persistence.OptionalWithDefault(
					"disk_id",
					&snapshotBackupID.DiskID,
				),
			)
			if err != nil {
				return nil, err
			}

			snapshotBackupIDsForDeletion = append(
				snapshotBackupIDsForDeletion,
				snapshotBackupID,
			)
		}
	}

	return snapshotBackupIDsForDeletion, nil
}

func (s *storageYDB) snapshotBackupDeletionsCompleted(
	ctx context.Context,
	session *persistence.Session,
	snapshotIDs []string,
) error {

	if len(snapshotIDs) == 0 {
		return nil
	}

	var snapshotIDValues []persistence.Value
	for _, snapshotID := range snapshotIDs {
		snapshotIDValues = append(
			snapshotIDValues,
			persistence.UTF8Value(snapshotID),
		)
	}

	_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_ids as List<Utf8>;

		delete from backup_delete_queue
		where snapshot_id in $snapshot_ids
	`, s.snapshotsPath),
		persistence.ValueParam(
			"$snapshot_ids",
			persistence.ListValue(snapshotIDValues...),
		),
	)
	return err
}

func (s *storageYDB) enqueueSnapshotBackup(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	backupID string,
) error {

	if len(snapshotID) == 0 || len(backupID) == 0 {
		return errors.NewNonRetriableErrorf("empty snapshot or backup ID")
	}

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select * from snapshots where id = $id
	`, s.snapshotsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanSnapshotStates(ctx, res)
	if err != nil {
		return err
	}
	if err = res.Err(); err != nil {
		return err
	}
	if len(states) == 0 {
		return errors.NewNonRetriableErrorf(
			"snapshot with id %v is not found",
			snapshotID,
		)
	}

	state := states[0]
	if state.backupCompleted {
		return tx.Commit(ctx)
	}
	if state.status != snapshotStatusReady {
		return errors.NewNonRetriableErrorf(
			"snapshot with id %v and status %v can't be backed up",
			snapshotID,
			snapshotStatusToString(state.status),
		)
	}

	queued, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;

		select snapshot_id from backup_queue where snapshot_id = $snapshot_id
	`, s.snapshotsPath),
		persistence.ValueParam(
			"$snapshot_id",
			persistence.UTF8Value(snapshotID),
		),
	)
	if err != nil {
		return err
	}
	defer queued.Close()

	found := queued.NextResultSet(ctx) && queued.NextRow()
	if err = queued.Err(); err != nil {
		return err
	}
	if found {
		return tx.Commit(ctx)
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $backup_id as Utf8;
		declare $enqueued_at as Timestamp;

		upsert into backup_queue (snapshot_id, backup_id, task_id, enqueued_at)
		values ($snapshot_id, $backup_id, "", $enqueued_at)
	`, s.snapshotsPath),
		persistence.ValueParam(
			"$snapshot_id",
			persistence.UTF8Value(snapshotID),
		),
		persistence.ValueParam("$backup_id", persistence.UTF8Value(backupID)),
		persistence.ValueParam(
			"$enqueued_at",
			persistence.TimestampValue(time.Now()),
		),
	)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) CreateSnapshot(
	ctx context.Context,
	snapshot SnapshotMeta,
) (SnapshotMeta, error) {

	var created SnapshotMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			created, err = s.createSnapshot(ctx, session, snapshot)
			return err
		},
	)
	return created, err
}

func (s *storageYDB) SnapshotCreated(
	ctx context.Context,
	snapshotID string,
	checkpointID string,
	createdAt time.Time,
	snapshotSize uint64,
	snapshotStorageSize uint64,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.snapshotCreated(
				ctx,
				session,
				snapshotID,
				checkpointID,
				createdAt,
				snapshotSize,
				snapshotStorageSize,
			)
		},
	)
}

func (s *storageYDB) GetSnapshotMeta(
	ctx context.Context,
	snapshotID string,
) (*SnapshotMeta, error) {

	var snapshot *SnapshotMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			snapshot, err = s.getSnapshotMeta(ctx, session, snapshotID)
			return err
		},
	)
	return snapshot, err
}

func (s *storageYDB) DeleteSnapshot(
	ctx context.Context,
	snapshotID string,
	taskID string,
	deletingAt time.Time,
) (*SnapshotMeta, error) {

	var snapshot *SnapshotMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			snapshot, err = s.deleteSnapshot(ctx, session, snapshotID, taskID, deletingAt)
			return err
		},
	)
	return snapshot, err
}

func (s *storageYDB) SnapshotDeleted(
	ctx context.Context,
	snapshotID string,
	deletedAt time.Time,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.snapshotDeleted(ctx, session, snapshotID, deletedAt)
		},
	)
}

func (s *storageYDB) ClearDeletedSnapshots(
	ctx context.Context,
	deletedBefore time.Time,
	limit int,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.clearDeletedSnapshots(ctx, session, deletedBefore, limit)
		},
	)
}

func (s *storageYDB) ListSnapshots(
	ctx context.Context,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	var ids []string

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			ids, err = s.listSnapshots(ctx, session, folderID, creatingBefore)
			return err
		},
	)
	return ids, err
}

func (s *storageYDB) ListSnapshotsToBackup(
	ctx context.Context,
	limit int,
) ([]SnapshotBackupRequest, error) {

	var backups []SnapshotBackupRequest

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			backups, err = s.listSnapshotsToBackup(ctx, session, limit)
			return err
		},
	)
	return backups, err
}

func (s *storageYDB) RemoveSnapshotFromBackupQueue(
	ctx context.Context,
	snapshotID string,
	backupID string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.removeSnapshotFromBackupQueue(
				ctx,
				session,
				snapshotID,
				backupID,
			)
		},
	)
}

func (s *storageYDB) SnapshotBackupCompleted(
	ctx context.Context,
	snapshotID string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.snapshotBackupCompleted(ctx, session, snapshotID)
		},
	)
}

func (s *storageYDB) EnqueueSnapshotBackup(
	ctx context.Context,
	snapshotID string,
	backupID string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.enqueueSnapshotBackup(ctx, session, snapshotID, backupID)
		},
	)
}

func (s *storageYDB) SnapshotBackupScheduled(
	ctx context.Context,
	snapshotID string,
	backupID string,
	taskID string,
) (time.Time, error) {

	var enqueuedAt time.Time

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			enqueuedAt, err = s.snapshotBackupScheduled(
				ctx,
				session,
				snapshotID,
				backupID,
				taskID,
			)
			return err
		},
	)
	return enqueuedAt, err
}

func (s *storageYDB) ListScheduledSnapshotBackups(
	ctx context.Context,
	limit int,
) ([]ScheduledSnapshotBackup, error) {

	var backups []ScheduledSnapshotBackup

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			backups, err = s.listScheduledSnapshotBackups(ctx, session, limit)
			return err
		},
	)
	return backups, err
}

func (s *storageYDB) GetSnapshotBackupQueueStats(
	ctx context.Context,
) (SnapshotBackupQueueStats, error) {

	var stats SnapshotBackupQueueStats

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			stats, err = s.getSnapshotBackupQueueStats(ctx, session)
			return err
		},
	)
	return stats, err
}

func (s *storageYDB) GetSnapshotBackupDeleteQueue(
	ctx context.Context,
	limit int,
) ([]SnapshotBackupID, error) {

	var snapshotBackupIDsForDeletion []SnapshotBackupID

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			snapshotBackupIDsForDeletion, err =
				s.listSnapshotBackupIDsForDeletion(
					ctx,
					session,
					limit,
				)
			return err
		},
	)
	return snapshotBackupIDsForDeletion, err
}

func (s *storageYDB) SnapshotBackupDeletionsCompleted(
	ctx context.Context,
	snapshotIDs []string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.snapshotBackupDeletionsCompleted(
				ctx,
				session,
				snapshotIDs,
			)
		},
	)
}

////////////////////////////////////////////////////////////////////////////////

func createSnapshotsYDBTables(
	ctx context.Context,
	folder string,
	db *persistence.YDBClient,
	dropUnusedColumns bool,
) error {

	logging.Info(ctx, "Creating tables for snapshots in %v", db.AbsolutePath(folder))

	err := db.CreateOrAlterTable(
		ctx,
		folder,
		"snapshots",
		snapshotStateTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created snapshots table")

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		"incremental",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("disk_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("snapshot_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("checkpoint_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("zone_id", "disk_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created incremental table")

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		"deleted",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("deleted_at", persistence.Optional(persistence.TypeTimestamp)),
			persistence.WithColumn("snapshot_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("deleted_at", "snapshot_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created deleted table")

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		"backup_queue",
		persistence.NewCreateTableDescription(
			persistence.WithColumn(
				"backup_id",
				persistence.Optional(persistence.TypeUTF8),
			),
			persistence.WithColumn("snapshot_id", persistence.Optional(persistence.TypeUTF8)),
			// The snapshots.BackupSnapshot task of the attempt; empty while
			// the attempt waits for a free slot.
			persistence.WithColumn(
				"task_id",
				persistence.Optional(persistence.TypeUTF8),
			),
			persistence.WithColumn(
				"enqueued_at",
				persistence.Optional(persistence.TypeTimestamp),
			),
			persistence.WithPrimaryKeyColumn("snapshot_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created backup_queue table")

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		"backup_delete_queue",
		persistence.NewCreateTableDescription(
			persistence.WithColumn(
				"snapshot_id",
				persistence.Optional(persistence.TypeUTF8),
			),
			persistence.WithColumn(
				"disk_id",
				persistence.Optional(persistence.TypeUTF8),
			),
			persistence.WithPrimaryKeyColumn("snapshot_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created backup_delete_queue table")

	logging.Info(ctx, "Created tables for snapshots")

	return nil
}

func dropSnapshotsYDBTables(
	ctx context.Context,
	folder string,
	db *persistence.YDBClient,
) error {

	logging.Info(ctx, "Dropping tables for snapshots in %v", db.AbsolutePath(folder))

	err := db.DropTable(ctx, folder, "snapshots")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped snapshots table")

	err = db.DropTable(ctx, folder, "incremental")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped incremental table")

	err = db.DropTable(ctx, folder, "deleted")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped deleted table")

	err = db.DropTable(ctx, folder, "backup_queue")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped backup_queue table")

	err = db.DropTable(ctx, folder, "backup_delete_queue")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped backup_delete_queue table")

	logging.Info(ctx, "Dropped tables for snapshots")

	return nil
}
