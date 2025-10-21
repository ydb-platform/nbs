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
