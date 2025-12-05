package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/storage/protos"
	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	"google.golang.org/protobuf/types/known/timestamppb"
)

////////////////////////////////////////////////////////////////////////////////

type storageYDB struct {
	db         *persistence.YDBClient
	tablesPath string
}

func NewStorage(
	db *persistence.YDBClient,
	tablesPath string,
) Storage {

	return &storageYDB{
		db:         db,
		tablesPath: db.AbsolutePath(tablesPath),
	}
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) createFilesystemSnapshot(
	ctx context.Context,
	session *persistence.Session,
	snapshotMeta FilesystemSnapshotMeta,
) (created *FilesystemSnapshotMeta, err error) {

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
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotMeta.ID)),
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
			return nil, task_errors.NewSilentNonRetriableErrorf(
				"can't create already deleting filesystem snapshot with id %v",
				snapshotMeta.ID,
			)
		}

		// Should be idempotent.
		return state.toFilesystemSnapshotMeta(), nil
	}

	state := filesystemSnapshotState{
		id:           snapshotMeta.ID,
		createTaskID: snapshotMeta.CreateTaskID,
		creatingAt:   time.Now(),
		status:       filesystemSnapshotStatusCreating,
	}
	if snapshotMeta.Filesystem != nil {
		state.zoneID = snapshotMeta.Filesystem.ZoneId
		state.filesystemID = snapshotMeta.Filesystem.FilesystemId
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into filesystem_snapshots
		select *
		from AS_TABLE($states)
	`, s.tablesPath, filesystemSnapshotStateStructTypeString()),
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
	size uint64,
	storageSize uint64,
	chunkCount uint32,
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
	`, s.tablesPath),
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

		return task_errors.NewNonRetriableErrorf(
			"filesystem snapshot with id %v is not found",
			snapshotID,
		)
	}

	state := states[0]

	if state.status == filesystemSnapshotStatusReady {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	if state.status != filesystemSnapshotStatusCreating {
		return task_errors.NewSilentNonRetriableErrorf(
			"filesystem snapshot with id %v and status %v can't be created",
			snapshotID,
			filesystemSnapshotStatusToString(state.status),
		)
	}

	state.status = filesystemSnapshotStatusReady
	state.createdAt = time.Now()
	state.size = size
	state.storageSize = storageSize
	state.chunkCount = chunkCount

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into filesystem_snapshots
		select *
		from AS_TABLE($states)
	`, s.tablesPath, filesystemSnapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) deletingFilesystemSnapshot(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	taskID string,
) (deleting *FilesystemSnapshotMeta, err error) {

	deletingAt := time.Now()

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
	`, s.tablesPath),
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

	var state filesystemSnapshotState

	if len(states) != 0 {
		state = states[0]
		logging.Info(ctx, "Deleting filesystem snapshot %+v", *state.toFilesystemSnapshotMeta())

		if state.status >= filesystemSnapshotStatusDeleting {
			// Snapshot already marked as deleting.

			err = tx.Commit(ctx)
			if err != nil {
				return nil, err
			}

			// Should be idempotent.
			return state.toFilesystemSnapshotMeta(), err
		}

		if len(state.lockTaskID) != 0 && state.lockTaskID != taskID {
			err = tx.Commit(ctx)
			if err != nil {
				return nil, err
			}

			logging.Info(
				ctx,
				"Filesystem snapshot with id %v is locked and can't be deleted",
				snapshotID,
			)
			// Prevent deletion.
			return nil, task_errors.NewInterruptExecutionError()
		}
	}

	state.id = snapshotID
	state.status = filesystemSnapshotStatusDeleting
	state.deletingAt = deletingAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into filesystem_snapshots
		select *
		from AS_TABLE($states)
	`, s.tablesPath, filesystemSnapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return nil, err
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $deleting_at as Timestamp;
		declare $snapshot_id as Utf8;

		upsert into deleting (deleting_at, snapshot_id)
		values ($deleting_at, $snapshot_id)
	`, s.tablesPath),
		persistence.ValueParam("$deleting_at", persistence.TimestampValue(deletingAt)),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return nil, err
	}

	return state.toFilesystemSnapshotMeta(), tx.Commit(ctx)
}

func (s *storageYDB) lockFilesystemSnapshot(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	lockTaskID string,
) (locked bool, err error) {

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
		from filesystem_snapshots
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return false, err
	}
	defer res.Close()

	states, err := scanFilesystemSnapshotStates(ctx, res)
	if err != nil {
		return false, err
	}

	if len(states) == 0 {
		return false, tx.Commit(ctx)
	}

	state := states[0]
	if state.status >= filesystemSnapshotStatusDeleting {
		return false, tx.Commit(ctx)
	}

	if len(state.lockTaskID) != 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return false, err
		}

		if state.lockTaskID == lockTaskID {
			// Should be idempotent.
			return true, nil
		}

		logging.Info(ctx, "Another lock %v was found for filesystem snapshot %v", lockTaskID, snapshotID)
		return false, task_errors.NewInterruptExecutionError()
	}

	state.lockTaskID = lockTaskID

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into filesystem_snapshots
		select *
		from AS_TABLE($states)
	`, s.tablesPath, filesystemSnapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return false, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return false, err
	}

	logging.Info(ctx, "Locked filesystem snapshot with id %v", snapshotID)
	return true, nil
}

func (s *storageYDB) unlockFilesystemSnapshot(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	lockTaskID string,
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
	`, s.tablesPath),
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
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	state := states[0]
	if state.status >= filesystemSnapshotStatusDeleting {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	if len(state.lockTaskID) == 0 {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	if state.lockTaskID != lockTaskID {
		// Our lock is not present, so it's a success.
		return tx.Commit(ctx)
	}

	state.lockTaskID = ""

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into filesystem_snapshots
		select *
		from AS_TABLE($states)
	`, s.tablesPath, filesystemSnapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	logging.Info(ctx, "Unlocked filesystem snapshot with id %v", snapshotID)
	return nil
}

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
	`, s.tablesPath),
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

func (s *storageYDB) listFilesystemSnapshots(
	ctx context.Context,
	session *persistence.Session,
) (tasks_common.StringSet, error) {

	snapshots := tasks_common.NewStringSet()
	res, err := session.StreamExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshotStatusReady as Int64;
		select id
		from filesystem_snapshots where status = $snapshotStatusReady
	`, s.tablesPath),
		persistence.ValueParam(
			"$snapshotStatusReady",
			persistence.Int64Value(int64(filesystemSnapshotStatusReady)),
		),
	)
	if err != nil {
		return snapshots, err
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var id *string
			err := res.Scan(&id)
			if err != nil {
				return snapshots, err
			}

			snapshots.Add(*id)
		}
	}

	err = res.Err()
	if err != nil {
		return snapshots, task_errors.NewRetriableError(err)
	}

	return snapshots, nil
}

func (s *storageYDB) getFilesystemSnapshot(
	ctx context.Context,
	snapshotID string,
) (state *filesystemSnapshotState, err error) {

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from filesystem_snapshots
		where id = $id
	`, s.tablesPath),
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

	if len(states) != 0 {
		return &states[0], nil
	} else {
		return nil, nil
	}
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) CreateFilesystemSnapshot(
	ctx context.Context,
	snapshotMeta FilesystemSnapshotMeta,
) (*FilesystemSnapshotMeta, error) {

	var created *FilesystemSnapshotMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			created, err = s.createFilesystemSnapshot(
				ctx,
				session,
				snapshotMeta,
			)
			return err
		},
	)
	return created, err
}

func (s *storageYDB) FilesystemSnapshotCreated(
	ctx context.Context,
	snapshotID string,
	size uint64,
	storageSize uint64,
	chunkCount uint32,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.filesystemSnapshotCreated(
				ctx,
				session,
				snapshotID,
				size,
				storageSize,
				chunkCount,
			)
		},
	)
}

func (s *storageYDB) DeletingFilesystemSnapshot(
	ctx context.Context,
	snapshotID string,
	taskID string,
) (*FilesystemSnapshotMeta, error) {

	var snapshotMeta *FilesystemSnapshotMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			snapshotMeta, err = s.deletingFilesystemSnapshot(
				ctx,
				session,
				snapshotID,
				taskID,
			)
			return err
		},
	)
	return snapshotMeta, err
}

func (s *storageYDB) GetFilesystemSnapshotsToDelete(
	ctx context.Context,
	deletingBefore time.Time,
	limit int,
) (keys []*protos.DeletingFilesystemSnapshotKey, err error) {

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $deleting_before as Timestamp;
		declare $limit as Uint64;

		select *
		from deleting
		where deleting_at < $deleting_before
		limit $limit
	`, s.tablesPath),
		persistence.ValueParam("$deleting_before", persistence.TimestampValue(deletingBefore)),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			key := &protos.DeletingFilesystemSnapshotKey{}
			var deletingAt time.Time
			err = res.ScanNamed(
				persistence.OptionalWithDefault("deleting_at", &deletingAt),
				persistence.OptionalWithDefault("snapshot_id", &key.SnapshotId),
			)
			if err != nil {
				return nil, err
			}

			key.DeletingAt = timestamppb.New(deletingAt)
			keys = append(keys, key)
		}
	}

	return keys, nil
}

func (s *storageYDB) ClearDeletingFilesystemSnapshots(
	ctx context.Context,
	keys []*protos.DeletingFilesystemSnapshotKey,
) error {

	for _, key := range keys {
		_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $deleting_at as Timestamp;
			declare $snapshot_id as Utf8;
			declare $status as Int64;

			delete from filesystem_snapshots
			where id = $snapshot_id and status = $status;

			delete from deleting
			where deleting_at = $deleting_at and snapshot_id = $snapshot_id
		`, s.tablesPath),
			persistence.ValueParam("$deleting_at", persistence.TimestampValue(key.DeletingAt.AsTime())),
			persistence.ValueParam("$snapshot_id", persistence.UTF8Value(key.SnapshotId)),
			persistence.ValueParam("$status", persistence.Int64Value(int64(filesystemSnapshotStatusDeleting))),
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *storageYDB) CheckFilesystemSnapshotAlive(
	ctx context.Context,
	snapshotID string,
) error {

	state, err := s.getFilesystemSnapshot(ctx, snapshotID)
	if err != nil {
		return err
	}

	if state == nil {
		return task_errors.NewSilentNonRetriableErrorf(
			"filesystem snapshot with id %v is not found",
			snapshotID,
		)
	}

	if state.status >= filesystemSnapshotStatusDeleting {
		return task_errors.NewSilentNonRetriableErrorf(
			"filesystem snapshot with id %v status %v is not alive",
			snapshotID,
			filesystemSnapshotStatusToString(state.status),
		)
	}

	return nil
}

func (s *storageYDB) LockFilesystemSnapshot(
	ctx context.Context,
	snapshotID string,
	lockTaskID string,
) (locked bool, err error) {

	err = s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			locked, err = s.lockFilesystemSnapshot(ctx, session, snapshotID, lockTaskID)
			return err
		},
	)
	return locked, err
}

func (s *storageYDB) UnlockFilesystemSnapshot(
	ctx context.Context,
	snapshotID string,
	lockTaskID string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.unlockFilesystemSnapshot(ctx, session, snapshotID, lockTaskID)
		},
	)
}

func (s *storageYDB) GetFilesystemSnapshotMeta(
	ctx context.Context,
	snapshotID string,
) (*FilesystemSnapshotMeta, error) {

	var snapshotMeta *FilesystemSnapshotMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			snapshotMeta, err = s.getFilesystemSnapshotMeta(
				ctx,
				session,
				snapshotID,
			)
			return err
		},
	)
	return snapshotMeta, err
}

func (s *storageYDB) ListFilesystemSnapshots(
	ctx context.Context,
) (ids tasks_common.StringSet, err error) {

	err = s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			ids, err = s.listFilesystemSnapshots(ctx, session)
			return err
		},
	)
	return ids, err
}

func (s *storageYDB) GetFilesystemSnapshotCount(ctx context.Context) (count uint64, err error) {
	res, err := s.db.ExecuteRO(
		ctx,
		fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $status as Int64;

		select count(*)
		from filesystem_snapshots
		where status = $status;
	`, s.tablesPath),
		persistence.ValueParam(
			"$status",
			persistence.Int64Value(int64(filesystemSnapshotStatusReady)),
		),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return 0, nil
	}

	err = res.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (s *storageYDB) GetTotalFilesystemSnapshotSize(ctx context.Context) (size uint64, err error) {
	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $status as Int64;

		select sum(size)
		from filesystem_snapshots
		where status = $status;
	`, s.tablesPath),
		persistence.ValueParam(
			"$status",
			persistence.Int64Value(int64(filesystemSnapshotStatusReady)),
		),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return 0, nil
	}

	err = res.ScanWithDefaults(&size)
	if err != nil {
		return 0, err
	}

	return size, nil
}

func (s *storageYDB) GetTotalFilesystemSnapshotStorageSize(ctx context.Context) (storageSize uint64, err error) {
	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $status as Int64;

		select sum(storage_size)
		from filesystem_snapshots
		where status = $status;
	`, s.tablesPath),
		persistence.ValueParam(
			"$status",
			persistence.Int64Value(int64(filesystemSnapshotStatusReady)),
		),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return 0, nil
	}

	err = res.ScanWithDefaults(&storageSize)
	if err != nil {
		return 0, err
	}

	return storageSize, nil
}
