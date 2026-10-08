package storage

import (
	"context"
	"fmt"

	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type backupChunkStatus int64

const (
	backupChunkStatusQueued    backupChunkStatus = iota
	backupChunkStatusCompleted backupChunkStatus = iota
)

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) findBackupChunkIDsTx(
	ctx context.Context,
	tx *persistence.Transaction,
	snapshotID string,
	entries []BackupChunkQueueEntry,
) (tasks_common.StringSet, error) {

	chunkIDs := tasks_common.NewStringSet()

	var values []persistence.Value
	for _, entry := range entries {
		values = append(values, persistence.UTF8Value(entry.ChunkID))
	}

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $chunk_ids as List<Utf8>;

		select chunk_id
		from backup_chunks
		where snapshot_id = $snapshot_id and chunk_id in $chunk_ids
	`, s.tablesPath),
		persistence.ValueParam(
			"$snapshot_id",
			persistence.UTF8Value(snapshotID),
		),
		persistence.ValueParam("$chunk_ids", persistence.ListValue(values...)),
	)
	if err != nil {
		return chunkIDs, err
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var chunkID string
			err = res.ScanNamed(
				persistence.OptionalWithDefault("chunk_id", &chunkID),
			)
			if err != nil {
				return tasks_common.StringSet{}, err
			}

			chunkIDs.Add(chunkID)
		}
	}

	err = res.Err()
	if err != nil {
		return tasks_common.StringSet{}, err
	}

	return chunkIDs, nil
}

func (s *storageYDB) enqueueBackupChunks(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	entries []BackupChunkQueueEntry,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Chunks that are found are either queued or backed up already.
	found, err := s.findBackupChunkIDsTx(ctx, tx, snapshotID, entries)
	if err != nil {
		return err
	}

	var toEnqueue []BackupChunkQueueEntry
	for _, entry := range entries {
		if !found.Has(entry.ChunkID) {
			toEnqueue = append(toEnqueue, entry)
		}
	}

	if len(toEnqueue) == 0 {
		return tx.Commit(ctx)
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $chunks as List<%v>;
		declare $entries as List<%v>;

		upsert into backup_chunks
		select *
		from AS_TABLE($chunks);

		upsert into backup_chunk_queue
		select *
		from AS_TABLE($entries)
	`,
		s.tablesPath,
		backupChunkStructTypeString(),
		backupChunkQueueEntryStructTypeString(),
	),
		persistence.ValueParam(
			"$chunks",
			backupChunkListValue(toEnqueue, backupChunkStatusQueued),
		),
		persistence.ValueParam(
			"$entries",
			backupChunkQueueEntryListValue(toEnqueue),
		),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) EnqueueBackupChunks(
	ctx context.Context,
	snapshotID string,
	entries []BackupChunkQueueEntry,
) (err error) {

	defer s.metrics.StatOperation("EnqueueBackupChunks")(&err)

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.enqueueBackupChunks(ctx, session, snapshotID, entries)
		},
	)
}

func (s *storageYDB) getQueuedChunksToBackup(
	ctx context.Context,
	session *persistence.Session,
	limit int,
) ([]BackupChunkQueueEntry, error) {

	res, err := session.StreamExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $limit as Uint64;

		select snapshot_id, chunk_id, stored_in_s3
		from backup_chunk_queue
		limit $limit
	`, s.tablesPath),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var entries []BackupChunkQueueEntry
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var entry BackupChunkQueueEntry
			err = res.ScanNamed(
				persistence.OptionalWithDefault("snapshot_id", &entry.SnapshotID),
				persistence.OptionalWithDefault("chunk_id", &entry.ChunkID),
				persistence.OptionalWithDefault("stored_in_s3", &entry.StoredInS3),
			)
			if err != nil {
				return nil, err
			}

			entries = append(entries, entry)
		}
	}

	if res.Err() != nil {
		return nil, task_errors.NewRetriableError(res.Err())
	}

	return entries, nil
}

func (s *storageYDB) GetQueuedChunksToBackup(
	ctx context.Context,
	limit int,
) (entries []BackupChunkQueueEntry, err error) {

	defer s.metrics.StatOperation("GetQueuedChunksToBackup")(&err)

	err = s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			entries, err = s.getQueuedChunksToBackup(ctx, session, limit)
			return err
		},
	)
	return entries, err
}

func (s *storageYDB) GetBackedUpChunkCount(
	ctx context.Context,
	snapshotID string,
) (count uint64, err error) {

	defer s.metrics.StatOperation("GetBackedUpChunkCount")(&err)

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $status as Int64;

		select count(*)
		from backup_chunks
		where snapshot_id = $snapshot_id and status = $status
	`, s.tablesPath),
		persistence.ValueParam(
			"$snapshot_id",
			persistence.UTF8Value(snapshotID),
		),
		persistence.ValueParam(
			"$status",
			persistence.Int64Value(int64(backupChunkStatusCompleted)),
		),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return 0, res.Err()
	}

	err = res.Scan(&count)
	if err != nil {
		return 0, err
	}

	err = res.Err()
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (s *storageYDB) ChunksBackupCompleted(
	ctx context.Context,
	entries []BackupChunkQueueEntry,
) (err error) {

	defer s.metrics.StatOperation("ChunksBackupCompleted")(&err)

	// Only existing chunks are updated: a chunk that is not found has already
	// been backed up and cleared by the backup of its snapshot.
	_, err = s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $chunks as List<%v>;
		declare $keys as List<%v>;

		update backup_chunks
		on select * from AS_TABLE($chunks);

		delete from backup_chunk_queue
		on select * from AS_TABLE($keys)
	`,
		s.tablesPath,
		backupChunkStructTypeString(),
		backupChunkKeyStructTypeString(),
	),
		persistence.ValueParam(
			"$chunks",
			backupChunkListValue(entries, backupChunkStatusCompleted),
		),
		persistence.ValueParam("$keys", backupChunkKeyListValue(entries)),
	)
	return err
}

func (s *storageYDB) ClearCompletedBackupChunks(
	ctx context.Context,
	snapshotID string,
	limit int,
) (cleared int, err error) {

	defer s.metrics.StatOperation("ClearCompletedBackupChunks")(&err)

	res, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $status as Int64;
		declare $limit as Uint64;

		$keys = (
			select snapshot_id, chunk_id
			from backup_chunks
			where snapshot_id = $snapshot_id and status = $status
			limit $limit
		);

		select count(*) from $keys;

		delete from backup_chunks
		on select * from $keys;
	`, s.tablesPath),
		persistence.ValueParam(
			"$snapshot_id",
			persistence.UTF8Value(snapshotID),
		),
		persistence.ValueParam(
			"$status",
			persistence.Int64Value(int64(backupChunkStatusCompleted)),
		),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return 0, res.Err()
	}

	var count uint64
	err = res.Scan(&count)
	if err != nil {
		return 0, err
	}

	err = res.Err()
	if err != nil {
		return 0, err
	}

	return int(count), nil
}

func (s *storageYDB) GetBackupChunkQueueLength(
	ctx context.Context,
) (count uint64, err error) {

	defer s.metrics.StatOperation("GetBackupChunkQueueLength")(&err)

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";

		select count(*)
		from backup_chunk_queue
	`, s.tablesPath))
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

	err = res.Err()
	if err != nil {
		return 0, err
	}

	return count, nil
}

////////////////////////////////////////////////////////////////////////////////

func backupChunkStructTypeString() string {
	return "Struct<snapshot_id: Utf8, chunk_id: Utf8, status: Int64>"
}

func backupChunkListValue(
	entries []BackupChunkQueueEntry,
	status backupChunkStatus,
) persistence.Value {

	values := make([]persistence.Value, 0, len(entries))
	for _, entry := range entries {
		values = append(values, persistence.StructValue(
			persistence.StructFieldValue(
				"snapshot_id",
				persistence.UTF8Value(entry.SnapshotID),
			),
			persistence.StructFieldValue(
				"chunk_id",
				persistence.UTF8Value(entry.ChunkID),
			),
			persistence.StructFieldValue(
				"status",
				persistence.Int64Value(int64(status)),
			),
		))
	}

	return persistence.ListValue(values...)
}

func backupChunkKeyStructTypeString() string {
	return "Struct<snapshot_id: Utf8, chunk_id: Utf8>"
}

func backupChunkKeyListValue(
	entries []BackupChunkQueueEntry,
) persistence.Value {

	values := make([]persistence.Value, 0, len(entries))
	for _, entry := range entries {
		values = append(values, persistence.StructValue(
			persistence.StructFieldValue(
				"snapshot_id",
				persistence.UTF8Value(entry.SnapshotID),
			),
			persistence.StructFieldValue(
				"chunk_id",
				persistence.UTF8Value(entry.ChunkID),
			),
		))
	}

	return persistence.ListValue(values...)
}

func backupChunkQueueEntryStructTypeString() string {
	return "Struct<snapshot_id: Utf8, chunk_id: Utf8, stored_in_s3: Bool>"
}

func backupChunkQueueEntryListValue(
	entries []BackupChunkQueueEntry,
) persistence.Value {

	values := make([]persistence.Value, 0, len(entries))
	for _, entry := range entries {
		values = append(values, persistence.StructValue(
			persistence.StructFieldValue(
				"snapshot_id",
				persistence.UTF8Value(entry.SnapshotID),
			),
			persistence.StructFieldValue(
				"chunk_id",
				persistence.UTF8Value(entry.ChunkID),
			),
			persistence.StructFieldValue(
				"stored_in_s3",
				persistence.BoolValue(entry.StoredInS3),
			),
		))
	}

	return persistence.ListValue(values...)
}
