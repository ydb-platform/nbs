package storage

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type backupChunkStatus int64

const (
	backupChunkStatusQueued backupChunkStatus = iota
	backupChunkStatusCopied backupChunkStatus = iota
)

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) EnqueueBackupChunks(
	ctx context.Context,
	entries []BackupChunkQueueEntry,
) (err error) {

	defer s.metrics.StatOperation("EnqueueBackupChunks")(&err)

	if len(entries) == 0 {
		return nil
	}

	_, err = s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $entries as List<%v>;

		upsert into backup_chunk_queue
		select *
		from AS_TABLE($entries)
	`, s.tablesPath, backupChunkQueueKeyStructTypeString()),
		persistence.ValueParam(
			"$entries",
			backupChunkQueueKeyListValue(backupChunkStatusQueued, entries),
		),
	)
	return err
}

func (s *storageYDB) GetBackupChunkQueue(
	ctx context.Context,
	limit int,
) (entries []BackupChunkQueueEntry, err error) {

	defer s.metrics.StatOperation("GetBackupChunkQueue")(&err)

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $status as Int64;
		declare $limit as Uint64;

		select snapshot_id, chunk_id
		from backup_chunk_queue
		where status = $status
		limit $limit
	`, s.tablesPath),
		persistence.ValueParam(
			"$status",
			persistence.Int64Value(int64(backupChunkStatusQueued)),
		),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var entry BackupChunkQueueEntry
			err = res.ScanNamed(
				persistence.OptionalWithDefault("snapshot_id", &entry.SnapshotID),
				persistence.OptionalWithDefault("chunk_id", &entry.ChunkID),
			)
			if err != nil {
				return nil, err
			}

			entries = append(entries, entry)
		}
	}

	return entries, res.Err()
}

func (s *storageYDB) HasBackupChunkQueueEntries(
	ctx context.Context,
	snapshotID string,
) (has bool, err error) {

	defer s.metrics.StatOperation("HasBackupChunkQueueEntries")(&err)

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $status as Int64;
		declare $snapshot_id as Utf8;

		select chunk_id
		from backup_chunk_queue
		where status = $status and snapshot_id = $snapshot_id
		limit 1
	`, s.tablesPath),
		persistence.ValueParam(
			"$status",
			persistence.Int64Value(int64(backupChunkStatusQueued)),
		),
		persistence.ValueParam(
			"$snapshot_id",
			persistence.UTF8Value(snapshotID),
		),
	)
	if err != nil {
		return false, err
	}
	defer res.Close()

	has = res.NextResultSet(ctx) && res.NextRow()
	return has, res.Err()
}

func (s *storageYDB) ChunksBackupCompleted(
	ctx context.Context,
	entries []BackupChunkQueueEntry,
) (err error) {

	defer s.metrics.StatOperation("ChunksBackupCompleted")(&err)

	if len(entries) == 0 {
		return nil
	}

	_, err = s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $queued as List<%v>;
		declare $copied as List<%v>;

		delete from backup_chunk_queue
		on select * from AS_TABLE($queued);

		upsert into backup_chunk_queue
		select * from AS_TABLE($copied);
	`,
		s.tablesPath,
		backupChunkQueueKeyStructTypeString(),
		backupChunkQueueKeyStructTypeString(),
	),
		persistence.ValueParam(
			"$queued",
			backupChunkQueueKeyListValue(backupChunkStatusQueued, entries),
		),
		persistence.ValueParam(
			"$copied",
			backupChunkQueueKeyListValue(backupChunkStatusCopied, entries),
		),
	)
	return err
}

func (s *storageYDB) DeleteCopiedBackupChunks(
	ctx context.Context,
	snapshotID string,
	limit int,
) (deleted int, err error) {

	defer s.metrics.StatOperation("DeleteCopiedBackupChunks")(&err)

	res, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $status as Int64;
		declare $snapshot_id as Utf8;
		declare $limit as Uint64;

		$keys = (
			select status, snapshot_id, chunk_id
			from backup_chunk_queue
			where status = $status and snapshot_id = $snapshot_id
			limit $limit
		);

		select count(*) from $keys;

		delete from backup_chunk_queue
		on select * from $keys;
	`, s.tablesPath),
		persistence.ValueParam(
			"$status",
			persistence.Int64Value(int64(backupChunkStatusCopied)),
		),
		persistence.ValueParam(
			"$snapshot_id",
			persistence.UTF8Value(snapshotID),
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

	return int(count), res.Err()
}

func (s *storageYDB) GetBackupChunkQueueLength(
	ctx context.Context,
) (count uint64, err error) {

	defer s.metrics.StatOperation("GetBackupChunkQueueLength")(&err)

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $status as Int64;

		select count(*)
		from backup_chunk_queue
		where status = $status
	`, s.tablesPath),
		persistence.ValueParam(
			"$status",
			persistence.Int64Value(int64(backupChunkStatusQueued)),
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

	return count, res.Err()
}

////////////////////////////////////////////////////////////////////////////////

func backupChunkQueueKeyStructTypeString() string {
	return "Struct<status: Int64, snapshot_id: Utf8, chunk_id: Utf8>"
}

func backupChunkQueueKeyListValue(
	status backupChunkStatus,
	entries []BackupChunkQueueEntry,
) persistence.Value {

	values := make([]persistence.Value, 0, len(entries))
	for _, entry := range entries {
		values = append(values, persistence.StructValue(
			persistence.StructFieldValue(
				"status",
				persistence.Int64Value(int64(status)),
			),
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
