package storage

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
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

	values := make([]persistence.Value, 0, len(entries))
	for _, entry := range entries {
		values = append(values, backupChunkQueueEntryValue(entry))
	}

	_, err = s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $entries as List<Struct<snapshot_id: Utf8, chunk_id: Utf8>>;

		upsert into backup_chunk_queue
		select *
		from AS_TABLE($entries)
	`, s.tablesPath),
		persistence.ValueParam("$entries", persistence.ListValue(values...)),
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
		declare $limit as Uint64;

		select snapshot_id, chunk_id
		from backup_chunk_queue
		limit $limit
	`, s.tablesPath),
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
		declare $snapshot_id as Utf8;

		select chunk_id
		from backup_chunk_queue
		where snapshot_id = $snapshot_id
		limit 1
	`, s.tablesPath),
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

	values := make([]persistence.Value, 0, len(entries))
	for _, entry := range entries {
		values = append(values, backupChunkQueueEntryValue(entry))
	}

	_, err = s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $keys as List<Struct<snapshot_id: Utf8, chunk_id: Utf8>>;

		delete from backup_chunk_queue
		on select * from AS_TABLE($keys)
	`, s.tablesPath),
		persistence.ValueParam("$keys", persistence.ListValue(values...)),
	)
	return err
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

	return count, res.Err()
}

////////////////////////////////////////////////////////////////////////////////

func backupChunkQueueEntryValue(entry BackupChunkQueueEntry) persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue(
			"snapshot_id",
			persistence.UTF8Value(entry.SnapshotID),
		),
		persistence.StructFieldValue(
			"chunk_id",
			persistence.UTF8Value(entry.ChunkID),
		),
	)
}
