package storage

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) RelocateChunkToS3(
	ctx context.Context,
	chunkID string,
) (err error) {

	defer s.metrics.StatOperation("RelocateChunkToS3")(&err)

	if s.chunkStorageS3 == nil {
		return task_errors.NewNonRetriableErrorf(
			"S3 chunk storage is not configured",
		)
	}

	if len(chunkID) == 0 {
		return task_errors.NewNonRetriableErrorf("chunkID should not be empty")
	}

	data, checksum, compression, err := s.readChunkBlobRaw(ctx, chunkID)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		// Blob payload already cleared — verify S3 object is present.
		// Checksum/compression in YDB are zeroed after relocate, so only
		// existence/metadata presence is checked.
		return s.chunkStorageS3.CheckCompressedChunkData(
			ctx,
			chunkID,
			nil, // do not compare payload / ydb checksum
			0,
			"",
		)
	}

	err = s.chunkStorageS3.PutCompressedChunkData(
		ctx,
		chunkID,
		data,
		checksum,
		compression,
	)
	if err != nil {
		return err
	}

	return s.chunkStorageS3.CheckCompressedChunkData(
		ctx,
		chunkID,
		data,
		checksum,
		compression,
	)
}

func (s *storageYDB) RelocateSnapshotChunksToS3(
	ctx context.Context,
	snapshotID string,
	milestoneChunkIndex uint32,
	workerCount uint32,
	saveProgress func(context.Context, uint32) error,
) (err error) {

	defer s.metrics.StatOperation("RelocateSnapshotChunksToS3")(&err)

	if s.chunkStorageS3 == nil {
		return task_errors.NewNonRetriableErrorf(
			"S3 chunk storage is not configured",
		)
	}

	workers := int(workerCount)
	if workers == 0 {
		workers = s.relocateChunksToS3WorkerCount
	}
	if workers == 0 {
		workers = 1
	}

	inflightLimit := s.shallowCopyInflightLimit
	if inflightLimit < workers {
		inflightLimit = workers
	}

	processedIndices := make(chan uint32, inflightLimit)

	inflightQueue := common.NewInflightQueue(
		common.Milestone{Value: milestoneChunkIndex},
		processedIndices,
		common.ChannelWithCancellation{}, // holeValues
		inflightLimit,
	)
	defer inflightQueue.Close()

	waitSaver := func() error { return nil }
	var saverError <-chan error

	if saveProgress != nil {
		waitSaver, saverError = common.ProgressSaver(
			ctx,
			func(ctx context.Context) error {
				return saveProgress(ctx, inflightQueue.Milestone().Value)
			},
		)
		defer waitSaver()
	}

	var entries <-chan ChunkMapEntry
	var errors <-chan error

	err = s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			entries, errors = s.readChunkMap(
				ctx,
				session,
				snapshotID,
				milestoneChunkIndex,
				inflightQueue,
			)
			return nil
		},
	)
	if err != nil {
		return err
	}

	err = s.processChunkMapEntries(
		ctx,
		entries,
		workers,
		func(ctx context.Context, entry ChunkMapEntry) error {
			if len(entry.ChunkID) != 0 && !entry.StoredInS3 {
				err := s.RelocateChunkToS3(ctx, entry.ChunkID)
				if err != nil {
					return err
				}
			}

			select {
			case processedIndices <- entry.ChunkIndex:
			case <-ctx.Done():
				return ctx.Err()
			case err := <-saverError:
				return err
			}

			return nil
		},
	)
	if err != nil {
		return err
	}

	err = <-errors
	if err != nil {
		return err
	}

	err = waitSaver()
	if err != nil {
		return err
	}

	// Cutover at the end of the snapshot: flip this snapshot's map entries.
	err = s.flipSnapshotChunkMapToS3(ctx, snapshotID)
	if err != nil {
		return err
	}

	// Clear YDB payload for every chunk of this snapshot that is fully on S3.
	// Must cover the whole map (not only the resumed milestone range).
	err = s.clearSnapshotChunkBlobsIfFullyRelocated(ctx, snapshotID)
	if err != nil {
		return err
	}

	logging.Info(ctx, "relocated snapshot %v chunks to s3", snapshotID)
	return nil
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) readChunkBlobRaw(
	ctx context.Context,
	chunkID string,
) (data []byte, checksum uint32, compression string, err error) {

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $chunk_id as Utf8;

		select data, checksum, compression
		from chunk_blobs
		where shard_id = $shard_id and
			chunk_id = $chunk_id and
			referer = "";
	`, s.tablesPath),
		persistence.ValueParam("$shard_id", persistence.Uint64Value(makeShardID(chunkID))),
		persistence.ValueParam("$chunk_id", persistence.UTF8Value(chunkID)),
	)
	if err != nil {
		return nil, 0, "", err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return nil, 0, "", task_errors.NewNonRetriableErrorf(
			"chunk not found: %v",
			chunkID,
		)
	}

	err = res.ScanNamed(
		persistence.OptionalWithDefault("data", &data),
		persistence.OptionalWithDefault("checksum", &checksum),
		persistence.OptionalWithDefault("compression", &compression),
	)
	if err != nil {
		return nil, 0, "", err
	}

	return data, checksum, compression, nil
}

func (s *storageYDB) flipSnapshotChunkMapToS3(
	ctx context.Context,
	snapshotID string,
) error {

	_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $snapshot_id as Utf8;

		$to_update = (
			select
				shard_id,
				snapshot_id,
				chunk_index,
				true as stored_in_s3
			from chunk_map
			where shard_id = $shard_id and
				snapshot_id = $snapshot_id and
				chunk_id != "" and
				(stored_in_s3 is null or stored_in_s3 = false)
		);

		update chunk_map on
		select * from $to_update;
	`, s.tablesPath),
		persistence.ValueParam("$shard_id", persistence.Uint64Value(makeShardID(snapshotID))),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
	)
	return err
}

func (s *storageYDB) clearSnapshotChunkBlobsIfFullyRelocated(
	ctx context.Context,
	snapshotID string,
) error {

	entries, errors := s.ReadChunkMap(ctx, snapshotID, 0)
	var firstErr error
	for entry := range entries {
		if firstErr != nil || len(entry.ChunkID) == 0 {
			continue
		}
		err := s.clearChunkBlobDataIfFullyRelocated(ctx, entry.ChunkID)
		if err != nil {
			firstErr = err
		}
	}
	if err := <-errors; firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (s *storageYDB) clearChunkBlobDataIfFullyRelocated(
	ctx context.Context,
	chunkID string,
) error {

	stillInYDB, err := s.hasChunkMapEntriesStoredInYDB(ctx, chunkID)
	if err != nil {
		return err
	}
	if stillInYDB {
		return nil
	}

	_, err = s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $chunk_id as Utf8;

		update chunk_blobs
		set
			data = "",
			checksum = cast(0 as Uint32),
			compression = ""
		where shard_id = $shard_id and
			chunk_id = $chunk_id and
			referer = "";
	`, s.tablesPath),
		persistence.ValueParam("$shard_id", persistence.Uint64Value(makeShardID(chunkID))),
		persistence.ValueParam("$chunk_id", persistence.UTF8Value(chunkID)),
	)
	return err
}

func (s *storageYDB) hasChunkMapEntriesStoredInYDB(
	ctx context.Context,
	chunkID string,
) (bool, error) {

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $chunk_id as Utf8;

		select chunk_id
		from chunk_map
		where chunk_id = $chunk_id and
			(stored_in_s3 is null or stored_in_s3 = false)
		limit 1;
	`, s.tablesPath),
		persistence.ValueParam("$chunk_id", persistence.UTF8Value(chunkID)),
	)
	if err != nil {
		return false, err
	}
	defer res.Close()

	if res.NextResultSet(ctx) && res.NextRow() {
		return true, nil
	}

	return false, nil
}
