package storage

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	task_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
	"github.com/ydb-platform/nbs/contrib/go/cityhash"
	"google.golang.org/protobuf/types/known/timestamppb"
)

////////////////////////////////////////////////////////////////////////////////

func makeChunkID(
	uniqueID string,
	snapshotID string,
	chunk dataplane_common.Chunk,
) string {

	return fmt.Sprintf("%v.%v.%v", uniqueID, snapshotID, chunk.Index)
}

func makeShardID(s string) uint64 {
	return cityhash.Hash64([]byte(s))
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) getIncremental(
	ctx context.Context,
	session *persistence.Session,
	disk *types.Disk,
) (snapshotID string, checkpointID string, err error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback(ctx)

	snapshotID, checkpointID, err = s.getIncrementalTx(ctx, tx, disk)
	if err != nil {
		return "", "", err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return "", "", err
	}

	return snapshotID, checkpointID, err
}

func (s *storageYDB) getIncrementalTx(
	ctx context.Context,
	tx *persistence.Transaction,
	disk *types.Disk,
) (snapshotID string, checkpointID string, err error) {

	logging.Info(ctx, "Getting incremental snapshot for disk %+v", disk)
	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $zone_id as Utf8;
		declare $disk_id as Utf8;

		select *
		from incremental
		where zone_id = $zone_id and disk_id = $disk_id
	`, s.tablesPath),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(disk.ZoneId)),
		persistence.ValueParam("$disk_id", persistence.UTF8Value(disk.DiskId)),
	)
	if err != nil {
		return "", "", err
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			err = res.ScanNamed(
				persistence.OptionalWithDefault("snapshot_id", &snapshotID),
				persistence.OptionalWithDefault("checkpoint_id", &checkpointID),
			)
			if err != nil {
				return "", "", err
			}
		}
	}

	logging.Info(ctx, "snapshot %v is incremental for disk %+v", snapshotID, disk)
	return
}

func (s *storageYDB) createSnapshot(
	ctx context.Context,
	session *persistence.Session,
	snapshotMeta SnapshotMeta,
) (created *SnapshotMeta, err error) {

	defer s.metrics.StatOperation("createSnapshot")(&err)

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
		from snapshots
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotMeta.ID)),
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
		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}

		state := states[0]

		if state.status >= snapshotStatusDeleting {
			return nil, task_errors.NewSilentNonRetriableErrorf(
				"can't create already deleting snapshot with id %v",
				snapshotMeta.ID,
			)
		}

		// Should be idempotent.
		return state.toSnapshotMeta(), nil
	}

	state := snapshotState{
		id:           snapshotMeta.ID,
		createTaskID: snapshotMeta.CreateTaskID,
		creatingAt:   time.Now(),
		status:       snapshotStatusCreating,
	}
	if snapshotMeta.Disk != nil {
		state.zoneID = snapshotMeta.Disk.ZoneId
		state.diskID = snapshotMeta.Disk.DiskId
		state.checkpointID = snapshotMeta.CheckpointID

		baseSnapshotID, baseCheckpointID, err := s.getIncrementalTx(
			ctx,
			tx,
			snapshotMeta.Disk,
		)
		if err != nil {
			return nil, err
		}
		state.baseSnapshotID = baseSnapshotID
		state.baseCheckpointID = baseCheckpointID
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into snapshots
		select *
		from AS_TABLE($states)
	`, s.tablesPath, snapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return state.toSnapshotMeta(), nil
}

func (s *storageYDB) deleteDiskFromIncremental(
	ctx context.Context,
	session *persistence.Session,
	zoneID string,
	diskID string,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $zone_id as Utf8;
		declare $disk_id as Utf8;

		delete from incremental
		where zone_id = $zone_id and disk_id = $disk_id
	`, s.tablesPath),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(zoneID)),
		persistence.ValueParam("$disk_id", persistence.UTF8Value(diskID)),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) updateIncrementalTableAndSnapshotState(
	ctx context.Context,
	tx *persistence.Transaction,
	state snapshotState,
) error {

	if len(state.baseSnapshotID) == 0 {
		_, err := tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $zone_id as Utf8;
			declare $disk_id as Utf8;
			declare $snapshot_id as Utf8;
			declare $checkpoint_id as Utf8;
			declare $created_at as Timestamp;

			upsert into incremental (zone_id, disk_id, snapshot_id, checkpoint_id, created_at)
			values ($zone_id, $disk_id, $snapshot_id, $checkpoint_id, $created_at)
		`, s.tablesPath),
			persistence.ValueParam("$zone_id", persistence.UTF8Value(state.zoneID)),
			persistence.ValueParam("$disk_id", persistence.UTF8Value(state.diskID)),
			persistence.ValueParam("$snapshot_id", persistence.UTF8Value(state.id)),
			persistence.ValueParam("$checkpoint_id", persistence.UTF8Value(state.checkpointID)),
			persistence.ValueParam("$created_at", persistence.TimestampValue(state.createdAt)),
		)
		if err != nil {
			return err
		}
	} else {
		// Remove previous incremental snapshot and insert new one instead.
		_, err := tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $zone_id as Utf8;
			declare $disk_id as Utf8;
			declare $snapshot_id as Utf8;
			declare $checkpoint_id as Utf8;
			declare $base_snapshot_id as Utf8;
			declare $created_at as Timestamp;

			delete from incremental
			where zone_id = $zone_id and disk_id = $disk_id and snapshot_id = $base_snapshot_id;

			upsert into incremental (zone_id, disk_id, snapshot_id, checkpoint_id, created_at)
			values ($zone_id, $disk_id, $snapshot_id, $checkpoint_id, $created_at)
		`, s.tablesPath),
			persistence.ValueParam("$zone_id", persistence.UTF8Value(state.zoneID)),
			persistence.ValueParam("$disk_id", persistence.UTF8Value(state.diskID)),
			persistence.ValueParam("$snapshot_id", persistence.UTF8Value(state.id)),
			persistence.ValueParam("$checkpoint_id", persistence.UTF8Value(state.checkpointID)),
			persistence.ValueParam("$base_snapshot_id", persistence.UTF8Value(state.baseSnapshotID)),
			persistence.ValueParam("$created_at", persistence.TimestampValue(state.createdAt)),
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *storageYDB) snapshotCreated(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	size uint64,
	storageSize uint64,
	chunkCount uint32,
	encryption *types.EncryptionDesc,
) (err error) {

	defer s.metrics.StatOperation("snapshotCreated")(&err)

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
	`, s.tablesPath),
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

		return task_errors.NewNonRetriableErrorf(
			"snapshot with id %v is not found",
			snapshotID,
		)
	}

	state := states[0]

	if state.status == snapshotStatusReady {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	if state.status != snapshotStatusCreating {
		return task_errors.NewSilentNonRetriableErrorf(
			"snapshot with id %v and status %v can't be created",
			snapshotID,
			snapshotStatusToString(state.status),
		)
	}

	state.status = snapshotStatusReady
	state.createdAt = time.Now()
	state.size = size
	state.storageSize = storageSize
	state.chunkCount = chunkCount

	if encryption != nil {
		state.encryptionMode = uint32(encryption.Mode)

		switch key := encryption.Key.(type) {
		case *types.EncryptionDesc_KeyHash:
			state.encryptionKeyHash = key.KeyHash
		case nil:
			state.encryptionKeyHash = nil
		default:
			return task_errors.NewNonRetriableErrorf("unknown key %s", key)
		}
	} else {
		state.encryptionMode = uint32(types.EncryptionMode_NO_ENCRYPTION)
		state.encryptionKeyHash = nil
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into snapshots
		select *
		from AS_TABLE($states)
	`, s.tablesPath, snapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return err
	}

	err = s.updateIncrementalTableAndSnapshotState(ctx, tx, state)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) deletingSnapshot(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	taskID string,
) (deleting *SnapshotMeta, err error) {

	defer s.metrics.StatOperation("deletingSnapshot")(&err)

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
		from snapshots
		where id = $id
	`, s.tablesPath),
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

	var state snapshotState

	if len(states) != 0 {
		state = states[0]
		logging.Info(ctx, "Deleting snapshot %+v", *state.toSnapshotMeta())

		if state.status >= snapshotStatusDeleting {
			// Snapshot already marked as deleting.

			err = tx.Commit(ctx)
			if err != nil {
				return nil, err
			}

			// Should be idempotent.
			return state.toSnapshotMeta(), err
		}

		if len(state.lockTaskID) != 0 && state.lockTaskID != taskID {
			err = tx.Commit(ctx)
			if err != nil {
				return nil, err
			}

			logging.Info(
				ctx,
				"Snapshot with id %v is locked and can't be deleted",
				snapshotID,
			)
			// Prevent deletion.
			return nil, task_errors.NewInterruptExecutionError()
		}
	}

	state.id = snapshotID
	state.status = snapshotStatusDeleting
	state.deletingAt = deletingAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into snapshots
		select *
		from AS_TABLE($states)
	`, s.tablesPath, snapshotStateStructTypeString()),
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

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $zone_id as Utf8;
		declare $disk_id as Utf8;
		declare $snapshot_id as Utf8;

		delete from incremental
		where zone_id = $zone_id and disk_id = $disk_id and snapshot_id = $snapshot_id
	`, s.tablesPath),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(state.zoneID)),
		persistence.ValueParam("$disk_id", persistence.UTF8Value(state.diskID)),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return nil, err
	}

	return state.toSnapshotMeta(), tx.Commit(ctx)
}

func (s *storageYDB) GetSnapshotsToDelete(
	ctx context.Context,
	deletingBefore time.Time,
	limit int,
) (keys []*protos.DeletingSnapshotKey, err error) {

	defer s.metrics.StatOperation("GetSnapshotsToDelete")(&err)

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
			key := &protos.DeletingSnapshotKey{}
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

func (s *storageYDB) deleteSnapshotData(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
) error {

	entries, errors := s.readChunkMap(ctx, session, snapshotID, 0, nil)

	err := s.processChunkMapEntries(
		ctx,
		entries,
		s.deleteWorkerCount,
		func(ctx context.Context, entry ChunkMapEntry) error {
			return s.deleteChunk(ctx, snapshotID, entry)
		},
	)
	if err != nil {
		return err
	}

	return <-errors
}

func (s *storageYDB) deleteChunk(
	ctx context.Context,
	snapshotID string,
	entry ChunkMapEntry,
) (err error) {

	defer s.metrics.StatOperation("deleteChunk")(&err)

	// First, update chunk blob's ref count. We do this before deleting chunk
	// map entry to avoid orphaning blobs.
	if len(entry.ChunkID) != 0 {
		chunkStorage := s.getChunkStorage(entry.StoredInS3)
		err := chunkStorage.UnrefChunk(ctx, snapshotID, entry.ChunkID)
		if err != nil {
			return err
		}
	}

	// Second, delete chunk map entry.
	// This operation is idempotent.
	_, err = s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $snapshot_id as Utf8;
		declare $chunk_index as Uint32;

		delete from chunk_map
		where shard_id = $shard_id and
			snapshot_id = $snapshot_id and
			chunk_index = $chunk_index;
	`, s.tablesPath),
		persistence.ValueParam("$shard_id", persistence.Uint64Value(makeShardID(snapshotID))),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$chunk_index", persistence.Uint32Value(entry.ChunkIndex)),
	)

	return err
}

func (s *storageYDB) ClearDeletingSnapshots(
	ctx context.Context,
	keys []*protos.DeletingSnapshotKey,
) (err error) {

	defer s.metrics.StatOperation("ClearDeletingSnapshots")(&err)

	for _, key := range keys {
		_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $deleting_at as Timestamp;
			declare $snapshot_id as Utf8;
			declare $status as Int64;

			delete from snapshots
			where id = $snapshot_id and status = $status;

			delete from deleting
			where deleting_at = $deleting_at and snapshot_id = $snapshot_id
		`, s.tablesPath),
			persistence.ValueParam("$deleting_at", persistence.TimestampValue(key.DeletingAt.AsTime())),
			persistence.ValueParam("$snapshot_id", persistence.UTF8Value(key.SnapshotId)),
			persistence.ValueParam("$status", persistence.Int64Value(int64(snapshotStatusDeleting))),
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *storageYDB) ShallowCopyChunk(
	ctx context.Context,
	srcEntry ChunkMapEntry,
	dstSnapshotID string,
) (err error) {

	defer s.metrics.StatOperation("ShallowCopyChunk")(&err)

	// First, create new chunk map entry. It is safe to create chunk map entry
	// before updating chunk blob's ref count because whole snapshot is not
	// ready yet. We do this to avoid orphaning blobs.
	// This operation is idempotent.
	_, err = s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $snapshot_id as Utf8;
		declare $chunk_index as Uint32;
		declare $chunk_id as Utf8;
		declare $stored_in_s3 as Bool;

		upsert into chunk_map (shard_id, snapshot_id, chunk_index, chunk_id, stored_in_s3)
		values ($shard_id, $snapshot_id, $chunk_index, $chunk_id, $stored_in_s3);
	`, s.tablesPath),
		persistence.ValueParam("$shard_id", persistence.Uint64Value(makeShardID(dstSnapshotID))),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(dstSnapshotID)),
		persistence.ValueParam("$chunk_index", persistence.Uint32Value(srcEntry.ChunkIndex)),
		persistence.ValueParam("$chunk_id", persistence.UTF8Value(srcEntry.ChunkID)),
		persistence.ValueParam("$stored_in_s3", persistence.BoolValue(srcEntry.StoredInS3)),
	)
	if err != nil {
		return err
	}

	logging.Debug(
		ctx,
		"copied chunk map entry %+v to snapshot %v",
		srcEntry,
		dstSnapshotID,
	)

	if len(srcEntry.ChunkID) == 0 {
		return nil
	}

	chunkStorage := s.getChunkStorage(srcEntry.StoredInS3)
	return chunkStorage.RefChunk(ctx, dstSnapshotID, srcEntry.ChunkID)
}

func (s *storageYDB) shallowCopySnapshot(
	ctx context.Context,
	session *persistence.Session,
	srcSnapshotID string,
	dstSnapshotID string,
	milestoneChunkIndex uint32,
	saveProgress func(context.Context, uint32) error,
) error {

	processedIndices := make(chan uint32, s.shallowCopyInflightLimit)

	inflightQueue := common.NewInflightQueue(
		common.Milestone{Value: milestoneChunkIndex},
		processedIndices,
		common.ChannelWithCancellation[uint32]{}, // holeValues
		s.shallowCopyInflightLimit,
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

	entries, errors := s.readChunkMap(
		ctx,
		session,
		srcSnapshotID,
		milestoneChunkIndex,
		inflightQueue,
	)

	err := s.processChunkMapEntries(
		ctx,
		entries,
		s.shallowCopyWorkerCount,
		func(ctx context.Context, entry ChunkMapEntry) error {
			err := s.ShallowCopyChunk(ctx, entry, dstSnapshotID)
			if err != nil {
				return err
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

	return waitSaver()
}

func (s *storageYDB) WriteChunk(
	ctx context.Context,
	uniqueID string,
	snapshotID string,
	chunk dataplane_common.Chunk,
	useS3 bool,
) (chunkID string, err error) {

	if chunk.Zero {
		return s.writeZeroChunk(ctx, snapshotID, chunk)
	} else {
		return s.writeDataChunk(ctx, uniqueID, snapshotID, chunk, useS3)
	}
}

func (s *storageYDB) writeDataChunk(
	ctx context.Context,
	uniqueID string,
	snapshotID string,
	chunk dataplane_common.Chunk,
	useS3 bool,
) (string, error) {
	var err error

	defer s.metrics.StatOperation("writeDataChunk")(&err)

	chunk.ID = makeChunkID(uniqueID, snapshotID, chunk)

	chunk.Compression = s.chunkCompression
	// First, create chunk map entry. It is safe to create chunk map entry before
	// writing chunk blob because whole snapshot is not ready yet. We do this to
	// avoid orphaning blobs.
	// This operation is idempotent.
	_, err = s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $snapshot_id as Utf8;
		declare $chunk_index as Uint32;
		declare $chunk_id as Utf8;
		declare $stored_in_s3 as Bool;

		upsert into chunk_map (shard_id, snapshot_id, chunk_index, chunk_id, stored_in_s3)
		values ($shard_id, $snapshot_id, $chunk_index, $chunk_id, $stored_in_s3)
	`, s.tablesPath),
		persistence.ValueParam("$shard_id", persistence.Uint64Value(makeShardID(snapshotID))),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$chunk_index", persistence.Uint32Value(chunk.Index)),
		persistence.ValueParam("$chunk_id", persistence.UTF8Value(chunk.ID)),
		persistence.ValueParam("$stored_in_s3", persistence.BoolValue(useS3)),
	)
	if err != nil {
		return "", err
	}

	logging.Debug(
		ctx,
		"created chunk map entry %v for snapshot %v",
		chunk.ID,
		snapshotID,
	)

	chunkStorage := s.getChunkStorage(useS3)
	err = chunkStorage.WriteChunk(ctx, snapshotID, chunk)
	if err != nil {
		return "", err
	}

	logging.Debug(
		ctx,
		"written chunk %v for snapshot %v",
		chunk.ID,
		snapshotID,
	)
	return chunk.ID, nil
}

func (s *storageYDB) writeZeroChunk(
	ctx context.Context,
	snapshotID string,
	chunk dataplane_common.Chunk,
) (string, error) {

	var err error

	defer s.metrics.StatOperation("writeZeroChunk")(&err)

	_, err = s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $snapshot_id as Utf8;
		declare $chunk_index as Uint32;

		upsert into chunk_map (shard_id, snapshot_id, chunk_index, chunk_id)
		values ($shard_id, $snapshot_id, $chunk_index, "");
	`, s.tablesPath),
		persistence.ValueParam("$shard_id", persistence.Uint64Value(makeShardID(snapshotID))),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$chunk_index", persistence.Uint32Value(chunk.Index)),
	)
	if err == nil {
		logging.Debug(
			ctx,
			"created chunk map entry with index %v for snapshot %v",
			chunk.Index,
			snapshotID,
		)
	}

	return "", err
}

func (s *storageYDB) readChunkMap(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	milestoneChunkIndex uint32,
	inflightQueue *common.InflightQueue,
) (<-chan ChunkMapEntry, <-chan error) {

	entries := make(chan ChunkMapEntry)
	errors := make(chan error, 1)

	shardID := makeShardID(snapshotID)

	res, err := session.StreamReadTable(
		ctx,
		path.Join(s.tablesPath, "chunk_map"),
		persistence.ReadOrdered(),
		persistence.ReadColumn("chunk_index"),
		persistence.ReadColumn("chunk_id"),
		persistence.ReadColumn("stored_in_s3"),
		persistence.ReadGreaterOrEqual(persistence.TupleValue(
			persistence.OptionalValue(persistence.Uint64Value(shardID)),
			persistence.OptionalValue(persistence.UTF8Value(snapshotID)),
			persistence.OptionalValue(persistence.Uint32Value(milestoneChunkIndex)),
		)),
		persistence.ReadLessOrEqual(persistence.TupleValue(
			persistence.OptionalValue(persistence.Uint64Value(shardID)),
			persistence.OptionalValue(persistence.UTF8Value(snapshotID)),
			persistence.OptionalValue(persistence.Uint32Value(^uint32(0))),
		)),
	)
	if err != nil {
		errors <- err
		close(entries)
		close(errors)
		return entries, errors
	}

	go func() {
		defer res.Close()
		defer close(entries)
		defer close(errors)

		defer func() {
			if r := recover(); r != nil {
				errors <- task_errors.NewPanicError(r)
			}
		}()

		for res.NextResultSet(ctx) {
			for res.NextRow() {
				var entry ChunkMapEntry
				err = res.ScanNamed(
					persistence.OptionalWithDefault("chunk_index", &entry.ChunkIndex),
					persistence.OptionalWithDefault("chunk_id", &entry.ChunkID),
					persistence.OptionalWithDefault("stored_in_s3", &entry.StoredInS3),
				)
				if err != nil {
					errors <- err
					return
				}

				if inflightQueue != nil {
					_, err := inflightQueue.Add(ctx, entry.ChunkIndex)
					if err != nil {
						errors <- err
						return
					}
				}

				select {
				case entries <- entry:
				case <-ctx.Done():
					errors <- ctx.Err()
					return
				}
			}
		}

		err = res.Err()
		if err != nil {
			errors <- task_errors.NewRetriableError(err)
		}
	}()

	return entries, errors
}

func (s *storageYDB) ReadChunk(
	ctx context.Context,
	chunk *dataplane_common.Chunk,
) (err error) {

	defer s.metrics.StatOperation("ReadChunk")(&err)

	if len(chunk.ID) == 0 {
		return task_errors.NewNonRetriableErrorf("chunkID should not be empty")
	}

	chunkStorage := s.getChunkStorage(chunk.StoredInS3)
	return chunkStorage.ReadChunk(ctx, chunk)
}

func (s *storageYDB) CheckSnapshotReady(
	ctx context.Context,
	snapshotID string,
) (meta SnapshotMeta, err error) {

	state, err := s.getSnapshot(ctx, snapshotID)
	if err != nil {
		return SnapshotMeta{}, err
	}

	if state == nil {
		return SnapshotMeta{}, task_errors.NewSilentNonRetriableErrorf(
			"snapshot with id %v is not found",
			snapshotID,
		)
	}

	if state.status != snapshotStatusReady {
		return SnapshotMeta{}, task_errors.NewSilentNonRetriableErrorf(
			"snapshot with id %v is not ready",
			snapshotID,
		)
	}

	return *state.toSnapshotMeta(), nil
}

func (s *storageYDB) CheckSnapshotAlive(
	ctx context.Context,
	snapshotID string,
) (err error) {

	state, err := s.getSnapshot(ctx, snapshotID)
	if err != nil {
		return err
	}

	if state == nil {
		return task_errors.NewSilentNonRetriableErrorf(
			"snapshot with id %v is not found",
			snapshotID,
		)
	}

	if state.status >= snapshotStatusDeleting {
		return task_errors.NewSilentNonRetriableErrorf(
			"snapshot with id %v status %v is not alive",
			snapshotID,
			snapshotStatusToString(state.status),
		)
	}

	return nil
}

func (s *storageYDB) GetDataChunkCount(
	ctx context.Context,
	snapshotID string,
) (dataChunkCount uint64, err error) {

	defer s.metrics.StatOperation("GetDataChunkCount")(&err)

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $snapshot_id as Utf8;

		select count_if(chunk_id != '') as data_chunk_count
		from chunk_map
		where shard_id = $shard_id and snapshot_id = $snapshot_id
	`, s.tablesPath),
		persistence.ValueParam("$shard_id", persistence.Uint64Value(makeShardID(snapshotID))),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	dataChunkCount = uint64(0)

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return 0, nil
	}

	err = res.ScanNamed(
		persistence.OptionalWithDefault("data_chunk_count", &dataChunkCount),
	)
	if err != nil {
		return 0, err
	}

	return dataChunkCount, nil
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) getSnapshot(
	ctx context.Context,
	snapshotID string,
) (state *snapshotState, err error) {

	defer s.metrics.StatOperation("GetSnapshot")(&err)

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from snapshots
		where id = $id
	`, s.tablesPath),
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
		return &states[0], nil
	} else {
		return nil, nil
	}
}

func (s *storageYDB) processChunkMapEntries(
	ctx context.Context,
	entries <-chan ChunkMapEntry,
	workerCount int,
	process func(ctx context.Context, entry ChunkMapEntry) error,
) error {

	var wg sync.WaitGroup
	wg.Add(workerCount)
	// Should wait right after context cancelling.
	defer wg.Wait()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerErrors := make(chan error, workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()

			defer func() {
				if r := recover(); r != nil {
					workerErrors <- task_errors.NewPanicError(r)
				}
			}()

			var entry ChunkMapEntry
			more := true

			for more {
				select {
				case entry, more = <-entries:
					if !more {
						break
					}

					err := process(ctx, entry)
					if err != nil {
						workerErrors <- err
						return
					}
				case <-ctx.Done():
					workerErrors <- ctx.Err()
					return
				}
			}

			workerErrors <- nil
		}()
	}

	// Wait for all workers to complete.
	for i := 0; i < workerCount; i++ {
		err := <-workerErrors
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *storageYDB) getChunkStorage(useS3 bool) chunks.Storage {
	if useS3 {
		return s.chunkStorageS3
	} else {
		return s.chunkStorageYDB
	}
}

func (s *storageYDB) lockSnapshot(
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
		from snapshots
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)),
	)
	if err != nil {
		return false, err
	}
	defer res.Close()

	states, err := scanSnapshotStates(ctx, res)
	if err != nil {
		return false, err
	}

	if len(states) == 0 {
		return false, tx.Commit(ctx)
	}

	state := states[0]
	if state.status >= snapshotStatusDeleting {
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

		logging.Info(ctx, "Another lock %v was found for snapshot %v", lockTaskID, snapshotID)
		return false, task_errors.NewInterruptExecutionError()
	}

	state.lockTaskID = lockTaskID

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into snapshots
		select *
		from AS_TABLE($states)
	`, s.tablesPath, snapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return false, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return false, err
	}

	logging.Info(ctx, "Locked snapshot with id %v", snapshotID)
	return true, nil
}

func (s *storageYDB) unlockSnapshot(
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
		from snapshots
		where id = $id
	`, s.tablesPath),
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
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	state := states[0]
	if state.status >= snapshotStatusDeleting {
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

		upsert into snapshots
		select *
		from AS_TABLE($states)
	`, s.tablesPath, snapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	logging.Info(ctx, "Unlocked snapshot with id %v", snapshotID)
	return nil
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
	`, s.tablesPath),
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
		return nil, task_errors.NewNonRetriableErrorf(
			"snapshot with id %v does not exist",
			snapshotID,
		)
	}

	return states[0].toSnapshotMeta(), nil
}

func (s *storageYDB) listAllSnapshots(
	ctx context.Context,
	session *persistence.Session,
) (task_storage.StringSet, error) {

	result := task_storage.NewStringSet()
	res, err := session.StreamExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshotStatusReady as Int64;
		select id
		from snapshots where status = $snapshotStatusReady
	`, s.tablesPath),
		persistence.ValueParam(
			"$snapshotStatusReady",
			persistence.Int64Value(int64(snapshotStatusReady)),
		),
	)
	if err != nil {
		return result, err
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var id *string
			err := res.Scan(&id)
			if err != nil {
				return result, err
			}

			result.Add(*id)
		}
	}

	err = res.Err()
	if err != nil {
		return result, task_errors.NewRetriableError(err)
	}

	return result, nil
}
