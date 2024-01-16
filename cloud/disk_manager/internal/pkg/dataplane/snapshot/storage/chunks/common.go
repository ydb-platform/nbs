package chunks

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type storageCommon struct {
	db         *persistence.YDBClient
	tablesPath string
}

func newStorageCommon(
	db *persistence.YDBClient,
	tablesPath string,
) storageCommon {

	return storageCommon{
		db:         db,
		tablesPath: tablesPath,
	}
}

func (s *storageCommon) writeToChunkBlobs(
	ctx context.Context,
	referer string,
	chunk common.Chunk,
	compressedData []byte,
	checksum uint32,
) error {

	_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $referer as Utf8;
		declare $chunk_id as Utf8;
		declare $data as String;
		declare $checksum as Uint32;
		declare $compression as Utf8;

		upsert into chunk_blobs (shard_id, chunk_id, referer, data, refcnt, checksum, compression)
		values
			($shard_id, $chunk_id, "", $data, cast(1 as Uint32), $checksum, $compression),
			($shard_id, $chunk_id, $referer, null, null, null, null)
	`, s.tablesPath),
		persistence.ValueParam("$shard_id", persistence.Uint64Value(makeShardID(chunk.ID))),
		persistence.ValueParam("$chunk_id", persistence.UTF8Value(chunk.ID)),
		persistence.ValueParam("$referer", persistence.UTF8Value(referer)),
		persistence.ValueParam("$data", persistence.StringValue(compressedData)),
		persistence.ValueParam("$checksum", persistence.Uint32Value(checksum)),
		persistence.ValueParam("$compression", persistence.UTF8Value(chunk.Compression)),
	)
	return err
}

func (s *storageCommon) refChunk(
	ctx context.Context,
	referer string,
	chunkID string,
) error {

	// We use |referer| column to implement `exactly once` update semantics.
	_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $chunk_id as Utf8;
		declare $referer as Utf8;

		$existing = (
			select chunk_id
			from chunk_blobs
			where shard_id = $shard_id and
				chunk_id = $chunk_id and
				referer = $referer
		);

		$to_update = (
			select
				shard_id,
				chunk_id,
				referer,
				refcnt + cast(1 as Uint32) as refcnt,
			from chunk_blobs
			where shard_id = $shard_id and
				chunk_id = $chunk_id and
				referer = "" and
				$chunk_id not in $existing
		);

		update chunk_blobs
		on select * from $to_update;

		upsert into chunk_blobs (shard_id, chunk_id, referer)
		values ($shard_id, $chunk_id, $referer);
	`, s.tablesPath),
		persistence.ValueParam("$shard_id", persistence.Uint64Value(makeShardID(chunkID))),
		persistence.ValueParam("$chunk_id", persistence.UTF8Value(chunkID)),
		persistence.ValueParam("$referer", persistence.UTF8Value(referer)),
	)
	if err == nil {
		logging.Debug(
			ctx,
			"referred chunk %v for referer %v",
			chunkID,
			referer,
		)
	}

	return err
}

func (s *storageCommon) unrefChunk(
	ctx context.Context,
	referer string,
	chunkID string,
) (uint32, error) {

	// We use |referer| column to implement `exactly once` update semantics.
	// Chunk with zero ref count is deleted.
	res, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		pragma AnsiInForEmptyOrNullableItemsCollections;
		declare $shard_id as Uint64;
		declare $chunk_id as Utf8;
		declare $referer as Utf8;

		$existing = (
			select chunk_id
			from chunk_blobs
			where shard_id = $shard_id and
				chunk_id = $chunk_id and
				referer = $referer
		);

		$to_update = (
			select
				shard_id,
				chunk_id,
				referer,
				refcnt - cast(1 as Uint32) as refcnt,
			from chunk_blobs
			where shard_id = $shard_id and
				chunk_id = $chunk_id and
				referer = "" and
				refcnt > 1 and
				$chunk_id in $existing
		);

		$to_delete = (
			select
				shard_id,
				chunk_id,
				referer,
			from chunk_blobs
			where shard_id = $shard_id and
				chunk_id = $chunk_id and
				referer = "" and
				refcnt <= 1 and
				$chunk_id in $existing
			union all
			select
				shard_id,
				chunk_id,
				referer,
			from chunk_blobs
			where shard_id = $shard_id and
				chunk_id = $chunk_id and
				referer = $referer
		);

		select refcnt
		from chunk_blobs
		where shard_id = $shard_id and
			chunk_id = $chunk_id and
			referer = "";

		select count(*) as refs_to_delete
		from chunk_blobs
		where shard_id = $shard_id and
			chunk_id = $chunk_id and
			referer = $referer;

		update chunk_blobs
		on select * from $to_update;

		delete from chunk_blobs
		on select * from $to_delete;
	`, s.tablesPath),
		persistence.ValueParam("$shard_id", persistence.Uint64Value(makeShardID(chunkID))),
		persistence.ValueParam("$chunk_id", persistence.UTF8Value(chunkID)),
		persistence.ValueParam("$referer", persistence.UTF8Value(referer)),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	logging.Debug(
		ctx,
		"unreferred chunk %v for referer %v",
		chunkID,
		referer,
	)

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return 0, nil
	}

	var refCount uint32
	err = res.ScanNamed(
		persistence.OptionalWithDefault("refcnt", &refCount),
	)
	if err != nil {
		return 0, err
	}

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return refCount, nil
	}

	var refsToDelete uint64
	err = res.ScanNamed(
		persistence.OptionalWithDefault("refs_to_delete", &refsToDelete),
	)
	if err != nil {
		return 0, err
	}

	return refCount - uint32(refsToDelete), nil
}
