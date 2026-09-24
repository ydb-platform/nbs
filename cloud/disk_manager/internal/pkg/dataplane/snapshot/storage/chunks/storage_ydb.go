package chunks

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/compressor"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	"github.com/ydb-platform/nbs/contrib/go/cityhash"
)

////////////////////////////////////////////////////////////////////////////////

// Stores chunks data and metadata in YDB.
type StorageYDB struct {
	storageCommon
	metrics                    metrics.Metrics
	probeCompressionPercentage map[string]uint32
}

func NewStorageYDB(
	db *persistence.YDBClient,
	tablesPath string,
	tableName string,
	shadowTableName string,
	metrics metrics.Metrics,
	probeCompressionPercentage map[string]uint32,
) *StorageYDB {

	return &StorageYDB{
		storageCommon:              newStorageCommon(db, tablesPath, tableName, shadowTableName),
		metrics:                    metrics,
		probeCompressionPercentage: probeCompressionPercentage,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (s *StorageYDB) ReadChunk(
	ctx context.Context,
	chunk *common.Chunk,
) (err error) {

	defer s.metrics.StatOperation(metrics.OperationReadChunkBlob)(&err)

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%[1]v";
		declare $shard_id as Uint64;
		declare $chunk_id as Utf8;

		select * from %[2]v
		where shard_id = $shard_id and
			chunk_id = $chunk_id and
			referer = "";
	`, s.tablesPath, s.tableName),
		persistence.ValueParam("$shard_id", persistence.Uint64Value(makeShardID(chunk.ID))),
		persistence.ValueParam("$chunk_id", persistence.UTF8Value(chunk.ID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return errors.NewNonRetriableErrorf("chunk not found: %v", chunk.ID)
	}

	var (
		data        []byte
		checksum    uint32
		compression string
	)
	err = res.ScanNamed(
		persistence.OptionalWithDefault("data", &data),
		persistence.OptionalWithDefault("checksum", &checksum),
		persistence.OptionalWithDefault("compression", &compression),
	)
	if err != nil {
		return err
	}

	logging.Debug(
		ctx,
		"read chunk from ydb {id: %q, checksum: %v, compression: %q}",
		chunk.ID,
		checksum,
		compression,
	)

	err = compressor.Decompress(compression, data, chunk.Data, s.metrics)
	if err != nil {
		return err
	}

	actualChecksum := chunk.Checksum()
	if checksum != actualChecksum {
		return errors.NewNonRetriableErrorf(
			"ReadChunk: ydb chunk checksum mismatch: expected %v, actual %v",
			checksum,
			actualChecksum,
		)
	}

	return nil
}

func (s *StorageYDB) ReadChunkBlob(
	ctx context.Context,
	chunkID string,
) (chunkBlob ChunkBlob, err error) {

	defer s.metrics.StatOperation(metrics.OperationReadChunkBlob)(&err)

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%[1]v";
		declare $shard_id as Uint64;
		declare $chunk_id as Utf8;

		select data, checksum, compression from %[2]v
		where shard_id = $shard_id and
			chunk_id = $chunk_id and
			referer = "";
	`, s.tablesPath, s.tableName),
		persistence.ValueParam(
			"$shard_id",
			persistence.Uint64Value(makeShardID(chunkID)),
		),
		persistence.ValueParam("$chunk_id", persistence.UTF8Value(chunkID)),
	)
	if err != nil {
		return ChunkBlob{}, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return ChunkBlob{}, errors.NewNonRetriableErrorf(
			"chunk not found: %v",
			chunkID,
		)
	}

	err = res.ScanNamed(
		persistence.OptionalWithDefault("data", &chunkBlob.Data),
		persistence.OptionalWithDefault("checksum", &chunkBlob.Checksum),
		persistence.OptionalWithDefault("compression", &chunkBlob.Compression),
	)
	if err != nil {
		return ChunkBlob{}, err
	}

	return chunkBlob, nil
}

func (s *StorageYDB) WriteChunk(
	ctx context.Context,
	referer string,
	chunk common.Chunk,
) (err error) {

	defer s.metrics.StatOperation(metrics.OperationWriteChunkBlob)(&err)

	checksum := chunk.Checksum()

	logging.Debug(
		ctx,
		"write chunk to ydb {id: %q, checksum: %v, compression: %q}",
		chunk.ID,
		checksum,
		chunk.Compression,
	)

	compressedData, err := compressor.Compress(
		chunk.Compression,
		chunk.Data,
		s.metrics,
		s.probeCompressionPercentage,
	)
	if err != nil {
		return err
	}

	return s.writeToChunkBlobs(
		ctx,
		referer,
		chunk,
		compressedData,
		checksum,
	)
}

func (s *StorageYDB) RefChunk(
	ctx context.Context,
	referer string,
	chunkID string,
) (err error) {

	defer s.metrics.StatOperation(metrics.OperationRefChunkBlob)(&err)
	return s.refChunk(ctx, referer, chunkID)
}

func (s *StorageYDB) UnrefChunk(
	ctx context.Context,
	referer string,
	chunkID string,
) (err error) {

	defer s.metrics.StatOperation(metrics.OperationUnrefChunkBlob)(&err)
	_, err = s.unrefChunk(ctx, referer, chunkID)
	return err
}

func (s *StorageYDB) FilterExistingChunkIDs(
	ctx context.Context,
	chunkIDs []string,
) (existing []string, err error) {

	if len(chunkIDs) == 0 {
		return nil, nil
	}

	var shardIDs []persistence.Value
	var ids []persistence.Value
	for _, chunkID := range chunkIDs {
		shardIDs = append(
			shardIDs,
			persistence.Uint64Value(makeShardID(chunkID)),
		)
		ids = append(ids, persistence.UTF8Value(chunkID))
	}

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_ids as List<Uint64>;
		declare $chunk_ids as List<Utf8>;

		select chunk_id
		from %v
		where shard_id in $shard_ids and
			chunk_id in $chunk_ids and
			referer = ""
	`, s.tablesPath, s.tableName),
		persistence.ValueParam("$shard_ids", persistence.ListValue(shardIDs...)),
		persistence.ValueParam("$chunk_ids", persistence.ListValue(ids...)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var chunkID string
			err = res.ScanNamed(
				persistence.OptionalWithDefault("chunk_id", &chunkID),
			)
			if err != nil {
				return nil, err
			}

			existing = append(existing, chunkID)
		}
	}

	return existing, res.Err()
}

////////////////////////////////////////////////////////////////////////////////

func makeShardID(chunkID string) uint64 {
	return cityhash.Hash64([]byte(chunkID))
}
