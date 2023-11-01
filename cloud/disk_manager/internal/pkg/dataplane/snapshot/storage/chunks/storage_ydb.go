package chunks

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/compressor"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/persistence"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/rtc/mediator/cityhash"
	ydb_named "github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydb_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
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
	metrics metrics.Metrics,
	probeCompressionPercentage map[string]uint32,
) *StorageYDB {

	return &StorageYDB{
		storageCommon:              newStorageCommon(db, tablesPath),
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
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $chunk_id as Utf8;

		select * from chunk_blobs
		where shard_id = $shard_id and
			chunk_id = $chunk_id and
			referer = "";
	`, s.tablesPath),
		persistence.ValueParam("$shard_id", ydb_types.Uint64Value(makeShardID(chunk.ID))),
		persistence.ValueParam("$chunk_id", ydb_types.UTF8Value(chunk.ID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return errors.NewNonRetriableErrorf("chunk not found")
	}

	var (
		data        []byte
		checksum    uint32
		compression string
	)
	err = res.ScanNamed(
		ydb_named.OptionalWithDefault("data", &data),
		ydb_named.OptionalWithDefault("checksum", &checksum),
		ydb_named.OptionalWithDefault("compression", &compression),
	)
	if err != nil {
		return errors.NewNonRetriableErrorf(
			"ReadChunk: failed to parse row: %w",
			err,
		)
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

////////////////////////////////////////////////////////////////////////////////

func makeShardID(chunkID string) uint64 {
	return cityhash.Hash64([]byte(chunkID))
}
