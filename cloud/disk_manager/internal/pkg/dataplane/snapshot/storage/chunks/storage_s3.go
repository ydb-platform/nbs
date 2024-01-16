package chunks

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/compressor"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

// Stores chunks metadata in YDB and data in S3.
type StorageS3 struct {
	storageCommon
	s3                         *persistence.S3Client
	metrics                    metrics.Metrics
	bucket                     string
	keyPrefix                  string
	probeCompressionPercentage map[string]uint32
}

func NewStorageS3(
	db *persistence.YDBClient,
	s3 *persistence.S3Client,
	bucket string,
	keyPrefix string,
	tablesPath string,
	metrics metrics.Metrics,
	probeCompressionPercentage map[string]uint32,
) *StorageS3 {

	return &StorageS3{
		storageCommon:              newStorageCommon(db, tablesPath),
		s3:                         s3,
		bucket:                     bucket,
		keyPrefix:                  keyPrefix,
		metrics:                    metrics,
		probeCompressionPercentage: probeCompressionPercentage,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (s *StorageS3) ReadChunk(
	ctx context.Context,
	chunk *common.Chunk,
) (err error) {

	defer s.metrics.StatOperation(metrics.OperationReadChunkBlob)(&err)

	object, err := s.s3.GetObject(ctx, s.bucket, s.newS3Key(chunk.ID))
	if err != nil {
		return err
	}

	metadata, err := newS3Metadata(object.Metadata)
	if err != nil {
		return err
	}

	logging.Debug(
		ctx,
		"read chunk from s3 {id: %q, checksum: %v, compression: %q}",
		chunk.ID,
		metadata.checksum,
		metadata.compression,
	)

	err = compressor.Decompress(
		metadata.compression,
		object.Data,
		chunk.Data,
		s.metrics,
	)
	if err != nil {
		return err
	}

	actualChecksum := chunk.Checksum()
	if metadata.checksum != actualChecksum {
		return task_errors.NewNonRetriableErrorf(
			"ReadChunk: s3 chunk checksum mismatch: expected %v, actual %v",
			metadata.checksum,
			actualChecksum,
		)
	}

	return nil
}

func (s *StorageS3) WriteChunk(
	ctx context.Context,
	referer string,
	chunk common.Chunk,
) (err error) {

	defer s.metrics.StatOperation(metrics.OperationWriteChunkBlob)(&err)

	errGroup, ctx := errgroup.WithContext(ctx)

	errGroup.Go(func() error {
		return s.writeChunkMetadata(ctx, referer, chunk.ID)
	})

	errGroup.Go(func() error {
		return s.writeChunkData(ctx, chunk)
	})

	return errGroup.Wait()
}

func (s *StorageS3) RefChunk(
	ctx context.Context,
	referer string,
	chunkID string,
) (err error) {

	defer s.metrics.StatOperation(metrics.OperationRefChunkBlob)(&err)
	return s.refChunk(ctx, referer, chunkID)
}

func (s *StorageS3) UnrefChunk(
	ctx context.Context,
	referer string,
	chunkID string,
) (err error) {

	defer s.metrics.StatOperation(metrics.OperationUnrefChunkBlob)(&err)

	var refCount uint32
	refCount, err = s.unrefChunkMetadata(ctx, referer, chunkID)
	if err != nil {
		return err
	}

	if refCount == 0 {
		return s.deleteChunkData(ctx, chunkID)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func (s *StorageS3) writeChunkMetadata(
	ctx context.Context,
	referer string,
	chunkID string,
) error {

	return s.writeToChunkBlobs(
		ctx,
		referer,
		common.Chunk{ID: chunkID},
		[]byte{}, // data
		0,        // checksum
	)
}

func (s *StorageS3) writeChunkData(
	ctx context.Context,
	chunk common.Chunk,
) error {

	metadata := s3Metadata{
		compression: chunk.Compression,
		checksum:    chunk.Checksum(),
	}

	logging.Debug(
		ctx,
		"write chunk to s3 {id: %q, checksum: %v, compression: %q}",
		chunk.ID,
		metadata.checksum,
		metadata.compression,
	)

	compressedData, err := compressor.Compress(
		metadata.compression,
		chunk.Data,
		s.metrics,
		s.probeCompressionPercentage,
	)
	if err != nil {
		return err
	}
	object := persistence.S3Object{
		Data:     compressedData,
		Metadata: metadata.toMap(),
	}

	return s.s3.PutObject(ctx, s.bucket, s.newS3Key(chunk.ID), object)
}

func (s *StorageS3) unrefChunkMetadata(
	ctx context.Context,
	referer string,
	chunkID string,
) (uint32, error) {

	return s.unrefChunk(ctx, referer, chunkID)
}

func (s *StorageS3) deleteChunkData(
	ctx context.Context,
	chunkID string,
) error {

	return s.s3.DeleteObject(ctx, s.bucket, s.newS3Key(chunkID))
}

////////////////////////////////////////////////////////////////////////////////

type s3Metadata struct {
	compression string
	checksum    uint32
}

func newS3Metadata(metadataMap map[string]*string) (s3Metadata, error) {
	checksumStr := metadataMap[s3MetadataKeyChecksum]
	if checksumStr == nil {
		return s3Metadata{}, task_errors.NewNonRetriableErrorf(
			"s3 object metadata is missing",
		)
	}

	checksumUint64, err := strconv.ParseUint(
		*checksumStr,
		10,
		32,
	)
	if err != nil {
		return s3Metadata{}, task_errors.NewNonRetriableErrorf(
			"failed to parse checksum from s3 object metadata",
		)
	}

	metadata := s3Metadata{
		checksum: uint32(checksumUint64),
	}
	if v, ok := metadataMap[s3MetadataKeyCompression]; ok {
		metadata.compression = *v
	}

	return metadata, nil
}

func (m s3Metadata) toMap() map[string]*string {
	checksumStr := strconv.FormatUint(uint64(m.checksum), 10)

	metadataMap := map[string]*string{
		s3MetadataKeyChecksum: &checksumStr,
	}
	if len(m.compression) != 0 {
		metadataMap[s3MetadataKeyCompression] = &m.compression
	}

	return metadataMap
}

const (
	s3MetadataKeyCompression = "Compression"
	s3MetadataKeyChecksum    = "Checksum"
)

////////////////////////////////////////////////////////////////////////////////

func (s *StorageS3) newS3Key(chunkID string) string {
	return fmt.Sprintf("%v/%v", s.keyPrefix, chunkID)
}
