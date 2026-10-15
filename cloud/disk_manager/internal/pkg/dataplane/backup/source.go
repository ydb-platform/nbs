package backup

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

// Read-only access to the objects of a backup. Returned objects have their
// backup encryption envelope removed. Implemented by S3.
type ObjectReader interface {
	GetObject(ctx context.Context, key string) (persistence.S3Object, error)
}

////////////////////////////////////////////////////////////////////////////////

type backupSource struct {
	reader ObjectReader
	// Position is the chunk index, empty id stands for zero chunk.
	chunkIDs    []string
	storageSize uint64
	metrics     metrics.Metrics

	chunkIndices common.ChannelWithInflightQueue
}

func (s *backupSource) ChunkIndices(
	ctx context.Context,
	milestone dataplane_common.Milestone,
	processedChunkIndices <-chan uint32,
	holeChunkIndices common.ChannelWithCancellation,
) (<-chan uint32, common.ChannelWithCancellation, <-chan error) {

	common.Assert(s.chunkIndices.Empty(), "should be called once")

	inflightLimit := cap(processedChunkIndices)

	s.chunkIndices = common.NewChannelWithInflightQueue(
		common.Milestone{
			Value:               milestone.ChunkIndex,
			ProcessedValueCount: milestone.TransferredChunkCount,
		},
		processedChunkIndices,
		holeChunkIndices,
		inflightLimit,
	)

	errors := make(chan error, 1)

	go func() {
		defer close(errors)

		defer func() {
			if r := recover(); r != nil {
				errors <- task_errors.NewPanicError(r)
			}
		}()

		defer s.chunkIndices.Close()

		chunkCount := uint32(len(s.chunkIDs))

		for i := milestone.ChunkIndex; i < chunkCount; i++ {
			_, err := s.chunkIndices.Send(ctx, i)
			if err != nil {
				errors <- err
				return
			}
		}
	}()

	return s.chunkIndices.Channel(), common.ChannelWithCancellation{}, errors
}

func (s *backupSource) Read(
	ctx context.Context,
	chunk *dataplane_common.Chunk,
) error {

	chunkID := s.chunkIDs[chunk.Index]

	// The chunk is taken from a pool, drop what is left from its previous use.
	*chunk = dataplane_common.Chunk{
		ID:    chunkID,
		Index: chunk.Index,
		Data:  chunk.Data,
	}

	if len(chunkID) == 0 {
		chunk.Zero = true
		return nil
	}

	err := s.readChunk(ctx, chunk)
	if err == nil {
		return nil
	}

	if task_errors.Is(err, task_errors.NewEmptyNonRetriableError()) {
		// S3 client reports a missing object as a silent error, but a chunk
		// that is referenced by the chunk map and cannot be read is lost data.
		return task_errors.NewNonRetriableErrorf(
			"failed to read chunk with index %v and id %q from backup: %w",
			chunk.Index,
			chunkID,
			err,
		)
	}

	return err
}

func (s *backupSource) Milestone() dataplane_common.Milestone {
	common.Assert(!s.chunkIndices.Empty(), "should not be empty")

	milestone := s.chunkIndices.Milestone()
	return dataplane_common.Milestone{
		ChunkIndex:            milestone.Value,
		TransferredChunkCount: milestone.ProcessedValueCount,
	}
}

func (s *backupSource) ChunkCount(ctx context.Context) (uint32, error) {
	return uint32(len(s.chunkIDs)), nil
}

func (s *backupSource) EstimatedBytesToRead(
	ctx context.Context,
) (uint64, error) {

	return s.storageSize, nil
}

func (s *backupSource) Close(ctx context.Context) {
}

////////////////////////////////////////////////////////////////////////////////

func (s *backupSource) readChunk(
	ctx context.Context,
	chunk *dataplane_common.Chunk,
) (err error) {

	defer s.metrics.StatOperation(metrics.OperationReadChunkBlob)(&err)

	object, err := s.reader.GetObject(ctx, ChunkKey(chunk.ID))
	if err != nil {
		return err
	}

	return chunks.DecodeS3Chunk(ctx, object, chunk, s.metrics)
}

////////////////////////////////////////////////////////////////////////////////

// Reads chunks of the backup with given chunk map: chunkIDs[i] is the id of
// the chunk with index i, empty for zero chunk. The number of chunks should
// fit into uint32. The chunk map should not be modified during the transfer.
func NewBackupSource(
	reader ObjectReader,
	chunkIDs []string,
	storageSize uint64,
	metrics metrics.Metrics,
) dataplane_common.Source {

	return &backupSource{
		reader:      reader,
		chunkIDs:    chunkIDs,
		storageSize: storageSize,
		metrics:     metrics,
	}
}
