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

type ObjectReader interface {
	GetObject(ctx context.Context, key string) (persistence.S3Object, error)
}

type backupSource struct {
	reader      ObjectReader
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
	s.chunkIndices = common.NewChannelWithInflightQueue(
		common.Milestone{
			Value:               milestone.ChunkIndex,
			ProcessedValueCount: milestone.TransferredChunkCount,
		},
		processedChunkIndices,
		holeChunkIndices,
		cap(processedChunkIndices),
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

		for i := milestone.ChunkIndex; i < uint32(len(s.chunkIDs)); i++ {
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

	// Reset fields left by the previous user of the pooled buffer.
	*chunk = dataplane_common.Chunk{Index: chunk.Index, Data: chunk.Data}
	chunk.ID = s.chunkIDs[chunk.Index]
	if len(chunk.ID) == 0 {
		chunk.Zero = true
		return nil
	}
	chunk.StoredInS3 = true

	object, err := s.reader.GetObject(ctx, ChunkKey(chunk.ID))
	if err == nil {
		err = chunks.DecodeS3Chunk(ctx, object, chunk, s.metrics)
	}
	if task_errors.Is(err, task_errors.NewEmptyNonRetriableError()) {
		return task_errors.NewNonRetriableErrorf(
			"failed to read backup chunk at index %v with id %q: %v",
			chunk.Index,
			chunk.ID,
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

// The chunk map must be validated before constructing the source and remain
// immutable for the duration of the transfer.
func NewSource(
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
