package snapshot

import (
	"context"
	"sync"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/accounting"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type snapshotSource struct {
	snapshotID string
	storage    storage.Storage

	chunkMap      map[uint32]storage.ChunkMapEntry
	chunkMapMutex sync.Mutex
	chunkIndices  common.ChannelWithInflightQueue
	snapshotMeta  *storage.SnapshotMeta
}

func (s *snapshotSource) ChunkIndices(
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

		entries, entriesErrors := s.storage.ReadChunkMap(
			ctx,
			s.snapshotID,
			milestone.ChunkIndex,
		)

		var entry storage.ChunkMapEntry
		more := true

		for more {
			select {
			case entry, more = <-entries:
				if !more {
					break
				}

				err := s.send(ctx, entry)
				if err != nil {
					errors <- err
					return
				}
			case <-ctx.Done():
				errors <- ctx.Err()
				return
			}
		}

		err := <-entriesErrors
		if err != nil {
			errors <- err
		}
	}()

	return s.chunkIndices.Channel(), common.ChannelWithCancellation{}, errors
}

func (s *snapshotSource) Read(
	ctx context.Context,
	chunk *dataplane_common.Chunk,
) error {

	s.chunkMapMutex.Lock()
	entry, ok := s.chunkMap[chunk.Index]
	s.chunkMapMutex.Unlock()

	chunk.ID = entry.ChunkID
	chunk.StoredInS3 = entry.StoredInS3

	if !ok {
		// TODO: maybe reread chunk map
		return task_errors.NewNonRetriableErrorf(
			"failed to find chunk for index %v",
			chunk.Index,
		)
	}

	var err error
	if len(chunk.ID) != 0 {
		err = s.storage.ReadChunk(ctx, chunk)
	} else {
		chunk.Zero = true
	}
	if err == nil {
		// Evict successully read chunk to keep low memory footprint.
		s.chunkMapMutex.Lock()
		delete(s.chunkMap, chunk.Index)
		s.chunkMapMutex.Unlock()

		accounting.OnSnapshotRead(s.snapshotID, len(chunk.Data))
	}

	return err
}

func (s *snapshotSource) Milestone() dataplane_common.Milestone {
	common.Assert(!s.chunkIndices.Empty(), "should not be empty")

	milestone := s.chunkIndices.Milestone()
	return dataplane_common.Milestone{
		ChunkIndex:            milestone.Value,
		TransferredChunkCount: milestone.ProcessedValueCount,
	}
}

// Not thread-safe.
func (s *snapshotSource) ChunkCount(ctx context.Context) (uint32, error) {
	meta, err := s.getSnapshotMeta(ctx)
	if err != nil {
		return 0, err
	}

	return meta.ChunkCount, nil
}

// Not thread-safe.
func (s *snapshotSource) Size(ctx context.Context) (uint64, error) {
	meta, err := s.getSnapshotMeta(ctx)
	if err != nil {
		return 0, err
	}

	return meta.StorageSize, nil
}

func (s *snapshotSource) Close(ctx context.Context) {
}

////////////////////////////////////////////////////////////////////////////////

func (s *snapshotSource) getSnapshotMeta(ctx context.Context) (*storage.SnapshotMeta, error) {
	if s.snapshotMeta == nil {
		meta, err := s.storage.CheckSnapshotReady(ctx, s.snapshotID)
		if err != nil {
			return nil, err
		}

		s.snapshotMeta = &meta
	}

	return s.snapshotMeta, nil
}

func (s *snapshotSource) send(
	ctx context.Context,
	entry storage.ChunkMapEntry,
) error {

	// Cache chunk entry for further reads.
	s.chunkMapMutex.Lock()
	s.chunkMap[entry.ChunkIndex] = entry
	s.chunkMapMutex.Unlock()

	ok, err := s.chunkIndices.Send(ctx, entry.ChunkIndex)
	if err != nil {
		return err
	}

	if !ok {
		// Evict chunk entry.
		// TODO: get rid of chunkMap to avoid this eviction.
		s.chunkMapMutex.Lock()
		delete(s.chunkMap, entry.ChunkIndex)
		s.chunkMapMutex.Unlock()
	}

	return nil
}

func (s *snapshotSource) shallowCopy(
	ctx context.Context,
	chunkIndex uint32,
	dstSnapshotID string,
) error {

	s.chunkMapMutex.Lock()
	srcEntry, ok := s.chunkMap[chunkIndex]
	s.chunkMapMutex.Unlock()

	if !ok {
		// TODO: maybe reread chunk map
		return task_errors.NewNonRetriableErrorf(
			"failed to find chunk for index %v",
			chunkIndex,
		)
	}

	err := s.storage.ShallowCopyChunk(ctx, srcEntry, dstSnapshotID)
	if err == nil {
		// Evict successully copied chunk to keep low memory footprint.
		s.chunkMapMutex.Lock()
		delete(s.chunkMap, chunkIndex)
		s.chunkMapMutex.Unlock()
	}

	return err
}

////////////////////////////////////////////////////////////////////////////////

func newSnapshotSource(
	snapshotID string,
	snapshotStorage storage.Storage,
) *snapshotSource {

	return &snapshotSource{
		snapshotID: snapshotID,
		storage:    snapshotStorage,
		chunkMap:   make(map[uint32]storage.ChunkMapEntry),
	}
}

func NewSnapshotSource(
	snapshotID string,
	snapshotStorage storage.Storage,
) dataplane_common.Source {

	return newSnapshotSource(snapshotID, snapshotStorage)
}
