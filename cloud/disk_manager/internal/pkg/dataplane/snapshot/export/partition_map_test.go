package export

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
)

// The reader waits for cancellation after sending the first entry beyond the
// selected partition. This catches accidental scans of the remaining map.
type partitionCancellationStorage struct {
	*mocks.StorageMock
	entries []storage.ChunkMapEntry
}

func (s *partitionCancellationStorage) ReadChunkMap(
	ctx context.Context,
	_ string,
	milestoneChunkIndex uint32,
) (<-chan storage.ChunkMapEntry, <-chan error) {

	entries := make(chan storage.ChunkMapEntry)
	errors := make(chan error, 1)
	go func() {
		defer close(entries)
		defer close(errors)

		for _, entry := range s.entries {
			if entry.ChunkIndex < milestoneChunkIndex {
				continue
			}

			select {
			case entries <- entry:
			case <-ctx.Done():
				errors <- ctx.Err()
				return
			}

			if entry.ChunkIndex == 2 {
				<-ctx.Done()
				errors <- ctx.Err()
				return
			}
		}
	}()

	return entries, errors
}

func TestExportPartitionStopsReadingChunkMapAtPartitionEnd(t *testing.T) {
	baseStorage := mocks.NewStorageMock()
	baseStorage.On("CheckSnapshotReady", mock.Anything, "snapshot").Return(
		storage.SnapshotMeta{Size: 4 * chunkSize, ChunkCount: 4},
		nil,
	)
	baseStorage.On("ReadChunk", mock.Anything, mock.Anything).Run(
		fillChunkOnRead,
	).Return(nil)

	snapshotStorage := &partitionCancellationStorage{
		StorageMock: baseStorage,
		entries: []storage.ChunkMapEntry{
			{ChunkIndex: 0, ChunkID: "chunk-0"},
			{ChunkIndex: 1, ChunkID: "chunk-1"},
			{ChunkIndex: 2, ChunkID: "chunk-2"},
		},
	}

	ctx, cancel := context.WithTimeout(newContext(), time.Second)
	defer cancel()

	var dst bytes.Buffer
	stats, err := ExportPartitionToWriterWithReadWorkers(
		ctx,
		snapshotStorage,
		"snapshot",
		&dst,
		1,
		2,
		testWorkerCount,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2*chunkSize), stats.Size)
	require.Equal(t, uint32(2), stats.DataChunkCount)
	require.Len(t, dst.Bytes(), 2*chunkSize)
	baseStorage.AssertNumberOfCalls(t, "ReadChunk", 2)
}
