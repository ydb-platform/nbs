package exporter

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
)

func TestExportPartitionsConcatenateToCompleteSnapshot(t *testing.T) {
	ctx := newContext()
	entries := []storage.ChunkMapEntry{
		{ChunkIndex: 0, ChunkID: "chunk-0", StoredInS3: true},
		{ChunkIndex: 1, ChunkID: ""}, // Explicit zero chunk.
		{ChunkIndex: 2, ChunkID: "chunk-2"},
		// Chunk 3 is absent from the map and must be emitted as zeroes.
		{ChunkIndex: 4, ChunkID: "chunk-4", StoredInS3: true},
		{ChunkIndex: 5, ChunkID: "chunk-5"},
	}
	const partialLastChunkSize = 321
	meta := storage.SnapshotMeta{
		Size:       5*chunkSize + partialLastChunkSize,
		ChunkCount: 6,
	}
	expectedSnapshot := makeExpectedPartitionSnapshot(meta.Size, entries)
	expectedChecksum := sha256.Sum256(expectedSnapshot)

	for _, partitionCount := range []uint32{1, 2, 3, 4, 7, 10} {
		t.Run(fmt.Sprintf("partition-count-%v", partitionCount), func(t *testing.T) {
			var concatenated bytes.Buffer
			var totalDataChunkCount uint32
			var totalZeroChunkCount uint32

			for partition := uint32(1); partition <= partitionCount; partition++ {
				start, end, err := partitionChunkRange(
					meta.ChunkCount,
					partition,
					partitionCount,
				)
				require.NoError(t, err)

				snapshotStorage := newPartitionStorageMock(meta, entries, start, end)
				var part bytes.Buffer
				stats, err := ExportPartitionToWriterWithReadWorkers(
					ctx,
					snapshotStorage,
					"snapshot",
					&part,
					partition,
					partitionCount,
					testWorkerCount,
				)
				require.NoError(t, err)

				expectedPart := expectedPartitionBytes(expectedSnapshot, start, end)
				require.Equal(t, expectedPart, part.Bytes())
				require.Equal(t, uint64(len(expectedPart)), stats.Size)

				expectedDataChunks, expectedZeroChunks := countPartitionChunks(
					entries,
					start,
					end,
				)
				require.Equal(t, expectedDataChunks, stats.DataChunkCount)
				require.Equal(t, expectedZeroChunks, stats.ZeroChunkCount)
				snapshotStorage.AssertNumberOfCalls(t, "ReadChunk", int(expectedDataChunks))
				if start == end {
					snapshotStorage.AssertNumberOfCalls(t, "ReadChunkMap", 0)
				} else {
					snapshotStorage.AssertNumberOfCalls(t, "ReadChunkMap", 1)
				}

				totalDataChunkCount += stats.DataChunkCount
				totalZeroChunkCount += stats.ZeroChunkCount
				_, err = concatenated.Write(part.Bytes())
				require.NoError(t, err)
			}

			require.Equal(t, expectedSnapshot, concatenated.Bytes())
			require.Equal(t, expectedChecksum, sha256.Sum256(concatenated.Bytes()))
			require.Equal(t, uint32(4), totalDataChunkCount)
			require.Equal(t, uint32(2), totalZeroChunkCount)
		})
	}
}
