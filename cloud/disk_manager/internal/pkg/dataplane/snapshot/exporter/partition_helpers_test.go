package exporter

import (
	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
)

func filterPartitionEntriesFrom(
	entries []storage.ChunkMapEntry,
	startChunkIndex uint32,
) []storage.ChunkMapEntry {

	var filtered []storage.ChunkMapEntry
	for _, entry := range entries {
		if entry.ChunkIndex >= startChunkIndex {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func newPartitionStorageMock(
	meta storage.SnapshotMeta,
	entries []storage.ChunkMapEntry,
	startChunkIndex uint32,
	endChunkIndex uint32,
) *mocks.StorageMock {

	snapshotStorage := mocks.NewStorageMock()
	snapshotStorage.On("CheckSnapshotReady", mock.Anything, "snapshot").Return(meta, nil)

	if startChunkIndex == endChunkIndex {
		return snapshotStorage
	}

	entryChannel, errorChannel := newChunkMapChannels(
		filterPartitionEntriesFrom(entries, startChunkIndex),
		nil,
	)
	snapshotStorage.On(
		"ReadChunkMap",
		mock.Anything,
		"snapshot",
		startChunkIndex,
	).Return(entryChannel, errorChannel)
	snapshotStorage.On("ReadChunk", mock.Anything, mock.Anything).Run(
		fillChunkOnRead,
	).Return(nil)

	return snapshotStorage
}

func makeExpectedPartitionSnapshot(
	snapshotSize uint64,
	entries []storage.ChunkMapEntry,
) []byte {

	expected := make([]byte, int(snapshotSize))
	for _, entry := range entries {
		if len(entry.ChunkID) == 0 {
			continue
		}

		start := int(entry.ChunkIndex) * chunkSize
		if start >= len(expected) {
			continue
		}
		end := start + chunkSize
		if end > len(expected) {
			end = len(expected)
		}
		for i := start; i < end; i++ {
			expected[i] = chunkDataByte(entry.ChunkIndex)
		}
	}
	return expected
}

func expectedPartitionBytes(
	snapshot []byte,
	startChunkIndex uint32,
	endChunkIndex uint32,
) []byte {

	if startChunkIndex == endChunkIndex {
		return nil
	}

	start := int(startChunkIndex) * chunkSize
	end := int(endChunkIndex) * chunkSize
	if end > len(snapshot) {
		end = len(snapshot)
	}
	return snapshot[start:end]
}

func countPartitionChunks(
	entries []storage.ChunkMapEntry,
	startChunkIndex uint32,
	endChunkIndex uint32,
) (uint32, uint32) {

	entriesByIndex := make(map[uint32]storage.ChunkMapEntry)
	for _, entry := range entries {
		entriesByIndex[entry.ChunkIndex] = entry
	}

	var dataChunkCount uint32
	var zeroChunkCount uint32
	for chunkIndex := startChunkIndex; chunkIndex < endChunkIndex; chunkIndex++ {
		entry, ok := entriesByIndex[chunkIndex]
		if ok && len(entry.ChunkID) != 0 {
			dataChunkCount++
		} else {
			zeroChunkCount++
		}
	}
	return dataChunkCount, zeroChunkCount
}
