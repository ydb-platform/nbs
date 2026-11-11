package exporter

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
)

// The reader waits for cancellation after sending the first entry beyond the
// selected partition. This catches accidental scans of the remaining map.
type partitionCancellationStorage struct {
	*exportStorageMock
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
	baseStorage := newExportStorageMock()
	baseStorage.On("CheckSnapshotReady", mock.Anything, "snapshot").Return(
		storage.SnapshotMeta{Size: 4 * chunkSize, ChunkCount: 4},
		nil,
	)
	baseStorage.On("ReadChunk", mock.Anything, mock.Anything).Return(nil)

	snapshotStorage := &partitionCancellationStorage{
		exportStorageMock: baseStorage,
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

type partitionRangeStorage struct {
	*exportStorageMock
	start   uint32
	end     uint32
	entries []storage.ChunkMapEntry
	err     error
}

func (s *partitionRangeStorage) ReadChunkMapRange(
	ctx context.Context,
	snapshotID string,
	start uint32,
	end uint32,
) (<-chan storage.ChunkMapEntry, <-chan error) {

	s.start, s.end = start, end
	return newChunkMapChannels(s.entries, s.err)
}

func TestExportPartitionUsesBackendChunkMapRange(t *testing.T) {
	base := newExportStorageMock()
	base.On("CheckSnapshotReady", mock.Anything, "snapshot").Return(
		storage.SnapshotMeta{Size: 6 * chunkSize, ChunkCount: 6}, nil,
	)
	base.On("ReadChunk", mock.Anything, mock.Anything).Return(nil)
	source := &partitionRangeStorage{
		exportStorageMock: base,
		entries: []storage.ChunkMapEntry{
			{ChunkIndex: 2, ChunkID: "data-2"},
		},
	}

	var dst bytes.Buffer
	stats, err := ExportPartitionToWriterWithReadWorkers(
		newContext(), source, "snapshot", &dst, 2, 3, testWorkerCount,
	)
	require.NoError(t, err)
	require.EqualValues(t, 2, source.start)
	require.EqualValues(t, 4, source.end)
	require.EqualValues(t, 1, stats.DataChunkCount)
	require.EqualValues(t, 1, stats.ZeroChunkCount)
	require.Equal(t, make([]byte, chunkSize), dst.Bytes()[chunkSize:])
	base.AssertNumberOfCalls(t, "ReadChunk", 1)
	base.AssertNotCalled(
		t, "ReadChunkMap", mock.Anything, mock.Anything, mock.Anything,
	)
}

func TestExportBoundedChunkMapRejectsOutOfRangeRowsAndReadErrors(t *testing.T) {
	readErr := errors.New("bounded stream failed")
	for _, tc := range []struct {
		name    string
		entries []storage.ChunkMapEntry
		err     error
	}{
		{"before start", []storage.ChunkMapEntry{{ChunkIndex: 1}}, nil},
		{"at end", []storage.ChunkMapEntry{{ChunkIndex: 4}}, nil},
		{"stream error", []storage.ChunkMapEntry{{ChunkIndex: 2}}, readErr},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				source := &partitionRangeStorage{entries: tc.entries, err: tc.err}
				_, _, err := readStreamChunkMap(newContext(), source, "snapshot", 2, 4, 6)
				require.Error(t, err)
				if tc.err != nil {
					require.ErrorIs(t, err, tc.err)
				}
			},
		)
	}
}

func TestExportFinalPartitionStillRejectsChunksBeyondSnapshot(t *testing.T) {
	base := newExportStorageMock()
	entries, errors := newChunkMapChannels(
		[]storage.ChunkMapEntry{{ChunkIndex: 4}}, nil,
	)
	base.On("ReadChunkMap", mock.Anything, "snapshot", uint32(2)).Return(
		entries, errors,
	)
	source := &partitionRangeStorage{exportStorageMock: base}
	_, _, err := readStreamChunkMap(newContext(), source, "snapshot", 2, 4, 4)
	require.ErrorContains(t, err, "outside snapshot chunk count")
	base.AssertNumberOfCalls(t, "ReadChunkMap", 1)
}
