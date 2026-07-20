package exporter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

const testWorkerCount = 3

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.InfoLevel),
	)
}

func chunkDataByte(chunkIndex uint32) byte {
	return byte(chunkIndex) + 1
}

func newChunkMapChannels(
	entries []storage.ChunkMapEntry,
	err error,
) (<-chan storage.ChunkMapEntry, <-chan error) {

	entryChannel := make(chan storage.ChunkMapEntry, len(entries))
	for _, entry := range entries {
		entryChannel <- entry
	}
	close(entryChannel)

	errorChannel := make(chan error, 1)
	if err != nil {
		errorChannel <- err
	}
	close(errorChannel)

	return entryChannel, errorChannel
}

func fillChunkOnRead(args mock.Arguments) {
	chunk := args.Get(1).(*dataplane_common.Chunk)
	for i := range chunk.Data {
		chunk.Data[i] = chunkDataByte(chunk.Index)
	}
}

func assertTwoChunkStream(t *testing.T, data []byte) {
	expected := make([]byte, 2*chunkSize)
	for i := 0; i < chunkSize; i++ {
		expected[i] = chunkDataByte(0)
	}
	for i := chunkSize; i < 2*chunkSize; i++ {
		expected[i] = chunkDataByte(1)
	}
	require.Equal(t, expected, data)
}

////////////////////////////////////////////////////////////////////////////////

func TestExportPartitionWritesRawImageStream(t *testing.T) {
	ctx := newContext()

	entries := []storage.ChunkMapEntry{
		{ChunkIndex: 0, ChunkID: "chunk-0", StoredInS3: true},
		{ChunkIndex: 1, ChunkID: ""}, // Zero chunk.
		{ChunkIndex: 2, ChunkID: "chunk-2"},
	}

	const partialLastChunkSize = 123
	snapshotSize := uint64(2*chunkSize + partialLastChunkSize)
	snapshotStorage := mocks.NewStorageMock()
	snapshotStorage.On("CheckSnapshotReady", mock.Anything, "snapshot").Return(
		storage.SnapshotMeta{Size: snapshotSize, ChunkCount: 3},
		nil,
	)
	entryChannel, errorChannel := newChunkMapChannels(entries, nil)
	snapshotStorage.On("ReadChunkMap", mock.Anything, "snapshot", uint32(0)).Return(
		entryChannel,
		errorChannel,
	)
	snapshotStorage.On("ReadChunk", mock.Anything, mock.Anything).Run(
		fillChunkOnRead,
	).Return(nil)

	var dst bytes.Buffer

	stats, err := ExportPartitionToWriterWithReadWorkers(
		ctx,
		snapshotStorage,
		"snapshot",
		&dst,
		1, // partition
		1, // partitionCount
		testWorkerCount,
	)
	require.NoError(t, err)

	require.Equal(t, snapshotSize, stats.Size)
	require.Equal(t, uint32(2), stats.DataChunkCount)
	require.Equal(t, uint32(1), stats.ZeroChunkCount)

	expected := make([]byte, int(snapshotSize))
	for i := 0; i < chunkSize; i++ {
		expected[i] = chunkDataByte(0)
	}
	for i := 2 * chunkSize; i < len(expected); i++ {
		expected[i] = chunkDataByte(2)
	}
	require.Equal(t, expected, dst.Bytes())

	// Zero chunk should not be read from the storage.
	snapshotStorage.AssertNumberOfCalls(t, "ReadChunk", 2)
}

func TestExportPartitionReadsChunksConcurrently(t *testing.T) {
	ctx := newContext()

	entries := []storage.ChunkMapEntry{
		{ChunkIndex: 0, ChunkID: "chunk-0", StoredInS3: true},
		{ChunkIndex: 1, ChunkID: "chunk-1", StoredInS3: true},
	}

	snapshotStorage := mocks.NewStorageMock()
	snapshotStorage.On("CheckSnapshotReady", mock.Anything, "snapshot").Return(
		storage.SnapshotMeta{Size: 2 * chunkSize, ChunkCount: 2},
		nil,
	)
	entryChannel, errorChannel := newChunkMapChannels(entries, nil)
	snapshotStorage.On("ReadChunkMap", mock.Anything, "snapshot", uint32(0)).Return(
		entryChannel,
		errorChannel,
	)

	readStarted := make(chan uint32, len(entries))
	releaseReads := make(chan struct{})
	snapshotStorage.On("ReadChunk", mock.Anything, mock.Anything).Run(
		func(args mock.Arguments) {
			chunk := args.Get(1).(*dataplane_common.Chunk)
			readStarted <- chunk.Index
			<-releaseReads
			fillChunkOnRead(args)
		},
	).Return(nil)

	var dst bytes.Buffer
	type exportResult struct {
		stats Stats
		err   error
	}
	result := make(chan exportResult, 1)

	go func() {
		stats, err := ExportPartitionToWriterWithReadWorkers(
			ctx,
			snapshotStorage,
			"snapshot",
			&dst,
			1, // partition
			1, // partitionCount
			2, // readWorkerCount
		)
		result <- exportResult{stats: stats, err: err}
	}()

	started := make(map[uint32]bool)
	for len(started) < len(entries) {
		select {
		case chunkIndex := <-readStarted:
			started[chunkIndex] = true
		case <-time.After(time.Second):
			close(releaseReads)
			t.Fatal("timed out waiting for concurrent chunk reads")
		}
	}
	close(releaseReads)

	select {
	case res := <-result:
		require.NoError(t, res.err)
		require.Equal(t, uint32(2), res.stats.DataChunkCount)
		require.Equal(t, uint32(0), res.stats.ZeroChunkCount)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for export to finish")
	}

	assertTwoChunkStream(t, dst.Bytes())
	snapshotStorage.AssertNumberOfCalls(t, "ReadChunk", 2)
}

func TestExportPartitionWritesOutOfOrderReadsInChunkOrder(t *testing.T) {
	ctx := newContext()

	entries := []storage.ChunkMapEntry{
		{ChunkIndex: 0, ChunkID: "chunk-0"},
		{ChunkIndex: 1, ChunkID: "chunk-1"},
	}

	snapshotStorage := mocks.NewStorageMock()
	snapshotStorage.On("CheckSnapshotReady", mock.Anything, "snapshot").Return(
		storage.SnapshotMeta{Size: 2 * chunkSize, ChunkCount: 2},
		nil,
	)
	entryChannel, errorChannel := newChunkMapChannels(entries, nil)
	snapshotStorage.On("ReadChunkMap", mock.Anything, "snapshot", uint32(0)).Return(
		entryChannel,
		errorChannel,
	)

	readStarted := make(chan uint32, len(entries))
	releaseChunkZero := make(chan struct{})
	chunkOneRead := make(chan struct{})
	snapshotStorage.On("ReadChunk", mock.Anything, mock.Anything).Run(
		func(args mock.Arguments) {
			chunk := args.Get(1).(*dataplane_common.Chunk)
			readStarted <- chunk.Index
			if chunk.Index == 0 {
				<-releaseChunkZero
			} else {
				fillChunkOnRead(args)
				close(chunkOneRead)
				return
			}
			fillChunkOnRead(args)
		},
	).Return(nil)

	var dst bytes.Buffer
	type exportResult struct {
		stats Stats
		err   error
	}
	result := make(chan exportResult, 1)

	go func() {
		stats, err := ExportPartitionToWriterWithReadWorkers(
			ctx,
			snapshotStorage,
			"snapshot",
			&dst,
			1, // partition
			1, // partitionCount
			2, // readWorkerCount
		)
		result <- exportResult{stats: stats, err: err}
	}()

	started := make(map[uint32]bool)
	for len(started) < len(entries) {
		select {
		case chunkIndex := <-readStarted:
			started[chunkIndex] = true
		case <-time.After(time.Second):
			close(releaseChunkZero)
			t.Fatal("timed out waiting for concurrent chunk reads")
		}
	}

	select {
	case <-chunkOneRead:
	case <-time.After(time.Second):
		close(releaseChunkZero)
		t.Fatal("timed out waiting for chunk 1 read")
	}
	close(releaseChunkZero)

	select {
	case res := <-result:
		require.NoError(t, res.err)
		require.Equal(t, uint32(2), res.stats.DataChunkCount)
		require.Equal(t, uint32(0), res.stats.ZeroChunkCount)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for export to finish")
	}

	assertTwoChunkStream(t, dst.Bytes())
}

func TestExportPartitionBoundsOutOfOrderReadBuffering(t *testing.T) {
	ctx := newContext()

	readWorkerCount := 2
	readAheadChunkCount := readWorkerCount * streamReadAheadMultiplier
	chunkCount := uint32(readAheadChunkCount + 1)

	entries := make([]storage.ChunkMapEntry, 0, chunkCount)
	for chunkIndex := uint32(0); chunkIndex < chunkCount; chunkIndex++ {
		entries = append(entries, storage.ChunkMapEntry{
			ChunkIndex: chunkIndex,
			ChunkID:    fmt.Sprintf("chunk-%v", chunkIndex),
		})
	}

	snapshotStorage := mocks.NewStorageMock()
	snapshotStorage.On("CheckSnapshotReady", mock.Anything, "snapshot").Return(
		storage.SnapshotMeta{
			Size:       uint64(chunkCount) * chunkSize,
			ChunkCount: chunkCount,
		},
		nil,
	)
	entryChannel, errorChannel := newChunkMapChannels(entries, nil)
	snapshotStorage.On("ReadChunkMap", mock.Anything, "snapshot", uint32(0)).Return(
		entryChannel,
		errorChannel,
	)

	readStarted := make(chan uint32, int(chunkCount))
	releaseChunkZero := make(chan struct{})
	var releaseChunkZeroOnce sync.Once
	release := func() {
		releaseChunkZeroOnce.Do(func() {
			close(releaseChunkZero)
		})
	}
	defer release()

	snapshotStorage.On("ReadChunk", mock.Anything, mock.Anything).Run(
		func(args mock.Arguments) {
			chunk := args.Get(1).(*dataplane_common.Chunk)
			readStarted <- chunk.Index
			if chunk.Index == 0 {
				<-releaseChunkZero
			}
			fillChunkOnRead(args)
		},
	).Return(nil)

	type exportResult struct {
		stats Stats
		err   error
	}
	result := make(chan exportResult, 1)

	go func() {
		stats, err := ExportPartitionToWriterWithReadWorkers(
			ctx,
			snapshotStorage,
			"snapshot",
			io.Discard,
			1, // partition
			1, // partitionCount
			readWorkerCount,
		)
		result <- exportResult{stats: stats, err: err}
	}()

	started := make(map[uint32]bool)
	for len(started) < readAheadChunkCount {
		select {
		case chunkIndex := <-readStarted:
			require.Less(t, int(chunkIndex), readAheadChunkCount)
			started[chunkIndex] = true
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for read-ahead chunk reads")
		}
	}

	select {
	case chunkIndex := <-readStarted:
		require.Less(
			t,
			int(chunkIndex),
			readAheadChunkCount,
			"started another read while out-of-order chunks filled the read-ahead budget",
		)
	case <-time.After(200 * time.Millisecond):
	}

	release()

	select {
	case res := <-result:
		require.NoError(t, res.err)
		require.Equal(t, chunkCount, res.stats.DataChunkCount)
		require.Equal(t, uint32(0), res.stats.ZeroChunkCount)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for export to finish")
	}
}

func TestExportPartitionWritesMissingChunksAsZeroes(t *testing.T) {
	ctx := newContext()

	entries := []storage.ChunkMapEntry{
		{ChunkIndex: 0, ChunkID: "chunk-0"},
	}

	snapshotStorage := mocks.NewStorageMock()
	snapshotStorage.On("CheckSnapshotReady", mock.Anything, "snapshot").Return(
		storage.SnapshotMeta{Size: 2 * chunkSize, ChunkCount: 2},
		nil,
	)
	entryChannel, errorChannel := newChunkMapChannels(entries, nil)
	snapshotStorage.On("ReadChunkMap", mock.Anything, "snapshot", uint32(0)).Return(
		entryChannel,
		errorChannel,
	)
	snapshotStorage.On("ReadChunk", mock.Anything, mock.Anything).Run(
		fillChunkOnRead,
	).Return(nil)

	var dst bytes.Buffer

	stats, err := ExportPartitionToWriterWithReadWorkers(
		ctx,
		snapshotStorage,
		"snapshot",
		&dst,
		1, // partition
		1, // partitionCount
		testWorkerCount,
	)
	require.NoError(t, err)

	require.Equal(t, uint64(2*chunkSize), stats.Size)
	require.Equal(t, uint32(1), stats.DataChunkCount)
	require.Equal(t, uint32(1), stats.ZeroChunkCount)

	expected := make([]byte, 2*chunkSize)
	for i := 0; i < chunkSize; i++ {
		expected[i] = chunkDataByte(0)
	}
	require.Equal(t, expected, dst.Bytes())
	snapshotStorage.AssertNumberOfCalls(t, "ReadChunk", 1)
}

func TestExportPartitionFailsOnChunkReadError(t *testing.T) {
	ctx := newContext()

	entries := []storage.ChunkMapEntry{
		{ChunkIndex: 0, ChunkID: "chunk-0"},
	}

	snapshotStorage := mocks.NewStorageMock()
	snapshotStorage.On("CheckSnapshotReady", mock.Anything, "snapshot").Return(
		storage.SnapshotMeta{Size: chunkSize, ChunkCount: 1},
		nil,
	)
	entryChannel, errorChannel := newChunkMapChannels(entries, nil)
	snapshotStorage.On("ReadChunkMap", mock.Anything, "snapshot", uint32(0)).Return(
		entryChannel,
		errorChannel,
	)
	snapshotStorage.On("ReadChunk", mock.Anything, mock.Anything).Return(
		fmt.Errorf("chunk read failed"),
	)

	var dst bytes.Buffer

	_, err := ExportPartitionToWriterWithReadWorkers(
		ctx,
		snapshotStorage,
		"snapshot",
		&dst,
		1, // partition
		1, // partitionCount
		testWorkerCount,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "chunk read failed")
}

func TestExportPartitionFailsOnChunkMapError(t *testing.T) {
	ctx := newContext()

	entries := []storage.ChunkMapEntry{
		{ChunkIndex: 0, ChunkID: "chunk-0"},
	}

	snapshotStorage := mocks.NewStorageMock()
	snapshotStorage.On("CheckSnapshotReady", mock.Anything, "snapshot").Return(
		storage.SnapshotMeta{Size: 2 * chunkSize, ChunkCount: 2},
		nil,
	)
	entryChannel, errorChannel := newChunkMapChannels(
		entries,
		fmt.Errorf("chunk map read failed"),
	)
	snapshotStorage.On("ReadChunkMap", mock.Anything, "snapshot", uint32(0)).Return(
		entryChannel,
		errorChannel,
	)

	var dst bytes.Buffer

	_, err := ExportPartitionToWriterWithReadWorkers(
		ctx,
		snapshotStorage,
		"snapshot",
		&dst,
		1, // partition
		1, // partitionCount
		testWorkerCount,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "chunk map read failed")
	snapshotStorage.AssertNumberOfCalls(t, "ReadChunk", 0)
}

func TestExportPartitionFailsWhenSnapshotIsNotReady(t *testing.T) {
	ctx := newContext()

	snapshotStorage := mocks.NewStorageMock()
	snapshotStorage.On("CheckSnapshotReady", mock.Anything, "snapshot").Return(
		storage.SnapshotMeta{},
		fmt.Errorf("snapshot is not ready"),
	)

	var dst bytes.Buffer

	_, err := ExportPartitionToWriterWithReadWorkers(
		ctx,
		snapshotStorage,
		"snapshot",
		&dst,
		1, // partition
		1, // partitionCount
		testWorkerCount,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot is not ready")

	snapshotStorage.AssertNumberOfCalls(t, "ReadChunkMap", 0)
}

func TestReadStreamChunkMapValidation(t *testing.T) {
	testCases := []struct {
		name           string
		startChunk     uint32
		endChunk       uint32
		chunkCount     uint32
		entries        []storage.ChunkMapEntry
		errorSubstring string
	}{
		{
			name:       "unordered",
			startChunk: 0,
			endChunk:   2,
			chunkCount: 2,
			entries: []storage.ChunkMapEntry{
				{ChunkIndex: 1, ChunkID: "chunk-1"},
				{ChunkIndex: 0, ChunkID: "chunk-0"},
			},
			errorSubstring: "chunk map is not ordered",
		},
		{
			name:       "before range",
			startChunk: 1,
			endChunk:   3,
			chunkCount: 3,
			entries: []storage.ChunkMapEntry{
				{ChunkIndex: 0, ChunkID: "chunk-0"},
			},
			errorSubstring: "before requested chunk range",
		},
		{
			name:       "outside chunk count",
			startChunk: 0,
			endChunk:   2,
			chunkCount: 2,
			entries: []storage.ChunkMapEntry{
				{ChunkIndex: 2, ChunkID: "chunk-2"},
			},
			errorSubstring: "outside snapshot chunk count",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := newContext()
			snapshotStorage := mocks.NewStorageMock()
			entryChannel, errorChannel := newChunkMapChannels(testCase.entries, nil)
			snapshotStorage.On(
				"ReadChunkMap",
				mock.Anything,
				"snapshot",
				testCase.startChunk,
			).Return(entryChannel, errorChannel)

			_, _, err := readStreamChunkMap(
				ctx,
				snapshotStorage,
				"snapshot",
				testCase.startChunk,
				testCase.endChunk,
				testCase.chunkCount,
			)
			require.Error(t, err)
			require.Contains(t, err.Error(), testCase.errorSubstring)
		})
	}
}
