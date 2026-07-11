package export

import (
	"bytes"
	"context"
	"fmt"
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

////////////////////////////////////////////////////////////////////////////////

// In-memory Destination for tests.
type memDestination struct {
	mutex         sync.Mutex
	data          []byte
	truncatedSize int64
}

func newMemDestination(size uint64) *memDestination {
	return &memDestination{data: make([]byte, size)}
}

func (d *memDestination) WriteAt(p []byte, off int64) (int, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if off < 0 || int(off)+len(p) > len(d.data) {
		return 0, fmt.Errorf("write out of range: off %v, len %v", off, len(p))
	}

	copy(d.data[off:], p)
	return len(p), nil
}

func (d *memDestination) Truncate(size int64) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.truncatedSize = size
	return nil
}

////////////////////////////////////////////////////////////////////////////////

func TestExportWritesChunksAtTheirOffsets(t *testing.T) {
	ctx := newContext()

	entries := []storage.ChunkMapEntry{
		{ChunkIndex: 0, ChunkID: "chunk-0", StoredInS3: true},
		{ChunkIndex: 1, ChunkID: ""}, // Zero chunk.
		{ChunkIndex: 2, ChunkID: "chunk-2"},
	}

	snapshotStorage := mocks.NewStorageMock()
	snapshotStorage.On("CheckSnapshotReady", mock.Anything, "snapshot").Return(
		storage.SnapshotMeta{Size: 3 * chunkSize, ChunkCount: 3},
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

	dst := newMemDestination(3 * chunkSize)

	stats, err := Export(ctx, snapshotStorage, "snapshot", dst, testWorkerCount)
	require.NoError(t, err)

	require.Equal(t, uint64(3*chunkSize), stats.Size)
	require.Equal(t, uint32(2), stats.DataChunkCount)
	require.Equal(t, uint32(1), stats.ZeroChunkCount)
	require.Equal(t, int64(3*chunkSize), dst.truncatedSize)

	expected := make([]byte, 3*chunkSize)
	for i := 0; i < chunkSize; i++ {
		expected[i] = chunkDataByte(0)
	}
	for i := 2 * chunkSize; i < 3*chunkSize; i++ {
		expected[i] = chunkDataByte(2)
	}
	require.True(t, bytes.Equal(expected, dst.data))

	// Zero chunk should not be read from the storage.
	snapshotStorage.AssertNumberOfCalls(t, "ReadChunk", 2)
}

func TestExportToWriterWritesRawImageStream(t *testing.T) {
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

	stats, err := ExportToWriter(ctx, snapshotStorage, "snapshot", &dst)
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

func TestExportToWriterReadsChunksConcurrently(t *testing.T) {
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
		stats, err := ExportToWriterWithReadWorkers(
			ctx,
			snapshotStorage,
			"snapshot",
			&dst,
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

	expected := make([]byte, 2*chunkSize)
	for i := 0; i < chunkSize; i++ {
		expected[i] = chunkDataByte(0)
	}
	for i := chunkSize; i < 2*chunkSize; i++ {
		expected[i] = chunkDataByte(1)
	}
	require.Equal(t, expected, dst.Bytes())

	snapshotStorage.AssertNumberOfCalls(t, "ReadChunk", 2)
}

func TestExportToWriterWritesMissingChunksAsZeroes(t *testing.T) {
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

	stats, err := ExportToWriter(ctx, snapshotStorage, "snapshot", &dst)
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

func TestExportFailsOnChunkReadError(t *testing.T) {
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

	dst := newMemDestination(chunkSize)

	_, err := Export(ctx, snapshotStorage, "snapshot", dst, testWorkerCount)
	require.Error(t, err)
	require.Contains(t, err.Error(), "chunk read failed")

	// Destination should not be truncated after a failed export.
	require.Equal(t, int64(0), dst.truncatedSize)
}

func TestExportFailsOnChunkMapError(t *testing.T) {
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
	snapshotStorage.On("ReadChunk", mock.Anything, mock.Anything).Run(
		fillChunkOnRead,
	).Return(nil)

	dst := newMemDestination(2 * chunkSize)

	_, err := Export(ctx, snapshotStorage, "snapshot", dst, testWorkerCount)
	require.Error(t, err)
	require.Contains(t, err.Error(), "chunk map read failed")
	require.Equal(t, int64(0), dst.truncatedSize)
}

func TestExportFailsOnNonPositiveWorkerCount(t *testing.T) {
	ctx := newContext()

	snapshotStorage := mocks.NewStorageMock()
	dst := newMemDestination(chunkSize)

	_, err := Export(ctx, snapshotStorage, "snapshot", dst, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "workerCount must be positive")
}

func TestExportFailsWhenSnapshotIsNotReady(t *testing.T) {
	ctx := newContext()

	snapshotStorage := mocks.NewStorageMock()
	snapshotStorage.On("CheckSnapshotReady", mock.Anything, "snapshot").Return(
		storage.SnapshotMeta{},
		fmt.Errorf("snapshot is not ready"),
	)

	dst := newMemDestination(chunkSize)

	_, err := Export(ctx, snapshotStorage, "snapshot", dst, testWorkerCount)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot is not ready")

	snapshotStorage.AssertNumberOfCalls(t, "ReadChunkMap", 0)
}
