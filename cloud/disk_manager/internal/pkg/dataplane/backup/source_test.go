package backup

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/compressor"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	common_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

const testChunkSize = 4096

func newTestContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

func newTestMetrics() metrics.Metrics {
	return metrics.New(common_metrics.NewEmptyRegistry(), "backup")
}

func newTestChunkData(chunkIndex int) []byte {
	data := make([]byte, testChunkSize)
	for i := range data {
		data[i] = byte(1 + (chunkIndex+i)%250)
	}

	return data
}

func newTestChunk(chunkIndex uint32) *dataplane_common.Chunk {
	return &dataplane_common.Chunk{
		Index: chunkIndex,
		Data:  make([]byte, testChunkSize),
	}
}

////////////////////////////////////////////////////////////////////////////////

// Returns objects the way S3 does: with the backup encryption envelope
// removed.
type fakeObjectReader struct {
	mutex         sync.Mutex
	objects       map[string]persistence.S3Object
	errors        map[string]error
	requestedKeys []string
}

func newFakeObjectReader() *fakeObjectReader {
	return &fakeObjectReader{
		objects: make(map[string]persistence.S3Object),
		errors:  make(map[string]error),
	}
}

func (r *fakeObjectReader) GetObject(
	ctx context.Context,
	key string,
) (persistence.S3Object, error) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.requestedKeys = append(r.requestedKeys, key)

	err, ok := r.errors[key]
	if ok {
		return persistence.S3Object{}, err
	}

	object, ok := r.objects[key]
	if !ok {
		// The same error as persistence.S3Client returns for a missing key.
		return persistence.S3Object{}, errors.NewSilentNonRetriableErrorf(
			"s3 object not found: %v",
			key,
		)
	}

	return object, nil
}

// Stores the chunk the way backup tasks do.
func (r *fakeObjectReader) putChunk(
	t *testing.T,
	chunkID string,
	data []byte,
	compression string,
) {

	compressedData, err := compressor.Compress(
		compression,
		data,
		newTestMetrics(),
		nil, // probeCompressionPercentage
	)
	require.NoError(t, err)

	r.objects[ChunkKey(chunkID)] = chunks.NewS3Object(chunks.ChunkBlob{
		Data:        compressedData,
		Checksum:    dataplane_common.Chunk{Data: data}.Checksum(),
		Compression: compression,
	})
}

// Puts chunks of a backup with |chunkCount| chunks, every third of them is
// zero. Returns the chunk map and the data of the backed up disk.
func (r *fakeObjectReader) putBackup(
	t *testing.T,
	chunkCount int,
) ([]string, []byte) {

	chunkIDs := make([]string, 0, chunkCount)
	diskData := make([]byte, 0, chunkCount*testChunkSize)

	for i := 0; i < chunkCount; i++ {
		if i%3 == 1 {
			chunkIDs = append(chunkIDs, "")
			diskData = append(diskData, make([]byte, testChunkSize)...)
			continue
		}

		compression := ""
		if i%2 == 0 {
			compression = "lz4"
		}

		// The id of a chunk says nothing about its position in the backup.
		chunkID := fmt.Sprintf("chunk%v", chunkCount-i)
		data := newTestChunkData(i)
		r.putChunk(t, chunkID, data, compression)

		chunkIDs = append(chunkIDs, chunkID)
		diskData = append(diskData, data...)
	}

	return chunkIDs, diskData
}

////////////////////////////////////////////////////////////////////////////////

// Disk that is filled with garbage before the transfer.
type fakeDisk struct {
	mutex               sync.Mutex
	data                []byte
	writtenChunkIndices []uint32
	// Called before the chunk is written, the chunk is not written if it fails.
	beforeWrite func(ctx context.Context, chunkIndex uint32) error
}

func newFakeDisk(chunkCount int) *fakeDisk {
	return &fakeDisk{
		data: bytes.Repeat([]byte{0xFF}, chunkCount*testChunkSize),
	}
}

func (d *fakeDisk) Write(
	ctx context.Context,
	chunk dataplane_common.Chunk,
) error {

	if d.beforeWrite != nil {
		err := d.beforeWrite(ctx, chunk.Index)
		if err != nil {
			return err
		}
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	data := chunk.Data
	if chunk.Zero {
		data = make([]byte, testChunkSize)
	}

	copy(d.data[int(chunk.Index)*testChunkSize:], data)
	d.writtenChunkIndices = append(d.writtenChunkIndices, chunk.Index)
	return nil
}

func (d *fakeDisk) Close(ctx context.Context) {
}

func (d *fakeDisk) isWritten(chunkIndex uint32) bool {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for _, index := range d.writtenChunkIndices {
		if index == chunkIndex {
			return true
		}
	}

	return false
}

////////////////////////////////////////////////////////////////////////////////

func newTestTransferer() dataplane_common.Transferer {
	return dataplane_common.Transferer{
		ReaderCount:         3,
		WriterCount:         3,
		ChunksInflightLimit: 5,
		ChunkSize:           testChunkSize,
	}
}

func requireNonSilentChunkError(
	t *testing.T,
	err error,
	chunkIndex uint32,
	chunkID string,
) {

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.False(t, errors.Is(err, errors.NewEmptyRetriableError()))
	require.False(t, errors.IsSilent(err))
	require.ErrorContains(t, err, fmt.Sprintf("index %v", chunkIndex))
	require.ErrorContains(t, err, fmt.Sprintf("id %q", chunkID))
}

////////////////////////////////////////////////////////////////////////////////

func TestBackupSourceRead(t *testing.T) {
	ctx := newTestContext()

	reader := newFakeObjectReader()
	reader.putChunk(t, "chunk0", newTestChunkData(0), "")
	reader.putChunk(t, "chunk2", newTestChunkData(2), "lz4")

	source := NewBackupSource(
		reader,
		[]string{"chunk0", "", "chunk2"},
		123, // storageSize
		newTestMetrics(),
	)
	defer source.Close(ctx)

	chunkCount, err := source.ChunkCount(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 3, chunkCount)

	bytesToRead, err := source.EstimatedBytesToRead(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 123, bytesToRead)

	// The chunk is reused the way the transferer reuses chunks of its pool.
	chunk := newTestChunk(0)
	err = source.Read(ctx, chunk)
	require.NoError(t, err)
	require.Equal(t, "chunk0", chunk.ID)
	require.False(t, chunk.Zero)
	require.Equal(t, newTestChunkData(0), chunk.Data)

	chunk.Index = 1
	chunk.StoredInS3 = true
	chunk.Compression = "lz4"
	err = source.Read(ctx, chunk)
	require.NoError(t, err)
	require.Equal(
		t,
		dataplane_common.Chunk{Index: 1, Data: chunk.Data, Zero: true},
		*chunk,
	)

	chunk.Index = 2
	err = source.Read(ctx, chunk)
	require.NoError(t, err)
	require.Equal(t, "chunk2", chunk.ID)
	require.False(t, chunk.Zero)
	require.Equal(t, newTestChunkData(2), chunk.Data)

	// Zero chunk needs no object.
	require.Equal(
		t,
		[]string{ChunkKey("chunk0"), ChunkKey("chunk2")},
		reader.requestedKeys,
	)
}

// Backup tasks do not set compression of a chunk that is not compressed, but
// an empty one means the same.
func TestBackupSourceReadChunkWithEmptyCompression(t *testing.T) {
	ctx := newTestContext()

	data := newTestChunkData(0)
	checksum := fmt.Sprint(dataplane_common.Chunk{Data: data}.Checksum())
	compression := ""

	reader := newFakeObjectReader()
	reader.objects[ChunkKey("chunk0")] = persistence.S3Object{
		Data: data,
		Metadata: map[string]*string{
			"Checksum":    &checksum,
			"Compression": &compression,
		},
	}

	source := NewBackupSource(reader, []string{"chunk0"}, 0, newTestMetrics())
	defer source.Close(ctx)

	chunk := newTestChunk(0)
	err := source.Read(ctx, chunk)
	require.NoError(t, err)
	require.Equal(t, data, chunk.Data)
}

func TestBackupSourceReadFailsOnMissingChunk(t *testing.T) {
	ctx := newTestContext()

	reader := newFakeObjectReader()
	source := NewBackupSource(
		reader,
		[]string{"", "chunk1"},
		0, // storageSize
		newTestMetrics(),
	)
	defer source.Close(ctx)

	// Reader reports it as a silent error.
	_, err := reader.GetObject(ctx, ChunkKey("chunk1"))
	require.True(t, errors.IsSilent(err))

	err = source.Read(ctx, newTestChunk(1))
	requireNonSilentChunkError(t, err, 1, "chunk1")
}

func TestBackupSourceReadFailsOnBadChunk(t *testing.T) {
	ctx := newTestContext()

	data := newTestChunkData(0)
	checksum := fmt.Sprint(dataplane_common.Chunk{Data: data}.Checksum())
	otherChecksum := fmt.Sprint(
		dataplane_common.Chunk{Data: newTestChunkData(1)}.Checksum(),
	)
	invalidChecksum := "abc"
	lz4 := "lz4"
	unknownCompression := "unknown"

	testCases := []struct {
		name     string
		metadata map[string]*string
	}{
		{
			name:     "no checksum",
			metadata: map[string]*string{},
		},
		{
			name:     "invalid checksum",
			metadata: map[string]*string{"Checksum": &invalidChecksum},
		},
		{
			name:     "checksum mismatch",
			metadata: map[string]*string{"Checksum": &otherChecksum},
		},
		{
			name: "unknown compression",
			metadata: map[string]*string{
				"Checksum":    &checksum,
				"Compression": &unknownCompression,
			},
		},
		{
			name: "data is not compressed",
			metadata: map[string]*string{
				"Checksum":    &checksum,
				"Compression": &lz4,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			reader := newFakeObjectReader()
			reader.objects[ChunkKey("chunk0")] = persistence.S3Object{
				Data:     data,
				Metadata: testCase.metadata,
			}

			source := NewBackupSource(
				reader,
				[]string{"chunk0"},
				0, // storageSize
				newTestMetrics(),
			)
			defer source.Close(ctx)

			err := source.Read(ctx, newTestChunk(0))
			requireNonSilentChunkError(t, err, 0, "chunk0")
		})
	}
}

func TestBackupSourceReadPassesRetriableErrorThrough(t *testing.T) {
	ctx := newTestContext()

	retriableErr := errors.NewRetriableErrorf("s3 is not available")

	reader := newFakeObjectReader()
	reader.errors[ChunkKey("chunk0")] = retriableErr

	source := NewBackupSource(reader, []string{"chunk0"}, 0, newTestMetrics())
	defer source.Close(ctx)

	err := source.Read(ctx, newTestChunk(0))
	require.Same(t, retriableErr, err)
}

func TestBackupSourceTransfer(t *testing.T) {
	ctx := newTestContext()

	chunkCount := 20
	reader := newFakeObjectReader()
	chunkIDs, diskData := reader.putBackup(t, chunkCount)

	disk := newFakeDisk(chunkCount)
	source := NewBackupSource(reader, chunkIDs, 0, newTestMetrics())
	defer source.Close(ctx)

	// Chunks are written concurrently, in any order. A chunk that is not
	// written yet should never be before the milestone.
	disk.beforeWrite = func(ctx context.Context, chunkIndex uint32) error {
		milestone := source.Milestone()
		if milestone.ChunkIndex > chunkIndex {
			return errors.NewNonRetriableErrorf(
				"milestone %+v skips chunk %v",
				milestone,
				chunkIndex,
			)
		}

		return nil
	}

	transferredChunkCount, err := newTestTransferer().Transfer(
		ctx,
		source,
		disk,
		dataplane_common.Milestone{},
		func(context.Context, dataplane_common.Milestone) error {
			return nil
		},
	)
	require.NoError(t, err)
	require.EqualValues(t, chunkCount, transferredChunkCount)

	// Zero chunks are written too, they should overwrite the garbage.
	require.Len(t, disk.writtenChunkIndices, chunkCount)
	require.True(t, bytes.Equal(diskData, disk.data))
}

func TestBackupSourceTransferOfZeroChunksOnly(t *testing.T) {
	ctx := newTestContext()

	chunkCount := 7
	reader := newFakeObjectReader()

	disk := newFakeDisk(chunkCount)
	source := NewBackupSource(
		reader,
		make([]string, chunkCount),
		0, // storageSize
		newTestMetrics(),
	)
	defer source.Close(ctx)

	transferredChunkCount, err := newTestTransferer().Transfer(
		ctx,
		source,
		disk,
		dataplane_common.Milestone{},
		func(context.Context, dataplane_common.Milestone) error {
			return nil
		},
	)
	require.NoError(t, err)
	require.EqualValues(t, chunkCount, transferredChunkCount)

	require.Empty(t, reader.requestedKeys)
	require.True(
		t,
		bytes.Equal(make([]byte, chunkCount*testChunkSize), disk.data),
	)
}

func TestBackupSourceTransferFromMilestone(t *testing.T) {
	ctx := newTestContext()

	chunkCount := 20
	milestone := dataplane_common.Milestone{
		ChunkIndex:            8,
		TransferredChunkCount: 8,
	}

	reader := newFakeObjectReader()
	chunkIDs, diskData := reader.putBackup(t, chunkCount)

	disk := newFakeDisk(chunkCount)
	source := NewBackupSource(reader, chunkIDs, 0, newTestMetrics())
	defer source.Close(ctx)

	var savedMilestones []dataplane_common.Milestone

	transferredChunkCount, err := newTestTransferer().Transfer(
		ctx,
		source,
		disk,
		milestone,
		func(
			ctx context.Context,
			savedMilestone dataplane_common.Milestone,
		) error {

			savedMilestones = append(savedMilestones, savedMilestone)
			return nil
		},
	)
	require.NoError(t, err)
	require.EqualValues(t, chunkCount, transferredChunkCount)

	// The source has every chunk, so all the chunks before a milestone are
	// transferred, the ones before the initial milestone included.
	require.NotEmpty(t, savedMilestones)
	for _, savedMilestone := range savedMilestones {
		require.GreaterOrEqual(
			t,
			savedMilestone.ChunkIndex,
			milestone.ChunkIndex,
		)
		require.Equal(
			t,
			savedMilestone.ChunkIndex,
			savedMilestone.TransferredChunkCount,
		)
	}

	// Chunks before the milestone should stay untouched.
	offset := int(milestone.ChunkIndex) * testChunkSize
	require.Len(
		t,
		disk.writtenChunkIndices,
		chunkCount-int(milestone.ChunkIndex),
	)
	require.True(
		t,
		bytes.Equal(bytes.Repeat([]byte{0xFF}, offset), disk.data[:offset]),
	)
	require.True(t, bytes.Equal(diskData[offset:], disk.data[offset:]))
}

// Chunks are written out of order, so chunks that follow the failed one may be
// written and acknowledged already. The milestone should not go past the
// failed chunk, otherwise the next attempt skips it.
func TestBackupSourceMilestoneDoesNotSkipFailedChunk(t *testing.T) {
	ctx, cancel := context.WithTimeout(newTestContext(), time.Minute)
	defer cancel()

	chunkCount := 20
	transferer := newTestTransferer()
	failedChunkIndex := uint32(6)
	// While the failed chunk is inflight, the source gives out this chunk only
	// after one of the chunks between them is acknowledged.
	lateChunkIndex := failedChunkIndex + transferer.ChunksInflightLimit

	reader := newFakeObjectReader()
	chunkIDs, diskData := reader.putBackup(t, chunkCount)

	disk := newFakeDisk(chunkCount)
	source := NewBackupSource(reader, chunkIDs, 0, newTestMetrics())
	writeErr := errors.NewRetriableErrorf("disk is not available")

	var milestoneAtFailure dataplane_common.Milestone

	disk.beforeWrite = func(ctx context.Context, chunkIndex uint32) error {
		if chunkIndex != failedChunkIndex {
			return nil
		}

		// Fail when everything around the chunk is written and acknowledged.
		for {
			milestone := source.Milestone()
			if milestone.ChunkIndex >= failedChunkIndex &&
				disk.isWritten(lateChunkIndex) {

				milestoneAtFailure = milestone
				return writeErr
			}

			select {
			case <-time.After(time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	_, err := transferer.Transfer(
		ctx,
		source,
		disk,
		dataplane_common.Milestone{},
		func(context.Context, dataplane_common.Milestone) error {
			return nil
		},
	)
	require.Same(t, writeErr, err)

	expectedMilestone := dataplane_common.Milestone{
		ChunkIndex:            failedChunkIndex,
		TransferredChunkCount: failedChunkIndex,
	}
	require.Equal(t, expectedMilestone, milestoneAtFailure)
	require.Equal(t, expectedMilestone, source.Milestone())
	source.Close(ctx)

	require.False(t, disk.isWritten(failedChunkIndex))

	// The next attempt starts from the milestone and rewrites the chunks
	// that were written after it.
	disk.beforeWrite = nil
	source = NewBackupSource(reader, chunkIDs, 0, newTestMetrics())
	defer source.Close(ctx)

	transferredChunkCount, err := transferer.Transfer(
		ctx,
		source,
		disk,
		expectedMilestone,
		func(context.Context, dataplane_common.Milestone) error {
			return nil
		},
	)
	require.NoError(t, err)
	require.EqualValues(t, chunkCount, transferredChunkCount)
	require.True(t, bytes.Equal(diskData, disk.data))
}
