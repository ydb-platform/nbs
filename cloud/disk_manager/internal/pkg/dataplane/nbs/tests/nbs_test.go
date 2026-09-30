package tests

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs_sdk "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/config"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

const (
	blockSize     = uint32(4096)
	blocksInChunk = uint32(8)
	chunkSize     = blocksInChunk * blockSize
	chunkCount    = uint32(33)
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

func newFactory(t *testing.T, ctx context.Context) nbs_client.Factory {
	rootCertsFile := os.Getenv("DISK_MANAGER_RECIPE_ROOT_CERTS_FILE")

	factory, err := nbs_client.NewFactory(
		ctx,
		&config.ClientConfig{
			Zones: map[string]*config.Zone{
				"zone": {
					Endpoints: []string{
						fmt.Sprintf(
							"localhost:%v",
							os.Getenv("DISK_MANAGER_RECIPE_NBS_PORT"),
						),
						fmt.Sprintf(
							"localhost:%v",
							os.Getenv("DISK_MANAGER_RECIPE_NBS_PORT"),
						),
					},
				},
			},
			RootCertsFile: &rootCertsFile,
		},
		metrics.NewEmptyRegistry(),
		metrics.NewEmptyRegistry(),
		nil, // tlsProvider
	)
	require.NoError(t, err)

	return factory
}

////////////////////////////////////////////////////////////////////////////////

func checkChunks(
	t *testing.T,
	ctx context.Context,
	source dataplane_common.Source,
	expectedChunks []dataplane_common.Chunk,
) {

	for _, expected := range expectedChunks {
		actual := dataplane_common.Chunk{
			Index: expected.Index,
			Data:  make([]byte, chunkSize),
		}
		err := source.Read(ctx, &actual)
		require.NoError(t, err)

		require.Equal(t, expected.Index, actual.Index)
		require.Equal(t, expected.Zero, actual.Zero)
		if !expected.Zero {
			require.Equal(t, expected.Data, actual.Data)
		}
	}
}

func TestChunkIndices(t *testing.T) {
	ctx := newContext()

	factory := newFactory(t, ctx)
	client, err := factory.GetClient(ctx, "zone")
	require.NoError(t, err)

	diskID := t.Name()

	blockCount := uint64(blocksInChunk * chunkCount)
	err = client.Create(ctx, nbs_client.CreateDiskParams{
		ID:          diskID,
		BlocksCount: blockCount,
		BlockSize:   blockSize,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)

	session, err := client.MountRW(
		ctx,
		diskID,
		0,   // fillGeneration
		0,   // fillSeqNumber
		nil, // encryption
	)
	require.NoError(t, err)
	defer session.Close(ctx)

	var totalChunkIndices []uint32

	rand.Seed(time.Now().UnixNano())
	for blockIndex := uint64(0); blockIndex < blockCount; blockIndex++ {
		changed := false
		dice := rand.Intn(3)

		var err error
		switch dice {
		case 0:
			changed = true
			bytes := make([]byte, blockSize)
			err = session.Write(ctx, blockIndex, bytes)
		case 1:
			changed = true
			err = session.Zero(ctx, blockIndex, 1)
		}
		require.NoError(t, err)

		if changed {
			chunkIndex := uint32(blockIndex) / blocksInChunk

			size := len(totalChunkIndices)
			if size == 0 || totalChunkIndices[size-1] != chunkIndex {
				totalChunkIndices = append(totalChunkIndices, chunkIndex)
			}
		}
	}

	err = client.CreateCheckpoint(
		ctx,
		nbs_client.CheckpointParams{
			DiskID:       diskID,
			CheckpointID: "checkpoint",
		},
	)
	require.NoError(t, err)

	milestoneChunkIndex := uint32(0)
	for milestoneChunkIndex < uint32(len(totalChunkIndices)) {
		logging.Info(
			ctx,
			"doing iteration with milestoneChunkIndex=%v",
			milestoneChunkIndex,
		)

		processedChunkIndices := make(chan uint32, 1)
		var actualChunkIndices []uint32

		source, err := nbs.NewDiskSource(
			ctx,
			client,
			diskID,
			"",
			"",
			"checkpoint",
			nil, // encryption
			chunkSize,
			false, // duplicateChunkIndices
			false, // ignoreBaseDisk
			false, // dontReadFromCheckpoint
		)
		require.NoError(t, err)
		defer source.Close(ctx)

		chunkIndices, _, errors := source.ChunkIndices(
			ctx,
			dataplane_common.Milestone{ChunkIndex: milestoneChunkIndex},
			processedChunkIndices,
			common.ChannelWithCancellation{}, // holeChunkIndices
		)

		for chunkIndex := range chunkIndices {
			processedChunkIndices <- chunkIndex
			actualChunkIndices = append(actualChunkIndices, chunkIndex)
		}

		position := 0
		for i, chunkIndex := range totalChunkIndices {
			if chunkIndex >= milestoneChunkIndex {
				position = i
				break
			}
		}
		expectedChunkIndices := totalChunkIndices[position:]

		for err := range errors {
			require.NoError(t, err)
		}
		require.Equal(t, expectedChunkIndices, actualChunkIndices)

		milestoneChunkIndex++
	}
}

func TestReadWrite(t *testing.T) {
	ctx := newContext()

	factory := newFactory(t, ctx)
	client, err := factory.GetClient(ctx, "zone")
	require.NoError(t, err)

	diskID := t.Name()
	disk := &types.Disk{ZoneId: "zone", DiskId: diskID}

	err = client.Create(ctx, nbs_client.CreateDiskParams{
		ID:          diskID,
		BlocksCount: uint64(blocksInChunk * chunkCount),
		BlockSize:   blockSize,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)

	target, err := nbs.NewDiskTarget(
		ctx,
		factory,
		disk,
		nil,
		chunkSize,
		false,
		0, // fillGeneration
		0, // fillSeqNumber
	)
	require.NoError(t, err)
	defer target.Close(ctx)

	chunks := make([]dataplane_common.Chunk, 0)
	for i := uint32(0); i < chunkCount; i++ {
		var chunk dataplane_common.Chunk

		if rand.Intn(2) == 1 {
			data := make([]byte, chunkSize)
			rand.Read(data)
			chunk = dataplane_common.Chunk{Index: i, Data: data}
		} else {
			// Zero chunk.
			chunk = dataplane_common.Chunk{Index: i, Zero: true}
		}

		err = target.Write(ctx, chunk)
		require.NoError(t, err)

		chunks = append(chunks, chunk)
	}

	err = client.CreateCheckpoint(
		ctx,
		nbs_client.CheckpointParams{
			DiskID:       diskID,
			CheckpointID: "checkpoint",
		},
	)
	require.NoError(t, err)

	source, err := nbs.NewDiskSource(
		ctx,
		client,
		diskID,
		"",
		"",
		"checkpoint",
		nil, // encryption
		chunkSize,
		false, // duplicateChunkIndices
		false, // ignoreBaseDisk
		false, // dontReadFromCheckpoint
	)
	require.NoError(t, err)
	defer source.Close(ctx)

	checkChunks(t, ctx, source, chunks)

	sourceChunkCount, err := source.ChunkCount(ctx)
	require.NoError(t, err)
	require.Equal(t, chunkCount, sourceChunkCount)
}

func TestDontReadFromCheckpoint(t *testing.T) {
	ctx := newContext()

	factory := newFactory(t, ctx)
	client, err := factory.GetClient(ctx, "zone")
	require.NoError(t, err)

	diskID := t.Name()
	disk := &types.Disk{ZoneId: "zone", DiskId: diskID}

	err = client.Create(ctx, nbs_client.CreateDiskParams{
		ID:          diskID,
		BlocksCount: uint64(blocksInChunk * chunkCount),
		BlockSize:   blockSize,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)

	target, err := nbs.NewDiskTarget(
		ctx,
		factory,
		disk,
		nil,
		chunkSize,
		false,
		0, // fillGeneration
		0, // fillSeqNumber
	)
	require.NoError(t, err)
	defer target.Close(ctx)

	baseChunks := test.FillTarget(t, ctx, target, chunkCount, chunkSize)

	err = client.CreateCheckpoint(
		ctx,
		nbs_client.CheckpointParams{
			DiskID:       diskID,
			CheckpointID: "checkpoint",
		},
	)
	require.NoError(t, err)

	newChunks := test.FillTarget(t, ctx, target, chunkCount, chunkSize)

	baseIndex2Chunk := make(map[uint32]dataplane_common.Chunk)
	for _, chunk := range baseChunks {
		baseIndex2Chunk[chunk.Index] = chunk
	}

	newIndex2Chunk := make(map[uint32]dataplane_common.Chunk)
	for _, chunk := range newChunks {
		newIndex2Chunk[chunk.Index] = chunk
	}

	updatedBaseChunks := []dataplane_common.Chunk{}
	for i, baseChunk := range baseIndex2Chunk {
		if newChunk, ok := newIndex2Chunk[i]; ok {
			updatedBaseChunks = append(updatedBaseChunks, newChunk)
		} else {
			updatedBaseChunks = append(updatedBaseChunks, baseChunk)
		}
	}

	for _, dontReadFromCheckpoint := range []bool{false, true} {
		func() {
			source, err := nbs.NewDiskSource(
				ctx,
				client,
				diskID,
				"",
				"",
				"checkpoint",
				nil, // encryption
				chunkSize,
				false, // duplicateChunkIndices
				false, // ignoreBaseDisk
				dontReadFromCheckpoint,
			)
			require.NoError(t, err)
			defer source.Close(ctx)

			expectedChunks := baseChunks
			if dontReadFromCheckpoint {
				expectedChunks = updatedBaseChunks
			}
			checkChunks(t, ctx, source, expectedChunks)

			sourceChunkCount, err := source.ChunkCount(ctx)
			require.NoError(t, err)
			require.Equal(t, chunkCount, sourceChunkCount)
		}()
	}
}

////////////////////////////////////////////////////////////////////////////////

type diskTestParams struct {
	blockSize     uint32
	blocksInChunk uint32
	blocksCount   uint64
	chunksCount   uint32
}

// blockRange describes blocks in [start, end).
type blockRange struct {
	start uint64
	end   uint64
	zero  bool
}

func (r blockRange) data(blockSize uint32) []byte {
	data := make([]byte, (r.end-r.start)*uint64(blockSize))
	if !r.zero {
		for block := r.start; block < r.end; block++ {
			value := byte(1 + block%255)
			copy(data[(block-r.start)*uint64(blockSize):],
				bytes.Repeat([]byte{value}, int(blockSize)))
		}
	}
	return data
}

func (p diskTestParams) chunkSize() uint32 {
	return p.blockSize * p.blocksInChunk
}

func forEachDiskParams(
	t *testing.T,
	run func(*testing.T, diskTestParams, []blockRange),
) {

	t.Helper()

	for _, blockSize := range []uint32{4096, 16384, 131072} {
		for _, testCase := range []struct {
			params diskTestParams
			ranges []blockRange
		}{
			{
				params: diskTestParams{blockSize, 8, 1, 1},
				ranges: []blockRange{
					{start: 0, end: 1, zero: false},
				},
			},
			{
				params: diskTestParams{blockSize, 16, 8, 1},
				ranges: []blockRange{
					{start: 0, end: 4, zero: true},
					{start: 4, end: 8, zero: false},
				},
			},
			{
				params: diskTestParams{blockSize, 8, 24, 3},
				ranges: []blockRange{
					{start: 0, end: 5, zero: false},
					{start: 20, end: 23, zero: true},
					{start: 23, end: 24, zero: false},
				},
			},
			{
				params: diskTestParams{blockSize, 8, 25, 4},
				ranges: []blockRange{
					{start: 4, end: 8, zero: false},
					{start: 16, end: 24, zero: true},
					{start: 24, end: 25, zero: false},
				},
			},
			{
				params: diskTestParams{blockSize, 16, 40, 3},
				ranges: []blockRange{
					{start: 8, end: 16, zero: false},
					{start: 32, end: 39, zero: true},
					{start: 39, end: 40, zero: false},
				},
			},
			{
				params: diskTestParams{blockSize, 32, 83, 3},
				ranges: []blockRange{
					{start: 16, end: 32, zero: false},
					{start: 64, end: 82, zero: true},
					{start: 82, end: 83, zero: false},
				},
			},
		} {
			params := testCase.params
			name := fmt.Sprintf(
				"blockSize_%v_blocksInChunk_%v_blocksCount_%v_chunksCount_%v",
				params.blockSize,
				params.blocksInChunk,
				params.blocksCount,
				params.chunksCount,
			)
			t.Run(name, func(t *testing.T) {
				for _, r := range testCase.ranges {
					require.Less(t, r.start, r.end)
					require.LessOrEqual(t, r.end, params.blocksCount)
				}
				run(t, params, testCase.ranges)
			})
		}
	}
}

func createDisk(
	t *testing.T,
	ctx context.Context,
	client nbs_client.Client,
	params diskTestParams,
) string {

	t.Helper()

	diskID := strings.ReplaceAll(t.Name(), "/", "-")
	err := client.Create(ctx, nbs_client.CreateDiskParams{
		ID:          diskID,
		BlocksCount: params.blocksCount,
		BlockSize:   params.blockSize,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)
	return diskID
}

func newSource(
	t *testing.T,
	ctx context.Context,
	client nbs_client.Client,
	diskID string,
	params diskTestParams,
	baseCheckpointID string,
	checkpointID string,
) nbs.DiskSource {

	t.Helper()

	source, err := nbs.NewDiskSource(
		ctx,
		client,
		diskID,
		"", // proxyOverlayDiskID
		baseCheckpointID,
		checkpointID,
		nil, // encryption
		params.chunkSize(),
		false, // duplicateChunkIndices
		false, // ignoreBaseDisk
		false, // dontReadFromCheckpoint
	)
	require.NoError(t, err)
	t.Cleanup(func() { source.Close(ctx) })

	require.Equal(t, params.blocksCount*uint64(params.blockSize), source.Size())
	chunksCount, err := source.ChunkCount(ctx)
	require.NoError(t, err)
	require.Equal(t, params.chunksCount, chunksCount)
	return source
}

func checkDisk(
	t *testing.T,
	ctx context.Context,
	client nbs_client.Client,
	diskID string,
	params diskTestParams,
	expected []byte,
) {

	t.Helper()

	session, err := client.MountRO(ctx, diskID, nil)
	require.NoError(t, err)
	defer session.Close(ctx)

	actual := make([]byte, len(expected))
	for start := uint64(0); start < params.blocksCount; start += uint64(params.blocksInChunk) {
		blocksCount := min(uint64(params.blocksInChunk), params.blocksCount-start)
		var zero bool
		err = session.Read(
			ctx,
			start,
			uint32(blocksCount),
			"", // checkpointID
			actual[start*uint64(params.blockSize):(start+blocksCount)*uint64(params.blockSize)],
			&zero,
		)
		require.NoError(t, err)
	}

	require.True(t, bytes.Equal(expected, actual), "disk contents differ")
}

func checkChunksWithPadding(
	t *testing.T,
	ctx context.Context,
	source nbs.DiskSource,
	chunkSize uint32,
	expectedChunks []dataplane_common.Chunk,
) {

	t.Helper()

	actual := dataplane_common.Chunk{
		Data: bytes.Repeat([]byte{0xff}, int(chunkSize)),
		Zero: true,
	}
	for _, expected := range expectedChunks {
		actual.Index = expected.Index
		err := source.Read(ctx, &actual)
		require.NoError(t, err)

		require.Equal(t, expected.Index, actual.Index)
		require.Equal(t, expected.Zero, actual.Zero)
		if !expected.Zero {
			require.True(t, bytes.Equal(expected.Data, actual.Data),
				"chunk %v contents differ", expected.Index)
		}

		dataSize := min(uint64(chunkSize), source.Size()-uint64(expected.Index)*uint64(chunkSize))
		require.True(t, bytes.Equal(make([]byte, uint64(chunkSize)-dataSize), actual.Data[dataSize:]),
			"chunk %v padding is not zero", expected.Index)
	}
}

func checkChunkIndices(
	t *testing.T,
	ctx context.Context,
	client nbs_client.Client,
	diskID string,
	params diskTestParams,
	baseCheckpointID string,
	checkpointID string,
	expectedIndices []uint32,
) {

	t.Helper()

	for milestone := uint32(0); milestone <= params.chunksCount+1; milestone++ {
		t.Run(fmt.Sprintf("%v/milestone_%v", checkpointID, milestone), func(t *testing.T) {
			source := newSource(t, ctx, client, diskID, params, baseCheckpointID, checkpointID)
			processed := make(chan uint32, 1)
			indices, _, errors := source.ChunkIndices(
				ctx,
				dataplane_common.Milestone{ChunkIndex: milestone},
				processed,
				common.ChannelWithCancellation{}, // holeChunkIndices
			)
			actual := []uint32{}
			for index := range indices {
				actual = append(actual, index)
				processed <- index
			}

			close(processed)

			for err := range errors {
				require.NoError(t, err)
			}

			expected := []uint32{}
			for _, index := range expectedIndices {
				if index >= milestone {
					expected = append(expected, index)
				}
			}

			require.Equal(t, expected, actual)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestChunkIndicesWithDiskSizes(t *testing.T) {
	ctx := newContext()
	factory := newFactory(t, ctx)
	client, err := factory.GetClient(ctx, "zone")
	require.NoError(t, err)

	forEachDiskParams(t, func(t *testing.T, params diskTestParams, ranges []blockRange) {
		diskID := createDisk(t, ctx, client, params)
		session, err := client.MountRW(ctx, diskID, 0, 0, nil)
		require.NoError(t, err)
		defer session.Close(ctx)

		changedChunks := make([]bool, params.chunksCount)
		for _, r := range ranges {
			if r.zero {
				require.NoError(t, session.Zero(ctx, r.start, uint32(r.end-r.start)))
			} else {
				require.NoError(t, session.Write(ctx, r.start, r.data(params.blockSize)))
			}

			for block := r.start; block < r.end; block++ {
				changedChunks[block/uint64(params.blocksInChunk)] = true
			}
		}

		changedIndices := []uint32{}
		for index, changed := range changedChunks {
			if changed {
				changedIndices = append(changedIndices, uint32(index))
			}
		}

		require.NoError(t, client.CreateCheckpoint(ctx, nbs_client.CheckpointParams{
			DiskID: diskID, CheckpointID: "base",
		}))
		checkChunkIndices(t, ctx, client, diskID, params, "", "base", changedIndices)

		require.NoError(t, client.CreateCheckpoint(ctx, nbs_client.CheckpointParams{
			DiskID: diskID, CheckpointID: "unchanged",
		}))
		checkChunkIndices(t, ctx, client, diskID, params, "base", "unchanged", nil)

		require.NoError(t, session.Zero(ctx, params.blocksCount-1, 1))
		require.NoError(t, client.CreateCheckpoint(ctx, nbs_client.CheckpointParams{
			DiskID: diskID, CheckpointID: "incremental",
		}))
		checkChunkIndices(t, ctx, client, diskID, params, "unchanged", "incremental",
			[]uint32{params.chunksCount - 1})
	})
}

func TestReadWriteWithDiskSizes(t *testing.T) {
	ctx := newContext()
	factory := newFactory(t, ctx)
	client, err := factory.GetClient(ctx, "zone")
	require.NoError(t, err)

	forEachDiskParams(t, func(t *testing.T, params diskTestParams, ranges []blockRange) {
		diskID := createDisk(t, ctx, client, params)
		disk := &types.Disk{ZoneId: "zone", DiskId: diskID}
		target, err := nbs.NewDiskTarget(
			ctx,
			factory,
			disk,
			nil, // encryption
			params.chunkSize(),
			false, // ignoreZeroChunks
			0,     // fillGeneration
			0,     // fillSeqNumber
		)
		require.NoError(t, err)
		defer target.Close(ctx)
		require.Equal(t, params.blocksCount*uint64(params.blockSize), target.Size())

		expectedDisk := make([]byte, target.Size())
		for _, r := range ranges {
			copy(expectedDisk[r.start*uint64(params.blockSize):], r.data(params.blockSize))
		}
		chunks := make([]dataplane_common.Chunk, 0, params.chunksCount)
		for index := uint32(0); index < params.chunksCount; index++ {
			start := uint64(index) * uint64(params.chunkSize())
			end := min(start+uint64(params.chunkSize()), target.Size())
			zero := bytes.Equal(expectedDisk[start:end], make([]byte, end-start))
			// Nonzero padding checks that the target writes only valid disk blocks.
			data := bytes.Repeat([]byte{0xff}, int(params.chunkSize()))
			dataSize := copy(data, expectedDisk[start:end])
			chunk := dataplane_common.Chunk{Index: index, Data: data, Zero: zero}
			require.NoError(t, target.Write(ctx, chunk))
			clear(data[dataSize:])
			chunks = append(chunks, chunk)
		}
		checkDisk(t, ctx, client, diskID, params, expectedDisk)

		lastChunk := params.chunksCount - 1
		// A target needs only the on-disk bytes of a partial chunk.
		for _, index := range []uint32{0, lastChunk} {
			start := uint64(index) * uint64(params.chunkSize())
			dataSize := min(uint64(params.chunkSize()), target.Size()-start)
			if !chunks[index].Zero {
				require.NoError(t, target.Write(ctx, dataplane_common.Chunk{
					Index: index, Data: expectedDisk[start : start+dataSize],
				}))
			}
		}
		checkDisk(t, ctx, client, diskID, params, expectedDisk)

		require.NoError(t, client.CreateCheckpoint(ctx, nbs_client.CheckpointParams{
			DiskID: diskID, CheckpointID: "written",
		}))
		source := newSource(t, ctx, client, diskID, params, "", "written")
		checkChunksWithPadding(t, ctx, source, params.chunkSize(), chunks)

		target.Close(ctx)
		for _, ignoreZeroChunks := range []bool{true, false} {
			t.Run(fmt.Sprintf("ignoreZeroChunks_%v", ignoreZeroChunks), func(t *testing.T) {
				target, err := nbs.NewDiskTarget(
					ctx, factory, disk, nil, params.chunkSize(), ignoreZeroChunks, 0, 0,
				)
				require.NoError(t, err)
				defer target.Close(ctx)
				require.NoError(t, target.Write(ctx, dataplane_common.Chunk{Index: lastChunk, Zero: true}))
				if !ignoreZeroChunks {
					clear(expectedDisk[uint64(lastChunk)*uint64(params.chunkSize()):])
				}
				checkDisk(t, ctx, client, diskID, params, expectedDisk)
			})
		}
		require.NoError(t, client.CreateCheckpoint(ctx, nbs_client.CheckpointParams{
			DiskID: diskID, CheckpointID: "zeroed",
		}))

		zeroedSource := newSource(t, ctx, client, diskID, params, "", "zeroed")
		lastWrittenChunk := chunks[lastChunk]
		chunks[lastChunk] = dataplane_common.Chunk{Index: lastChunk, Zero: true}
		checkChunksWithPadding(t, ctx, zeroedSource, params.chunkSize(), chunks)
		checkChunksWithPadding(t, ctx, source, params.chunkSize(), []dataplane_common.Chunk{lastWrittenChunk})
	})
}

////////////////////////////////////////////////////////////////////////////////

func newMockSession(
	t *testing.T,
	ctx context.Context,
	blockSize uint32,
	blockCount uint64,
) *nbs_mocks.SessionMock {

	t.Helper()
	session := nbs_mocks.NewSessionMock()
	session.On("BlockSize").Return(blockSize).Maybe()
	session.On("BlockCount").Return(blockCount).Maybe()
	session.On("Close", ctx).Once()
	t.Cleanup(func() { session.AssertExpectations(t) })
	return session
}

func TestDiskReadWriteValidation(t *testing.T) {
	ctx := newContext()
	forEachDiskParams(t, func(t *testing.T, params diskTestParams, _ []blockRange) {
		// No I/O is expected for invalid chunks; any read/write session call fails the test.
		session := newMockSession(t, ctx, params.blockSize, params.blocksCount)
		client := nbs_mocks.NewClientMock()
		client.On("MountRO", ctx, "disk", (*types.EncryptionDesc)(nil)).Return(session, nil).Once()
		t.Cleanup(func() { client.AssertExpectations(t) })
		source := newSource(t, ctx, client, "disk", params, "", "")

		newTarget := func(ignoreZeroChunks bool) nbs.DiskTarget {
			session := newMockSession(t, ctx, params.blockSize, params.blocksCount)
			client := nbs_mocks.NewClientMock()
			client.On("MountRW", ctx, "disk", uint64(0), uint64(0), (*types.EncryptionDesc)(nil)).
				Return(session, nil).Once()
			factory := nbs_mocks.NewFactoryMock()
			factory.On("GetClient", ctx, "zone").Return(client, nil).Once()
			t.Cleanup(func() {
				client.AssertExpectations(t)
				factory.AssertExpectations(t)
			})
			target, err := nbs.NewDiskTarget(
				ctx, factory, &types.Disk{DiskId: "disk", ZoneId: "zone"},
				nil, params.chunkSize(), ignoreZeroChunks, 0, 0,
			)
			require.NoError(t, err)
			t.Cleanup(func() { target.Close(ctx) })
			return target
		}
		target := newTarget(false)
		for _, index := range []uint32{params.chunksCount, math.MaxUint32} {
			for _, zero := range []bool{false, true} {
				chunk := dataplane_common.Chunk{
					Index: index, Data: make([]byte, params.chunkSize()), Zero: zero,
				}
				err := target.Write(ctx, chunk)
				require.ErrorContains(t, err, "starts beyond the disk")
				require.ErrorIs(t, err, task_errors.NewEmptyNonRetriableError())
				err = source.Read(ctx, &chunk)
				require.ErrorContains(t, err, "starts beyond the disk")
				require.ErrorIs(t, err, task_errors.NewEmptyNonRetriableError())
			}
		}
		for _, index := range []uint32{0, params.chunksCount - 1} {
			start := uint64(index) * uint64(params.chunkSize())
			dataSize := min(uint64(params.chunkSize()), target.Size()-start)
			for _, size := range []uint64{0, dataSize - 1} {
				err := target.Write(ctx, dataplane_common.Chunk{Index: index, Data: make([]byte, size)})
				require.ErrorContains(t, err, "buffer is too small")
				require.ErrorIs(t, err, task_errors.NewEmptyNonRetriableError())
			}
			for _, size := range []uint32{0, params.chunkSize() - 1} {
				chunk := dataplane_common.Chunk{
					Index: index, Data: bytes.Repeat([]byte{0xff}, int(size)), Zero: true,
				}
				err := source.Read(ctx, &chunk)
				require.ErrorContains(t, err, "buffer is too small")
				require.ErrorIs(t, err, task_errors.NewEmptyNonRetriableError())
				require.True(t, chunk.Zero)
				require.True(t, bytes.Equal(bytes.Repeat([]byte{0xff}, int(size)), chunk.Data),
					"invalid read modified the buffer")
			}
		}
		target = newTarget(true)
		require.NoError(t, target.Write(ctx, dataplane_common.Chunk{Index: params.chunksCount, Zero: true}))
		err := target.Write(ctx, dataplane_common.Chunk{Index: params.chunksCount})
		require.ErrorContains(t, err, "starts beyond the disk")
		require.ErrorIs(t, err, task_errors.NewEmptyNonRetriableError())
	})
}

func TestDiskSourceValidation(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		chunkSize  uint32
		blockSize  uint32
		blockCount uint64
		error      string
	}{
		{
			name:       "zero_chunk_size",
			blockSize:  blockSize,
			blockCount: 8,
			error:      "chunkSize should not be zero",
		},
		{
			name:       "zero_block_size",
			chunkSize:  chunkSize,
			blockCount: 8,
			error:      "blockSize should not be zero",
		},
		{
			name:       "unaligned_chunk_size",
			chunkSize:  chunkSize + 1,
			blockSize:  blockSize,
			blockCount: 8,
			error:      "chunkSize should be multiple of blockSize",
		},
		{
			name:       "blocks_in_chunk_not_multiple_of_eight",
			chunkSize:  7 * blockSize,
			blockSize:  blockSize,
			blockCount: 8,
			error:      "blocksInChunk should be multiple of 8",
		},
		{
			name:       "chunk_not_divisor_of_changed_blocks_iteration",
			chunkSize:  24 * blockSize,
			blockSize:  blockSize,
			blockCount: 8,
			error:      "maxChangedBlockCountPerIteration should be multiple of blocksInChunk",
		},
		{
			name:       "too_many_full_chunks",
			chunkSize:  chunkSize,
			blockSize:  blockSize,
			blockCount: (uint64(math.MaxUint32) + 1) * uint64(blocksInChunk),
			error:      "disk has too many chunks: chunkCount=4294967296",
		},
		{
			name:       "too_many_chunks_after_rounding_up",
			chunkSize:  chunkSize,
			blockSize:  blockSize,
			blockCount: uint64(math.MaxUint32)*uint64(blocksInChunk) + 1,
			error:      "disk has too many chunks: chunkCount=4294967296",
		},
		{
			name:       "maximum_block_count",
			chunkSize:  chunkSize,
			blockSize:  blockSize,
			blockCount: math.MaxUint64,
			error:      "disk has too many chunks: chunkCount=2305843009213693952",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(newContext(), 10*time.Second)
			defer cancel()
			session := newMockSession(t, ctx, testCase.blockSize, testCase.blockCount)
			client := nbs_mocks.NewClientMock()
			client.On("MountRO", ctx, "disk", (*types.EncryptionDesc)(nil)).
				Return(session, nil).Once()
			t.Cleanup(func() { client.AssertExpectations(t) })

			source, err := nbs.NewDiskSource(
				ctx, client, "disk", "", "", "", nil,
				testCase.chunkSize, false, false, false,
			)
			require.Nil(t, source)
			require.ErrorContains(t, err, testCase.error)
			require.ErrorIs(t, err, task_errors.NewEmptyNonRetriableError())
		})
	}
}

func TestDiskSourceMaximumChunkCount(t *testing.T) {
	for _, blockCount := range []uint64{
		uint64(math.MaxUint32) * uint64(blocksInChunk),
		uint64(math.MaxUint32-1)*uint64(blocksInChunk) + 1,
	} {
		t.Run(fmt.Sprintf("blocks_%v", blockCount), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(newContext(), 10*time.Second)
			defer cancel()
			session := newMockSession(t, ctx, blockSize, blockCount)
			client := nbs_mocks.NewClientMock()
			client.On("MountRO", ctx, "disk", (*types.EncryptionDesc)(nil)).
				Return(session, nil).Once()
			t.Cleanup(func() { client.AssertExpectations(t) })

			source, err := nbs.NewDiskSource(
				ctx, client, "disk", "", "", "", nil,
				chunkSize, false, false, false,
			)
			require.NoError(t, err)
			count, err := source.ChunkCount(ctx)
			require.NoError(t, err)
			require.Equal(t, uint32(math.MaxUint32), count)
			require.Equal(t, blockCount*uint64(blockSize), source.Size())
			source.Close(ctx)
		})
	}
}

func TestDiskTargetValidation(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		chunkSize uint32
		blockSize uint32
		error     string
	}{
		{"zero_chunk_size", 0, blockSize, "chunkSize should not be zero"},
		{"zero_block_size", chunkSize, 0, "blockSize should not be zero"},
		{"unaligned_chunk_size", chunkSize + 1, blockSize, "chunkSize should be multiple of blockSize"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(newContext(), 10*time.Second)
			defer cancel()
			session := newMockSession(t, ctx, testCase.blockSize, 8)
			client := nbs_mocks.NewClientMock()
			client.On("MountRW", ctx, "disk", uint64(0), uint64(0), (*types.EncryptionDesc)(nil)).
				Return(session, nil).Once()
			factory := nbs_mocks.NewFactoryMock()
			factory.On("GetClient", ctx, "zone").Return(client, nil).Once()
			t.Cleanup(func() {
				client.AssertExpectations(t)
				factory.AssertExpectations(t)
			})

			target, err := nbs.NewDiskTarget(
				ctx, factory, &types.Disk{DiskId: "disk", ZoneId: "zone"},
				nil, testCase.chunkSize, false, 0, 0,
			)
			require.Nil(t, target)
			require.ErrorContains(t, err, testCase.error)
			require.ErrorIs(t, err, task_errors.NewEmptyNonRetriableError())
		})
	}
}

func TestDiskSourceProxyMount(t *testing.T) {
	ctx := newContext()
	session := newMockSession(t, ctx, blockSize, 9)
	client := nbs_mocks.NewClientMock()
	client.On("MountLocalRO", ctx, "proxy", (*types.EncryptionDesc)(nil)).Return(session, nil).Once()
	t.Cleanup(func() { client.AssertExpectations(t) })

	source, err := nbs.NewDiskSource(
		ctx, client, "original", "proxy", "base", "checkpoint", nil,
		chunkSize, false, false, false,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(9)*uint64(blockSize), source.Size())
	count, err := source.ChunkCount(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(2), count)
	source.Close(ctx)
}

func TestDiskConstructorErrors(t *testing.T) {
	ctx := newContext()
	mountError := errors.New("mount failed")
	for _, proxyDiskID := range []string{"", "proxy"} {
		t.Run("source_proxy_"+proxyDiskID, func(t *testing.T) {
			client := nbs_mocks.NewClientMock()
			method, diskID := "MountRO", "disk"
			if proxyDiskID != "" {
				method, diskID = "MountLocalRO", proxyDiskID
			}
			client.On(method, ctx, diskID, (*types.EncryptionDesc)(nil)).Return(nil, mountError).Once()
			source, err := nbs.NewDiskSource(
				ctx, client, "disk", proxyDiskID, "", "", nil,
				chunkSize, false, false, false,
			)
			require.Nil(t, source)
			require.ErrorIs(t, err, mountError)
			client.AssertExpectations(t)
		})
	}

	t.Run("target_mount", func(t *testing.T) {
		client := nbs_mocks.NewClientMock()
		client.On("MountRW", ctx, "disk", uint64(2), uint64(3), (*types.EncryptionDesc)(nil)).
			Return(nil, mountError).Once()
		factory := nbs_mocks.NewFactoryMock()
		factory.On("GetClient", ctx, "zone").Return(client, nil).Once()
		target, err := nbs.NewDiskTarget(
			ctx, factory, &types.Disk{DiskId: "disk", ZoneId: "zone"},
			nil, chunkSize, false, 2, 3,
		)
		require.Nil(t, target)
		require.ErrorIs(t, err, mountError)
		client.AssertExpectations(t)
		factory.AssertExpectations(t)
	})

	t.Run("target_factory", func(t *testing.T) {
		factoryError := errors.New("client unavailable")
		factory := nbs_mocks.NewFactoryMock()
		factory.On("GetClient", ctx, "zone").Return(nil, factoryError).Once()
		target, err := nbs.NewDiskTarget(
			ctx, factory, &types.Disk{DiskId: "disk", ZoneId: "zone"},
			nil, chunkSize, false, 0, 0,
		)
		require.Nil(t, target)
		require.ErrorIs(t, err, factoryError)
		factory.AssertExpectations(t)
	})
}

////////////////////////////////////////////////////////////////////////////////

func sourceTestContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(newContext(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func receiveSourceIndex(t *testing.T, ctx context.Context, indices <-chan uint32) (uint32, bool) {
	t.Helper()
	select {
	case index, more := <-indices:
		return index, more
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for a chunk index")
		return 0, false
	}
}

func receiveSourceError(t *testing.T, ctx context.Context, errs <-chan error) error {
	t.Helper()
	select {
	case err, more := <-errs:
		if more {
			require.Error(t, err)
			select {
			case _, more = <-errs:
				require.False(t, more, "error channel must close after its first error")
			case <-ctx.Done():
				require.FailNow(t, "timed out waiting for the error channel to close")
			}
		}
		return err
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for chunk generation to finish")
		return ctx.Err()
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestSourceChangedMasksAndMilestone(t *testing.T) {
	const (
		blocksPerIteration = uint64(1 << 20)
		blocksPerChunk     = uint64(16)
		lastChunk          = uint32(blocksPerIteration/blocksPerChunk + 2)
		totalChunks        = lastChunk + 1
	)
	for _, testCase := range []struct {
		name     string
		lastByte byte
		indices  []uint32
	}{
		{name: "changed partial chunk", lastByte: 0xfb, indices: []uint32{1, lastChunk}},
		{name: "unused bits only", lastByte: 0xf8, indices: []uint32{1}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := sourceTestContext(t)
			session := newMockSession(t, ctx, blockSize, blocksPerIteration+43)
			client := nbs_mocks.NewClientMock()
			client.On("MountRO", ctx, "disk", (*types.EncryptionDesc)(nil)).
				Return(session, nil).Once()
			// Only the second mask byte of the first chunk is set. The
			// remaining unchanged chunks fill a complete 4 GiB query.
			firstMask := make([]byte, blocksPerIteration/8)
			firstMask[1] = 0x80
			client.On(
				"GetChangedBlocks", ctx, "disk", blocksPerChunk, uint32(blocksPerIteration),
				"base", "checkpoint", true,
			).Return(firstMask, nil).Once()
			client.On(
				"GetChangedBlocks", ctx, "disk", blocksPerIteration+blocksPerChunk, uint32(27),
				"base", "checkpoint", true,
			).Return([]byte{0, 0, 0, testCase.lastByte}, nil).Once()
			source, err := nbs.NewDiskSource(
				ctx, client, "disk", "", "base", "checkpoint", nil,
				uint32(blocksPerChunk)*blockSize, true, true, false,
			)
			require.NoError(t, err)
			defer source.Close(ctx)
			processed := make(chan uint32, 5)
			defer close(processed)
			initialMilestone := dataplane_common.Milestone{
				ChunkIndex:            1,
				TransferredChunkCount: 7,
			}
			indices, duplicated, errs := source.ChunkIndices(
				ctx, initialMilestone, processed, common.ChannelWithCancellation{},
			)
			require.NoError(t, receiveSourceError(t, ctx, errs))
			client.AssertExpectations(t)
			require.Equal(t, initialMilestone, source.Milestone())

			for _, expected := range testCase.indices {
				index, more := receiveSourceIndex(t, ctx, indices)
				require.True(t, more)
				require.Equal(t, expected, index)
				duplicateIndex, more, err := duplicated.Receive(ctx)
				require.NoError(t, err)
				require.True(t, more)
				require.Equal(t, index, duplicateIndex)
				processed <- index
			}
			_, more := receiveSourceIndex(t, ctx, indices)
			require.False(t, more)
			_, more, err = duplicated.Receive(ctx)
			require.NoError(t, err)
			require.False(t, more)
			require.Eventually(t, func() bool {
				return source.Milestone() == (dataplane_common.Milestone{
					ChunkIndex:            totalChunks,
					TransferredChunkCount: 7 + uint32(len(testCase.indices)),
				})
			}, time.Second, time.Millisecond)
		})
	}
}

func TestSourceChunkIndicesFailure(t *testing.T) {
	expectedError := errors.New("changed blocks failed")
	for _, panicValue := range []bool{false, true} {
		name := "error"
		if panicValue {
			name = "panic"
		}
		t.Run(name, func(t *testing.T) {
			ctx := sourceTestContext(t)
			session := newMockSession(t, ctx, blockSize, 8)
			client := nbs_mocks.NewClientMock()
			client.On("MountRO", ctx, "disk", (*types.EncryptionDesc)(nil)).
				Return(session, nil).Once()
			call := client.On(
				"GetChangedBlocks", ctx, "disk", uint64(0), uint32(8),
				"", "", false,
			).Return([]byte(nil), expectedError).Once()
			if panicValue {
				call.Run(func(mock.Arguments) { panic("changed blocks panic") })
			}
			source, err := nbs.NewDiskSource(
				ctx, client, "disk", "", "", "", nil,
				chunkSize, true, false, false,
			)
			require.NoError(t, err)
			defer source.Close(ctx)
			processed := make(chan uint32, 1)
			defer close(processed)
			indices, duplicated, errs := source.ChunkIndices(
				ctx, dataplane_common.Milestone{}, processed, common.ChannelWithCancellation{},
			)
			err = receiveSourceError(t, ctx, errs)
			client.AssertExpectations(t)
			if panicValue {
				var panicError *task_errors.PanicError
				require.ErrorAs(t, err, &panicError)
				require.Contains(t, err.Error(), "changed blocks panic")
			} else {
				require.ErrorIs(t, err, expectedError)
			}
			_, more := receiveSourceIndex(t, ctx, indices)
			require.False(t, more)
			_, more, err = duplicated.Receive(ctx)
			require.NoError(t, err)
			require.False(t, more)
		})
	}
}

func TestSourceChunkIndicesCancellation(t *testing.T) {
	for _, unsupported := range []bool{false, true} {
		name := "changed blocks"
		if unsupported {
			name = "default enumeration"
		}
		t.Run(name, func(t *testing.T) {
			waitCtx := sourceTestContext(t)
			ctx, cancel := context.WithCancel(waitCtx)
			defer cancel()
			session := newMockSession(t, ctx, blockSize, 16)
			client := nbs_mocks.NewClientMock()
			client.On("MountRO", ctx, "disk", (*types.EncryptionDesc)(nil)).
				Return(session, nil).Once()
			call := client.On(
				"GetChangedBlocks", ctx, "disk", uint64(0), uint32(16),
				"", "", false,
			).Once()
			if unsupported {
				call.Return([]byte(nil), &nbs_sdk.ClientError{Code: nbs_sdk.E_NOT_IMPLEMENTED})
			} else {
				call.Return([]byte{1, 1}, nil)
			}
			source, err := nbs.NewDiskSource(
				ctx, client, "disk", "", "", "", nil,
				chunkSize, false, false, false,
			)
			require.NoError(t, err)
			defer source.Close(ctx)
			processed := make(chan uint32, 1)
			defer close(processed)
			indices, duplicated, errs := source.ChunkIndices(
				ctx, dataplane_common.Milestone{}, processed, common.ChannelWithCancellation{},
			)
			require.True(t, duplicated.Empty())
			index, more := receiveSourceIndex(t, waitCtx, indices)
			if !more {
				t.Fatalf("chunk generation ended before the first index: %v", receiveSourceError(t, waitCtx, errs))
			}
			require.Zero(t, index)
			// Leave the first chunk unacknowledged so generation blocks on the
			// inflight limit, then verify that cancellation releases it.
			cancel()
			require.ErrorIs(t, receiveSourceError(t, waitCtx, errs), context.Canceled)
			client.AssertExpectations(t)
			_, more = receiveSourceIndex(t, waitCtx, indices)
			require.False(t, more)
		})
	}
}

func TestSourceEstimatedBytesToRead(t *testing.T) {
	expectedError := errors.New("changed bytes failed")
	for _, testCase := range []struct {
		name          string
		changedBytes  uint64
		clientError   error
		expectedBytes uint64
		expectedError error
	}{
		{name: "changed bytes", changedBytes: 512, expectedBytes: 512},
		{
			name:          "unsupported uses exact disk size",
			clientError:   &nbs_sdk.ClientError{Code: nbs_sdk.E_NOT_IMPLEMENTED},
			expectedBytes: 9 * 4096,
		},
		{name: "failure", clientError: expectedError, expectedError: expectedError},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := sourceTestContext(t)
			session := newMockSession(t, ctx, blockSize, 9)
			client := nbs_mocks.NewClientMock()
			client.On("MountRO", ctx, "disk", (*types.EncryptionDesc)(nil)).
				Return(session, nil).Once()
			client.On(
				"GetChangedBytes", ctx, "disk", "base", "checkpoint", true,
			).Return(testCase.changedBytes, testCase.clientError).Once()
			source, err := nbs.NewDiskSource(
				ctx, client, "disk", "", "base", "checkpoint", nil,
				chunkSize, false, true, false,
			)
			require.NoError(t, err)
			defer source.Close(ctx)
			bytes, err := source.EstimatedBytesToRead(ctx)
			require.ErrorIs(t, err, testCase.expectedError)
			require.Equal(t, testCase.expectedBytes, bytes)
			client.AssertExpectations(t)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func checkFallbackChunks(
	t *testing.T,
	ctx context.Context,
	source nbs.DiskSource,
	params diskTestParams,
	milestone uint32,
	expectedChunk func(uint32) dataplane_common.Chunk,
) {

	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	processed := make(chan uint32, 1)
	defer close(processed)
	indices, duplicates, errors := source.ChunkIndices(
		ctx,
		dataplane_common.Milestone{
			ChunkIndex:            milestone,
			TransferredChunkCount: 7,
		},
		processed,
		common.ChannelWithCancellation{},
	)
	require.True(t, duplicates.Empty())

	actualIndices := []uint32{}
	chunk := dataplane_common.Chunk{
		Data: bytes.Repeat([]byte{0xff}, int(params.chunkSize())),
		Zero: true,
	}
indicesLoop:
	for {
		var index uint32
		select {
		case value, ok := <-indices:
			if !ok {
				break indicesLoop
			}
			index = value
		case <-ctx.Done():
			t.Fatalf("waiting for chunk indices: %v", ctx.Err())
		}
		actualIndices = append(actualIndices, index)
		chunk.Index = index
		require.NoError(t, source.Read(ctx, &chunk))
		expected := expectedChunk(index)
		require.Equal(t, expected.Zero, chunk.Zero, "chunk %v", index)
		if !expected.Zero {
			require.True(t, bytes.Equal(expected.Data, chunk.Data),
				"chunk %v contents differ", index)
		}
		dataSize := min(uint64(params.chunkSize()), source.Size()-uint64(index)*uint64(params.chunkSize()))
		require.True(t, bytes.Equal(make([]byte, uint64(params.chunkSize())-dataSize), chunk.Data[dataSize:]),
			"chunk %v padding is not zero", index)
		select {
		case processed <- index:
		case <-ctx.Done():
			t.Fatalf("acknowledging chunk %v: %v", index, ctx.Err())
		}
	}
errorsLoop:
	for {
		select {
		case err, ok := <-errors:
			if !ok {
				break errorsLoop
			}
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatalf("waiting for chunk errors: %v", ctx.Err())
		}
	}

	expectedIndices := []uint32{}
	for index := milestone; index < params.chunksCount; index++ {
		expectedIndices = append(expectedIndices, index)
	}
	require.Equal(t, expectedIndices, actualIndices)

	expectedMilestone := dataplane_common.Milestone{
		ChunkIndex:            max(milestone, params.chunksCount),
		TransferredChunkCount: 7 + uint32(len(expectedIndices)),
	}
	require.Eventually(t, func() bool {
		return source.Milestone() == expectedMilestone
	}, time.Second, time.Millisecond)
}

////////////////////////////////////////////////////////////////////////////////

func TestReadWhenGetChangedBlocksIsNotSupported(t *testing.T) {
	ctx := newContext()
	factory := newFactory(t, ctx)
	client, err := factory.GetClient(ctx, "zone")
	require.NoError(t, err)

	forEachDiskParams(t, func(t *testing.T, params diskTestParams, ranges []blockRange) {
		diskID := createDisk(t, ctx, client, params)
		session, err := client.MountRW(ctx, diskID, 0, 0, nil)
		require.NoError(t, err)
		defer session.Close(ctx)

		expectedDisk := make([]byte, uint64(params.chunksCount)*uint64(params.chunkSize()))
		for _, r := range ranges {
			data := r.data(params.blockSize)
			copy(expectedDisk[r.start*uint64(params.blockSize):], data)
			if r.zero {
				require.NoError(t, session.Zero(ctx, r.start, uint32(r.end-r.start)))
			} else {
				require.NoError(t, session.Write(ctx, r.start, data))
			}
		}
		require.NoError(t, client.CreateCheckpoint(ctx, nbs_client.CheckpointParams{
			DiskID: diskID, CheckpointID: "written",
		}))

		for milestone := uint32(0); milestone <= params.chunksCount+1; milestone++ {
			t.Run(fmt.Sprintf("milestone_%v", milestone), func(t *testing.T) {
				readSession, err := client.MountRO(ctx, diskID, nil)
				require.NoError(t, err)
				fallbackClient := new(nbs_mocks.ClientMock)
				fallbackClient.On("MountRO", ctx, diskID, (*types.EncryptionDesc)(nil)).
					Return(readSession, nil).Once()
				if milestone < params.chunksCount {
					startIndex := uint64(milestone) * uint64(params.blocksInChunk)
					fallbackClient.On(
						"GetChangedBlocks", mock.Anything, diskID, startIndex,
						uint32(params.blocksCount-startIndex), "", "written", false,
					).Return([]byte(nil), &nbs_sdk.ClientError{
						Code: nbs_sdk.E_NOT_IMPLEMENTED,
					}).Once()
				}
				source := newSource(t, ctx, fallbackClient, diskID, params, "", "written")
				checkFallbackChunks(t, ctx, source, params, milestone, func(index uint32) dataplane_common.Chunk {
					start := uint64(index) * uint64(params.chunkSize())
					data := expectedDisk[start : start+uint64(params.chunkSize())]
					return dataplane_common.Chunk{
						Index: index,
						Data:  data,
						Zero:  bytes.Equal(data, make([]byte, len(data))),
					}
				})
				fallbackClient.AssertExpectations(t)
				if milestone >= params.chunksCount {
					fallbackClient.AssertNumberOfCalls(t, "GetChangedBlocks", 0)
				}
			})
		}
	})
}

func TestReadNonreplicatedDiskWithoutChangedBlocks(t *testing.T) {
	ctx := newContext()
	factory := newFactory(t, ctx)
	client, err := factory.GetClient(ctx, "zone")
	require.NoError(t, err)

	// Nonreplicated disks allocate complete 1 GiB devices in the recipe.
	params := diskTestParams{
		blockSize: blockSize, blocksInChunk: blocksInChunk,
		blocksCount: 262144, chunksCount: 262144 / blocksInChunk,
	}
	diskID := t.Name()
	require.NoError(t, client.Create(ctx, nbs_client.CreateDiskParams{
		ID: diskID, BlocksCount: params.blocksCount, BlockSize: params.blockSize,
		Kind: types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
	}))
	t.Cleanup(func() { require.NoError(t, client.Delete(ctx, diskID)) })
	session, err := client.MountRW(ctx, diskID, 0, 0, nil)
	require.NoError(t, err)
	defer session.Close(ctx)

	lastBlock := params.blocksCount - 1
	lastBlockData := (blockRange{start: lastBlock, end: lastBlock + 1}).data(params.blockSize)
	require.NoError(t, session.Write(ctx, lastBlock, lastBlockData))
	// The recipe disables shadow disks so normal nonreplicated checkpoints
	// return E_NOT_IMPLEMENTED from GetChangedBlocks.
	require.NoError(t, client.CreateCheckpoint(ctx, nbs_client.CheckpointParams{
		DiskID: diskID, CheckpointID: "normal_without_shadow",
		CheckpointType: nbs_client.CheckpointTypeNormal,
	}))
	_, err = client.GetChangedBlocks(ctx, diskID, 0, blocksInChunk, "", "normal_without_shadow", false)
	require.Error(t, err)
	require.True(t, nbs_client.IsNotImplementedError(err), "%v", err)

	source, err := nbs.NewDiskSource(
		ctx, client, diskID, "", "", "normal_without_shadow", nil,
		params.chunkSize(), false, false, true, // dontReadFromCheckpoint
	)
	require.NoError(t, err)
	t.Cleanup(func() { source.Close(ctx) })
	require.Equal(t, params.blocksCount*uint64(params.blockSize), source.Size())
	estimatedBytes, err := source.EstimatedBytesToRead(ctx)
	require.NoError(t, err)
	require.Equal(t, source.Size(), estimatedBytes)

	checkFallbackChunks(t, ctx, source, params, params.chunksCount-2, func(index uint32) dataplane_common.Chunk {
		data := make([]byte, params.chunkSize())
		if index == params.chunksCount-1 {
			copy(data[len(data)-len(lastBlockData):], lastBlockData)
		}
		// The recipe disables void-buffer optimization for nonreplicated
		// reads, so even empty chunks arrive as explicit zero-filled data.
		return dataplane_common.Chunk{Index: index, Data: data, Zero: false}
	})
}
