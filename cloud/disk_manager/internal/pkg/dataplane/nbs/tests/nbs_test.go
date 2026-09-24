package tests

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
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

func checkChunks(
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

func TestChunkIndices(t *testing.T) {
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

func TestReadWrite(t *testing.T) {
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
		for _, zero := range []bool{false, true} {
			require.Error(t, target.Write(ctx, dataplane_common.Chunk{
				Index: params.chunksCount, Data: make([]byte, params.chunkSize()), Zero: zero,
			}))
		}
		require.Error(t, target.Write(ctx, dataplane_common.Chunk{Index: lastChunk, Data: []byte{1}}))

		require.NoError(t, client.CreateCheckpoint(ctx, nbs_client.CheckpointParams{
			DiskID: diskID, CheckpointID: "written",
		}))
		source := newSource(t, ctx, client, diskID, params, "", "written")
		checkChunks(t, ctx, source, params.chunkSize(), chunks)
		require.Error(t, source.Read(ctx, &dataplane_common.Chunk{
			Index: params.chunksCount, Data: make([]byte, params.chunkSize()),
		}))
		require.Error(t, source.Read(ctx, &dataplane_common.Chunk{
			Index: lastChunk, Data: make([]byte, params.chunkSize()-1),
		}))

		target.Close(ctx)
		for _, ignoreZeroChunks := range []bool{true, false} {
			t.Run(fmt.Sprintf("ignoreZeroChunks_%v", ignoreZeroChunks), func(t *testing.T) {
				target, err := nbs.NewDiskTarget(
					ctx, factory, disk, nil, params.chunkSize(), ignoreZeroChunks, 0, 0,
				)
				require.NoError(t, err)
				defer target.Close(ctx)
				require.NoError(t, target.Write(ctx, dataplane_common.Chunk{Index: lastChunk, Zero: true}))
				if ignoreZeroChunks {
					require.NoError(t, target.Write(ctx, dataplane_common.Chunk{
						Index: params.chunksCount, Zero: true,
					}))
					require.Error(t, target.Write(ctx, dataplane_common.Chunk{
						Index: params.chunksCount, Data: make([]byte, params.chunkSize()),
					}))
				} else {
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
		checkChunks(t, ctx, zeroedSource, params.chunkSize(), chunks)
		checkChunks(t, ctx, source, params.chunkSize(), []dataplane_common.Chunk{lastWrittenChunk})
	})
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
			checkChunks(t, ctx, source, chunkSize, expectedChunks)

			sourceChunkCount, err := source.ChunkCount(ctx)
			require.NoError(t, err)
			require.Equal(t, chunkCount, sourceChunkCount)
		}()
	}
}
