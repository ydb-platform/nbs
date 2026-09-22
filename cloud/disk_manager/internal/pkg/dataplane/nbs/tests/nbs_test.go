package tests

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

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

func TestReadWritePartialChunkAndIncrementalZero(t *testing.T) {
	ctx := newContext()
	factory := newFactory(t, ctx)
	client, err := factory.GetClient(ctx, "zone")
	require.NoError(t, err)

	const partialChunkSize = uint32(4 * 1024 * 1024)
	const blocksPerChunk = uint64(partialChunkSize / blockSize)
	diskPrefix := t.Name()
	for _, diskBlocks := range []uint64{1, 7, 9, 1023, 1024, 1025, 2051} {
		t.Run(fmt.Sprintf("blocks_%v", diskBlocks), func(t *testing.T) {
			diskID := fmt.Sprintf("%v-blocks-%v", diskPrefix, diskBlocks)
			disk := &types.Disk{ZoneId: "zone", DiskId: diskID}
			diskSize := diskBlocks * uint64(blockSize)
			count := uint32((diskBlocks + blocksPerChunk - 1) / blocksPerChunk)
			lastChunk := count - 1
			tailStart := uint64(lastChunk) * uint64(partialChunkSize)
			tailSize := diskSize - tailStart
			require.NoError(t, client.Create(ctx, nbs_client.CreateDiskParams{
				ID:          diskID,
				BlocksCount: diskBlocks,
				BlockSize:   blockSize,
				Kind:        types.DiskKind_DISK_KIND_SSD,
			}))

			newSource := func(base, checkpoint string) nbs.DiskSource {
				source, err := nbs.NewDiskSource(
					ctx, client, diskID, "", base, checkpoint, nil, partialChunkSize,
					false, false, false,
				)
				require.NoError(t, err)
				require.Equal(t, diskSize, source.Size())
				actualCount, err := source.ChunkCount(ctx)
				require.NoError(t, err)
				require.Equal(t, count, actualCount)
				return source
			}
			newTarget := func(ignoreZero bool) nbs.DiskTarget {
				target, err := nbs.NewDiskTarget(
					ctx, factory, disk, nil, partialChunkSize, ignoreZero, 0, 0,
				)
				require.NoError(t, err)
				require.Equal(t, diskSize, target.Size())
				return target
			}
			// Check target effects independently of the dataplane source.
			checkDisk := func(expected []byte) {
				session, err := client.MountRO(ctx, diskID, nil)
				require.NoError(t, err)
				defer session.Close(ctx)
				actual := make([]byte, diskSize)
				for start := uint64(0); start < diskBlocks; start += blocksPerChunk {
					blocks := blocksPerChunk
					if blocks > diskBlocks-start {
						blocks = diskBlocks - start
					}
					var zero bool
					require.NoError(t, session.Read(
						ctx, start, uint32(blocks), "",
						actual[start*uint64(blockSize):(start+blocks)*uint64(blockSize)],
						&zero,
					))
				}
				require.True(t, bytes.Equal(expected, actual), "disk contents differ")
			}

			expected := make([]byte, diskSize)
			allIndices := make([]uint32, count)
			target := newTarget(false)
			for index := uint32(0); index < count; index++ {
				data := bytes.Repeat([]byte{byte(0x37 + index)}, int(partialChunkSize))
				require.NoError(t, target.Write(ctx, dataplane_common.Chunk{Index: index, Data: data}))
				copy(expected[uint64(index)*uint64(partialChunkSize):], data)
				allIndices[index] = index
			}
			for _, zero := range []bool{false, true} {
				require.Error(t, target.Write(ctx, dataplane_common.Chunk{
					Index: count, Data: make([]byte, partialChunkSize), Zero: zero,
				}))
			}
			require.Error(t, target.Write(ctx, dataplane_common.Chunk{Index: lastChunk, Data: []byte{1}}))
			target.Close(ctx)
			checkDisk(expected)

			target = newTarget(true)
			require.NoError(t, target.Write(ctx, dataplane_common.Chunk{Index: lastChunk, Zero: true}))
			require.NoError(t, target.Write(ctx, dataplane_common.Chunk{Index: count, Zero: true}))
			require.Error(t, target.Write(ctx, dataplane_common.Chunk{
				Index: count, Data: make([]byte, partialChunkSize),
			}))
			target.Close(ctx)
			checkDisk(expected)
			require.NoError(t, client.CreateCheckpoint(ctx, nbs_client.CheckpointParams{
				DiskID: diskID, CheckpointID: "base",
			}))

			chunk := dataplane_common.Chunk{
				Data: bytes.Repeat([]byte{0xff}, int(partialChunkSize)),
				Zero: true,
			}
			checkChunk := func(source nbs.DiskSource, index uint32) {
				chunk.Index = index
				require.NoError(t, source.Read(ctx, &chunk))
				require.False(t, chunk.Zero)
				contents := make([]byte, partialChunkSize)
				copy(contents, expected[uint64(index)*uint64(partialChunkSize):])
				require.True(t, bytes.Equal(contents, chunk.Data), "chunk %v data or padding differs", index)
			}
			source := newSource("", "base")
			for index := uint32(0); index < count; index++ {
				checkChunk(source, index)
			}
			chunk.Index = count
			require.Error(t, source.Read(ctx, &chunk))
			require.Error(t, source.Read(ctx, &dataplane_common.Chunk{
				Index: lastChunk, Data: make([]byte, partialChunkSize-1),
			}))
			source.Close(ctx)

			checkIndices := func(base, checkpoint string, expectedIndices []uint32) {
				milestones := []uint32{0, count, count + 1}
				if lastChunk != 0 {
					milestones = append(milestones, lastChunk)
				}
				for _, milestone := range milestones {
					source := newSource(base, checkpoint)
					processed := make(chan uint32, 1)
					indices, _, errors := source.ChunkIndices(
						ctx, dataplane_common.Milestone{ChunkIndex: milestone}, processed,
						common.ChannelWithCancellation{},
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
					wanted := []uint32{}
					for _, index := range expectedIndices {
						if index >= milestone {
							wanted = append(wanted, index)
						}
					}
					require.Equal(t, wanted, actual, "checkpoint %v, milestone %v", checkpoint, milestone)
					source.Close(ctx)
				}
			}
			checkIndices("", "base", allIndices)

			target = newTarget(false)
			require.NoError(t, target.Write(ctx, dataplane_common.Chunk{Index: lastChunk, Zero: true}))
			target.Close(ctx)
			zeroed := append([]byte(nil), expected...)
			clear(zeroed[tailStart:])
			checkDisk(zeroed)
			require.NoError(t, client.CreateCheckpoint(ctx, nbs_client.CheckpointParams{
				DiskID: diskID, CheckpointID: "incremental",
			}))
			checkIndices("base", "incremental", []uint32{lastChunk})

			source = newSource("base", "incremental")
			chunk.Index = lastChunk
			chunk.Data = bytes.Repeat([]byte{0xff}, int(partialChunkSize))
			require.NoError(t, source.Read(ctx, &chunk))
			require.True(t, chunk.Zero)
			require.True(t, bytes.Equal(make([]byte, uint64(partialChunkSize)-tailSize), chunk.Data[tailSize:]),
				"zero chunk padding retains previous buffer contents")
			source.Close(ctx)

			// Reuse the zero chunk's buffer and flag for an older nonzero version.
			// This also checks that zeroing the live disk preserved its checkpoint.
			source = newSource("", "base")
			checkChunk(source, lastChunk)
			source.Close(ctx)
		})
	}
}
