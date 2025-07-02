package tests

import (
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
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
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

func TestDeleteEncryptedDiskDuringFilling(t *testing.T) {
	ctx := newContext()

	factory := newFactory(t, ctx)
	client, err := factory.GetClient(ctx, "zone")
	require.NoError(t, err)

	diskID := t.Name()
	disk := &types.Disk{ZoneId: "zone", DiskId: diskID}

	ecnryptionDesc := &types.EncryptionDesc{
		Mode: types.EncryptionMode_ENCRYPTION_AES_XTS,
		Key: &types.EncryptionDesc_KmsKey{
			KmsKey: &types.KmsKey{
				KekId:        "kekid",
				EncryptedDEK: []byte("encrypteddek"),
				TaskId:       "taskid",
			},
		},
	}

	err = client.Create(ctx, nbs_client.CreateDiskParams{
		ID:             diskID,
		BlocksCount:    uint64(blocksInChunk * chunkCount),
		BlockSize:      blockSize,
		Kind:           types.DiskKind_DISK_KIND_SSD,
		EncryptionDesc: ecnryptionDesc,
	})
	require.NoError(t, err)

	client.Delete(ctx, diskID)

	_, err = nbs.NewDiskTarget(
		ctx,
		factory,
		disk,
		ecnryptionDesc,
		chunkSize,
		false, // ignoreZeroChunks
		0,     // fillGeneration
		0,     // fillSeqNumber
	)
	require.Error(t, err)
	nonRetriableErr := errors.NewEmptyNonRetriableError()
	require.True(t, errors.Is(err, nonRetriableErr))
	errors.As(err, &nonRetriableErr)
	require.True(t, nonRetriableErr.Silent)
}
