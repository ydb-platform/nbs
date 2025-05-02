package tests

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

const (
	defaultHTTPClientTimeout         = time.Minute
	defaultHTTPClientMinRetryTimeout = time.Second
	defaultHTTPClientMaxRetryTimeout = 8 * time.Second
	defaultHTTPClientMaxRetries      = 5
)

////////////////////////////////////////////////////////////////////////////////

func getImageFileURL(port string) string {
	return fmt.Sprintf("http://localhost:%v", port)
}

func parseUint32(t *testing.T, str string) uint32 {
	value, err := strconv.ParseUint(str, 10, 32)
	require.NoError(t, err)
	return uint32(value)
}

func parseUint64(t *testing.T, str string) uint64 {
	value, err := strconv.ParseUint(str, 10, 64)
	require.NoError(t, err)
	return value
}

func getExpectedChunkCount(imageSize uint64) uint32 {
	if imageSize%chunkSize == 0 {
		return uint32(imageSize / chunkSize)
	}

	return uint32(imageSize/chunkSize) + 1
}

////////////////////////////////////////////////////////////////////////////////

const (
	chunkSize = 4 * 1024 * 1024
)

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func checkChunks(
	t *testing.T,
	ctx context.Context,
	imageCRC32 uint32,
	chunkCount uint32,
	source dataplane_common.Source,
) {

	chunks := make(map[uint32]dataplane_common.Chunk)
	processedChunkIndices := make(chan uint32, chunkCount*2)

	chunkIndices, _, chunkIndicesErrors := source.ChunkIndices(
		ctx,
		dataplane_common.Milestone{},
		processedChunkIndices,
		common.ChannelWithCancellation[uint32]{}, // holeChunkIndices
	)

	more := true
	var chunkIndex uint32

	for more {
		select {
		case chunkIndex, more = <-chunkIndices:
			if !more {
				break
			}

			chunk := dataplane_common.Chunk{
				Index: chunkIndex,
				Data:  make([]byte, chunkSize),
			}

			err := source.Read(ctx, &chunk)
			require.NoError(t, err)

			chunks[chunkIndex] = chunk
		case err := <-chunkIndicesErrors:
			require.NoError(t, err)
		}
	}

	require.Equal(t, chunkCount, uint32(len(chunks)))

	acc := crc32.NewIEEE()
	for i := uint32(0); i < uint32(len(chunks)); i++ {
		data, ok := chunks[i]
		require.True(t, ok)

		_, err := acc.Write(data.Data)
		require.NoError(t, err)
	}

	require.Equal(t, imageCRC32, acc.Sum32())
}

////////////////////////////////////////////////////////////////////////////////

func TestImageReading(t *testing.T) {
	testCases := []struct {
		name       string
		imageURL   string
		imageSize  uint64
		imageCRC32 uint32
	}{
		{
			name:       "raw image",
			imageURL:   getImageFileURL(os.Getenv("DISK_MANAGER_RECIPE_RAW_IMAGE_FILE_SERVER_PORT")),
			imageSize:  parseUint64(t, os.Getenv("DISK_MANAGER_RECIPE_RAW_IMAGE_SIZE")),
			imageCRC32: parseUint32(t, os.Getenv("DISK_MANAGER_RECIPE_RAW_IMAGE_CRC32")),
		},
		{
			name:       "qcow2 image",
			imageURL:   getImageFileURL(os.Getenv("DISK_MANAGER_RECIPE_QCOW2_IMAGE_FILE_SERVER_PORT")),
			imageSize:  parseUint64(t, os.Getenv("DISK_MANAGER_RECIPE_QCOW2_IMAGE_SIZE")),
			imageCRC32: parseUint32(t, os.Getenv("DISK_MANAGER_RECIPE_QCOW2_IMAGE_CRC32")),
		},
		{
			name:       "vmdk image",
			imageURL:   getImageFileURL(os.Getenv("DISK_MANAGER_RECIPE_VMDK_IMAGE_FILE_SERVER_PORT")),
			imageSize:  parseUint64(t, os.Getenv("DISK_MANAGER_RECIPE_VMDK_IMAGE_SIZE")),
			imageCRC32: parseUint32(t, os.Getenv("DISK_MANAGER_RECIPE_VMDK_IMAGE_CRC32")),
		},
		{
			name:       "vhd raw image",
			imageURL:   getImageFileURL(os.Getenv("DISK_MANAGER_RECIPE_VHD_RAW_IMAGE_FILE_SERVER_PORT")),
			imageSize:  parseUint64(t, os.Getenv("DISK_MANAGER_RECIPE_VHD_RAW_IMAGE_SIZE")),
			imageCRC32: parseUint32(t, os.Getenv("DISK_MANAGER_RECIPE_VHD_RAW_IMAGE_CRC32")),
		},
		{
			name:       "vhd dynamic image",
			imageURL:   getImageFileURL(os.Getenv("DISK_MANAGER_RECIPE_VHD_DYNAMIC_IMAGE_FILE_SERVER_PORT")),
			imageSize:  parseUint64(t, os.Getenv("DISK_MANAGER_RECIPE_VHD_DYNAMIC_IMAGE_SIZE")),
			imageCRC32: parseUint32(t, os.Getenv("DISK_MANAGER_RECIPE_VHD_DYNAMIC_IMAGE_CRC32")),
		},
		{
			name:       "vhd ubuntu image",
			imageURL:   getImageFileURL(os.Getenv("DISK_MANAGER_RECIPE_VHD_UBUNTU1604_IMAGE_FILE_SERVER_PORT")),
			imageSize:  parseUint64(t, os.Getenv("DISK_MANAGER_RECIPE_VHD_UBUNTU1604_IMAGE_SIZE")),
			imageCRC32: parseUint32(t, os.Getenv("DISK_MANAGER_RECIPE_VHD_UBUNTU1604_IMAGE_CRC32")),
		},
		{
			name:       "vmdk stream optimized image",
			imageURL:   getImageFileURL(os.Getenv("DISK_MANAGER_RECIPE_VMDK_STREAM_OPTIMIZED_IMAGE_FILE_SERVER_PORT")),
			imageSize:  parseUint64(t, os.Getenv("DISK_MANAGER_RECIPE_VMDK_STREAM_OPTIMIZED_IMAGE_SIZE")),
			imageCRC32: parseUint32(t, os.Getenv("DISK_MANAGER_RECIPE_VMDK_STREAM_OPTIMIZED_IMAGE_CRC32")),
		},
		{
			// Has an additional footer (to specify grain directory offset), unlike the previous one.
			name:       "vmdk stream optimized ubuntu image",
			imageURL:   getImageFileURL(os.Getenv("DISK_MANAGER_RECIPE_VMDK_UBUNTU2204_IMAGE_FILE_SERVER_PORT")),
			imageSize:  parseUint64(t, os.Getenv("DISK_MANAGER_RECIPE_VMDK_UBUNTU2204_IMAGE_SIZE")),
			imageCRC32: parseUint32(t, os.Getenv("DISK_MANAGER_RECIPE_VMDK_UBUNTU2204_IMAGE_CRC32")),
		},
		{
			name:       "vmdk stream optimized windows image with multiple grains",
			imageURL:   getImageFileURL(os.Getenv("DISK_MANAGER_RECIPE_VMDK_WINDOWS_FILE_SERVER_PORT")),
			imageSize:  parseUint64(t, os.Getenv("DISK_MANAGER_RECIPE_VMDK_WINDOWS_IMAGE_SIZE")),
			imageCRC32: parseUint32(t, os.Getenv("DISK_MANAGER_RECIPE_VMDK_WINDOWS_IMAGE_CRC32")),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := newContext()

			source, err := url.NewURLSource(
				ctx,
				defaultHTTPClientTimeout,
				defaultHTTPClientMinRetryTimeout,
				defaultHTTPClientMaxRetryTimeout,
				defaultHTTPClientMaxRetries,
				testCase.imageURL,
				chunkSize,
			)
			require.NoError(t, err)
			defer source.Close(ctx)

			chunkCount, err := source.ChunkCount(ctx)
			require.NoError(t, err)
			expectedChunkCount := getExpectedChunkCount(testCase.imageSize)
			require.Equal(t, expectedChunkCount, chunkCount)

			checkChunks(t, ctx, testCase.imageCRC32, chunkCount, source)

			expectedCacheMissedReadCounts := uint64(getExpectedChunkCount(
				testCase.imageSize,
			) * 2)

			require.LessOrEqual(
				t,
				source.CacheMissedRequestsCount(),
				expectedCacheMissedReadCounts,
			)
		})
	}
}

func TestInvalidImageReading(t *testing.T) {
	ctx := newContext()
	imageURL := getImageFileURL(
		os.Getenv("DISK_MANAGER_RECIPE_INVALID_QCOW2_IMAGE_FILE_SERVER_PORT"),
	)

	source, err := url.NewURLSource(
		ctx,
		defaultHTTPClientTimeout,
		defaultHTTPClientMinRetryTimeout,
		defaultHTTPClientMaxRetryTimeout,
		defaultHTTPClientMaxRetries,
		imageURL,
		chunkSize,
	)
	require.NoError(t, err)
	defer source.Close(ctx)

	processedChunkIndices := make(chan uint32, 100500)

	chunkIndices, _, chunkIndicesErrors := source.ChunkIndices(
		ctx,
		dataplane_common.Milestone{},
		processedChunkIndices,
		common.ChannelWithCancellation[uint32]{}, // holeChunkIndices
	)

	var chunkIndex uint32
	more := true
	var foundErrors []error

	for more {
		select {
		case chunkIndex, more = <-chunkIndices:
			if !more {
				break
			}

			chunk := dataplane_common.Chunk{
				Index: chunkIndex,
				Data:  make([]byte, chunkSize),
			}

			err := source.Read(ctx, &chunk)
			if err != nil {
				foundErrors = append(foundErrors, err)
			}
		case err := <-chunkIndicesErrors:
			if err != nil {
				foundErrors = append(foundErrors, err)
			}
		}
	}

	require.NotEmpty(t, foundErrors)
}
