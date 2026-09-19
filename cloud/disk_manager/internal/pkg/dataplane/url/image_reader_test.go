package url

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
)

////////////////////////////////////////////////////////////////////////////////

type imageDataReader struct {
	common.Reader
	data []byte
}

func (r imageDataReader) Read(
	ctx context.Context,
	start uint64,
	data []byte,
) (uint64, error) {

	n, err := bytes.NewReader(r.data).ReadAt(data, int64(start))
	return uint64(n), err
}

////////////////////////////////////////////////////////////////////////////////

func TestImageReaderAlignedCompressedExtents(t *testing.T) {
	const extentSize = 2 * 1024 * 1024
	for _, compressionType := range []common.CompressionType{
		common.CompressionTypeFlate,
		common.CompressionTypeZlib,
	} {
		for _, chunkSize := range []uint64{4 * 1024 * 1024, 8 * 1024 * 1024, 12 * 1024 * 1024, 16 * 1024 * 1024} {
			t.Run(fmt.Sprintf("compression_%v/chunk_%v", compressionType, chunkSize), func(t *testing.T) {
				ctx := context.Background()
				data := make([]byte, extentSize)
				for i := range data {
					data[i] = byte(i/(128*1024) + 1)
				}

				var compressed bytes.Buffer
				var writer io.WriteCloser
				if compressionType == common.CompressionTypeFlate {
					var err error
					writer, err = flate.NewWriter(&compressed, flate.DefaultCompression)
					require.NoError(t, err)
				} else {
					writer = zlib.NewWriter(&compressed)
				}
				_, err := writer.Write(data)
				require.NoError(t, err)
				require.NoError(t, writer.Close())

				// Place compressed extents on both sides of a chunk boundary.
				prefix := bytes.Repeat([]byte{0xa5}, int(chunkSize)-extentSize)
				imageData := append(append([]byte{}, prefix...), compressed.Bytes()...)
				imageData = append(imageData, compressed.Bytes()...)
				expected := append(append([]byte{}, prefix...), data...)
				expected = append(expected, data...)
				rawOffset := uint64(0)
				compressedOffset := uint64(len(prefix))
				compressedSize := uint64(compressed.Len())
				nextCompressedOffset := compressedOffset + compressedSize
				imageMap := common.ImageMap{Items: []common.ImageMapItem{
					{
						Length:    uint64(len(prefix)),
						Data:      true,
						RawOffset: &rawOffset,
					},
					{
						Start:            uint64(len(prefix)),
						Length:           extentSize,
						Data:             true,
						CompressedOffset: &compressedOffset,
						CompressedSize:   &compressedSize,
						CompressionType:  compressionType,
					},
					{
						Start:            chunkSize,
						Length:           extentSize,
						Data:             true,
						CompressedOffset: &nextCompressedOffset,
						CompressedSize:   &compressedSize,
						CompressionType:  compressionType,
					},
				}}

				for index := uint32(0); !imageMap.Empty(); index++ {
					chunkMap := imageMap.CutChunk(chunkSize)
					reader := newImageReader(chunkMap.Items, chunkSize, imageDataReader{data: imageData})
					chunk := dataplane_common.Chunk{
						Index: index,
						Data:  bytes.Repeat([]byte{0xff}, int(chunkSize)),
					}
					require.NoError(t, reader.Read(ctx, &chunk))
					require.False(t, chunk.Zero)
					expectedChunk := make([]byte, chunkSize)
					copy(expectedChunk, expected[uint64(index)*chunkSize:])
					require.Equal(t, expectedChunk, chunk.Data)
				}
			})
		}
	}
}
