package url

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"context"
	"io"
	"sort"

	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
)

////////////////////////////////////////////////////////////////////////////////

func zero(data []byte) {
	// Optimized idiom https://github.com/golang/go/wiki/CompilerOptimizations#optimized-memclr.
	for i := range data {
		data[i] = 0
	}
}

// Returns amount of data that can be read from item.
func bytesToRead(item common.ImageMapItem, offset uint64, bytes int) uint64 {
	offsetInItem := offset - item.Start
	bytesAvailable := item.Length - offsetInItem

	bytesToRead := uint64(bytes)
	if bytesToRead > bytesAvailable {
		bytesToRead = bytesAvailable
	}

	return bytesToRead
}

////////////////////////////////////////////////////////////////////////////////

type imageReader struct {
	items     []common.ImageMapItem
	chunkSize uint64
	reader    common.Reader
	itemIndex int
	offset    uint64
}

func newImageReader(
	items []common.ImageMapItem,
	chunkSize uint64,
	reader common.Reader,
) *imageReader {

	return &imageReader{
		items:     items,
		chunkSize: chunkSize,
		reader:    reader,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (r *imageReader) Read(
	ctx context.Context,
	chunk *dataplane_common.Chunk,
) error {

	offset := r.chunkSize * uint64(chunk.Index)
	r.setOffset(offset)
	offsetInChunk := uint64(0)

	// Assume that chunk is zero.
	chunk.Zero = true
	for int(offsetInChunk) < len(chunk.Data) {
		if r.itemIndex >= len(r.items) {
			return nil
		}

		item := r.items[r.itemIndex]

		if !item.Zero {
			// We encountered non zero chunk and should zero all data at the
			// head.
			zero(chunk.Data[:offsetInChunk])
			chunk.Zero = false
			break
		}

		bytesToRead := bytesToRead(item, r.offset, len(chunk.Data))
		r.moveOffset(bytesToRead)
		offsetInChunk += bytesToRead
	}

	for int(offsetInChunk) < len(chunk.Data) {
		if r.itemIndex >= len(r.items) {
			// Should zero all data at the tail.
			zero(chunk.Data[offsetInChunk:])
			return nil
		}

		item := r.items[r.itemIndex]

		bytesRead, err := r.read(
			ctx,
			item,
			r.offset,
			chunk.Data[offsetInChunk:],
		)
		if err != nil {
			return err
		}

		r.moveOffset(bytesRead)
		offsetInChunk += bytesRead
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func (r *imageReader) setOffset(offset uint64) {
	r.itemIndex = sort.Search(len(r.items), func(i int) bool {
		return r.items[i].Start+r.items[i].Length > offset
	})
	r.offset = offset
}

func (r *imageReader) moveOffset(bytes uint64) {
	r.offset += bytes
	for r.itemIndex < len(r.items) &&
		r.items[r.itemIndex].Start+r.items[r.itemIndex].Length <= r.offset {
		r.itemIndex++
	}
}

// Reads data from item.
func (r *imageReader) read(
	ctx context.Context,
	item common.ImageMapItem,
	offset uint64,
	data []byte,
) (uint64, error) {

	bytesToRead := bytesToRead(item, offset, len(data))

	if item.Zero {
		zero(data[:bytesToRead])
		return bytesToRead, nil
	}

	if item.CompressedOffset != nil {
		return r.readCompressed(ctx, item, data[:bytesToRead])
	}

	return r.readRaw(ctx, item, offset, data[:bytesToRead])
}

// Reads raw data from item.
func (r *imageReader) readRaw(
	ctx context.Context,
	item common.ImageMapItem,
	offset uint64,
	data []byte,
) (uint64, error) {

	offsetInItem := offset - item.Start
	start := *item.RawOffset + offsetInItem
	return r.reader.Read(ctx, start, data)
}

// Reads compressed data from item.
func (r *imageReader) readCompressed(
	ctx context.Context,
	item common.ImageMapItem,
	data []byte,
) (uint64, error) {

	start := *item.CompressedOffset
	compressedData := make([]byte, *item.CompressedSize)

	compressedBytesRead, err := r.reader.Read(ctx, start, compressedData)
	if err != nil {
		return 0, err
	}

	compressedData = compressedData[:compressedBytesRead]

	var zreader io.ReadCloser
	switch item.CompressionType {
	case common.CompressionTypeFlate:
		zreader = flate.NewReader(bytes.NewReader(compressedData))
	case common.CompressionTypeZlib:
		zreader, err = zlib.NewReader(bytes.NewReader(compressedData))
		if err != nil {
			return 0, err
		}
	default:
		return 0, common.NewSourceInvalidError(
			"unknown compression type: %v",
			item.CompressionType,
		)
	}

	bytesRead, err := io.ReadFull(zreader, data)
	if err != nil {
		return 0, common.NewSourceInvalidError(
			"failed to inflate range: start %d, size %d: %v",
			start,
			len(data),
			err,
		)
	}

	return uint64(bytesRead), nil
}
