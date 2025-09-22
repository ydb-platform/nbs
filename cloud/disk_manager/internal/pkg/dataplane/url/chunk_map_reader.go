package url

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
)

////////////////////////////////////////////////////////////////////////////////

type chunkMapEntry struct {
	imageMap   common.ImageMap
	chunkIndex uint32
}

////////////////////////////////////////////////////////////////////////////////

type chunkMapReader struct {
	imageMapReader common.ImageMapReader
	chunkSize      uint64
}

func newChunkMapReader(
	ctx context.Context,
	imageMapReader common.ImageMapReader,
	chunkSize uint64,
) (*chunkMapReader, error) {

	return &chunkMapReader{
		imageMapReader: imageMapReader,
		chunkSize:      chunkSize,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func (r *chunkMapReader) size() uint64 {
	return r.imageMapReader.Size()
}

func (r *chunkMapReader) chunkCount() uint32 {
	size := r.size()

	if size%r.chunkSize == 0 {
		return uint32(size / r.chunkSize)
	}

	return uint32(size/r.chunkSize) + 1
}

func (r *chunkMapReader) read(ctx context.Context) ([]chunkMapEntry, error) {
	items, err := r.imageMapReader.Read(ctx)
	if err != nil {
		return nil, err
	}

	imageMap := common.ImageMap{Items: items}
	var entries []chunkMapEntry
	var chunkIndex uint32

	for {
		chunkImageMap := imageMap.CutChunk(r.chunkSize)
		if chunkImageMap.Empty() {
			break
		}

		entries = append(entries, chunkMapEntry{
			imageMap:   chunkImageMap,
			chunkIndex: chunkIndex,
		})
		chunkIndex++
	}

	return entries, nil
}
