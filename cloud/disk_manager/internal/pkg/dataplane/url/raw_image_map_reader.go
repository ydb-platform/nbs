package url

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
)

////////////////////////////////////////////////////////////////////////////////

type rawImageMapReader struct {
	size uint64
}

func (r *rawImageMapReader) Size() uint64 {
	return r.size
}

func (r *rawImageMapReader) Read(
	ctx context.Context,
) ([]common.ImageMapItem, error) {

	if r.size == 0 {
		return nil, nil
	}

	var items []common.ImageMapItem
	items = append(items, common.ImageMapItem{
		Start:     0,
		Length:    r.size,
		Zero:      false,
		Data:      true,
		RawOffset: new(uint64),
	})
	return items, nil
}

////////////////////////////////////////////////////////////////////////////////

func newRawImageMapReader(size uint64) common.ImageMapReader {
	return &rawImageMapReader{
		size: size,
	}
}
