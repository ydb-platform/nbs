package vmdk

import "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"

////////////////////////////////////////////////////////////////////////////////

type grain struct {
	start          uint64
	length         uint64
	zero           bool
	compressed     bool
	offset         uint64
	compressedSize uint64
}

func (g *grain) item() common.ImageMapItem {
	item := common.ImageMapItem{
		Start:  g.start,
		Length: g.length,
	}

	if g.zero {
		item.Zero = true
		return item
	}

	item.Data = true
	if !g.compressed {
		item.RawOffset = &g.offset
		return item
	}

	item.CompressedOffset = &g.offset
	item.CompressedSize = &g.compressedSize
	item.CompressionType = common.CompressionTypeZlib

	return item
}

func (g *grain) mergeable(other grain) bool {
	return g.zero == other.zero &&
		!g.compressed &&
		!other.compressed &&
		g.offset+g.length == other.offset
}

func (g *grain) merge(other grain) {
	g.length += other.length
}
