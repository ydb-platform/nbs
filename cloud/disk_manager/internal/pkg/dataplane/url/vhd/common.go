package vhd

import "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"

////////////////////////////////////////////////////////////////////////////////

type imageMapEntry struct {
	hasData bool
	offset  uint64
}

func (e *imageMapEntry) mergeable(other imageMapEntry) bool {
	return e.hasData == other.hasData && !e.hasData
}

func (e *imageMapEntry) dumpToItem(item *common.ImageMapItem) {
	item.Data = e.hasData
	item.Zero = !e.hasData

	if e.offset != 0 {
		item.RawOffset = new(uint64)
		*item.RawOffset = e.offset
	}
}
