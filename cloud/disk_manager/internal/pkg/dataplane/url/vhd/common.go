package vhd

import "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"

////////////////////////////////////////////////////////////////////////////////

type imageMapEntry struct {
	data   bool
	offset uint64
}

func (e *imageMapEntry) mergeable(other imageMapEntry) bool {
	return e.data == other.data && e.offset == other.offset
}

func (e *imageMapEntry) dumpToItem(item *common.ImageMapItem) {
	if e.data {
		item.Zero = false
		item.Data = true
	} else {
		item.Zero = true
		item.Data = false
	}

	if e.offset != 0 {
		item.RawOffset = new(uint64)
		*item.RawOffset = e.offset
	}
}
