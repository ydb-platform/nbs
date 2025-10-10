package tests

import (
	"testing"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

func TestImageServiceCellsCreateImageFromDisk(t *testing.T) {
	testImageServiceCreateImageFromDiskWithKind(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD,
		uint64(4*1024*1024), // diskSize.
		shardedZoneID,
		cellID, // diskCellID. Zone is sharded. Policy is FIRST_IN_CONFIG.
	)
}
