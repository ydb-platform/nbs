package tests

import (
	"testing"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

func TestSnapshotServiceCellsCreateSnapshotFromDisk(t *testing.T) {
	testCreateSnapshotFromDiskWithZoneID(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD,
		4096,         // diskBlockSize
		32*1024*4096, // diskSize
		shardedZoneID,
		cellID, // diskCellID. Zone is sharded. Policy is FIRST_IN_CONFIG.
	)
}
