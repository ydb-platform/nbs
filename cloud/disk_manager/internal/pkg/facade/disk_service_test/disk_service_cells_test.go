package disk_service_test

import (
	"testing"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type TestCase struct {
	name   string
	zoneID string
}

func cellsTestCases() []TestCase {
	return []TestCase{
		{
			name:   "Sharded zone",
			zoneID: shardedZoneID,
		},
		{
			name:   "Cell in sharded zone",
			zoneID: cellID1,
		},
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceCellsCreateEmptyDisk(t *testing.T) {
	for _, testCase := range cellsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testDiskServiceCreateEmptyDiskWithZoneID(
				t,
				testCase.zoneID,
			)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceCellsCreateDiskFromImage(t *testing.T) {
	for _, testCase := range cellsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testCreateDiskFromImage(
				t,
				disk_manager.DiskKind_DISK_KIND_SSD,
				32*1024*4096, // imageSize
				false,        // pooled
				32*1024*4096, // diskSize
				"folder",
				nil, // encryptionDesc
				testCase.zoneID,
			)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceCellsCreateDiskFromSnapshot(t *testing.T) {
	for _, testCase := range cellsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testDiskServiceCreateDiskFromSnapshotWithZoneID(t, testCase.zoneID)
		})
	}
}
