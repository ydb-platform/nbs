package disk_service_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
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
			testCreateDiskFromImageWithZoneID(
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

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceCellsResizeDisk(t *testing.T) {
	for _, testCase := range cellsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testDiskServiceResizeDiskWithZoneID(t, testCase.zoneID)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceCellsAlterDisk(t *testing.T) {
	for _, testCase := range cellsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testDiskServiceAlterDiskWithZoneID(t, testCase.zoneID)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceMigrateDiskBetweenCells(t *testing.T) {
	diskID := t.Name()
	params := migrationTestParams{
		SrcZoneID: "zone-d",
		DstZoneID: "zone-d-shard1",
		DiskID:    diskID,
		DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD,
		DiskSize:  migrationTestsDiskSize,
		FillDisk:  true,
	}

	ctx, client := setupMigrationTest(t, params)
	defer client.Close()

	successfullyMigrateDisk(t, ctx, client, params)

	testcommon.DeleteDisk(t, ctx, client, diskID)

	// Check that disk is deleted.
	srcZoneNBSClient := testcommon.NewNbsTestingClient(t, ctx, params.SrcZoneID)
	_, err := srcZoneNBSClient.Describe(ctx, params.DiskID)
	require.Error(t, err)
	require.ErrorContains(t, err, "Path not found")

	dstZoneNBSClient := testcommon.NewNbsTestingClient(t, ctx, params.DstZoneID)
	_, err = dstZoneNBSClient.Describe(ctx, params.DiskID)
	require.Error(t, err)
	require.ErrorContains(t, err, "Path not found")

	testcommon.CheckConsistency(t, ctx)
}
