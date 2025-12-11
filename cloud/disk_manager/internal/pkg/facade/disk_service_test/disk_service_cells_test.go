package disk_service_test

import (
	"context"
	"crypto/rand"
	"hash/crc32"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
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

func successfullyMigrateDiskBetweenCells(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	params migrationTestParams,
) {
	srcZoneNBSClient := testcommon.NewNbsTestingClient(t, ctx, params.SrcZoneID)

	diskSize := uint64(params.DiskSize)
	diskContentInfo, err := srcZoneNBSClient.CalculateCrc32(params.DiskID, diskSize)
	require.NoError(t, err)

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.MigrateDisk(reqCtx, &disk_manager.MigrateDiskRequest{
		DiskId: &disk_manager.DiskId{
			DiskId: params.DiskID,
			ZoneId: params.SrcZoneID,
		},
		DstZoneId:               params.DstZoneID,
		IsMigrationBetweenCells: true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	for {
		data := make([]byte, 4096)
		_, err := rand.Read(data)
		require.NoError(t, err)

		common.WaitForRandomDuration(1*time.Second, 3*time.Second)
		err = srcZoneNBSClient.Write(
			params.DiskID,
			0, // startIndex
			data,
		)
		if err != nil {
			if nbs.IsDiskNotFoundError(err) {
				break
			}

			require.NoError(t, err)
		} else {
			diskContentInfo.BlockCrc32s[0] = crc32.ChecksumIEEE(data)
		}
	}

	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	_, err = srcZoneNBSClient.Describe(ctx, params.DiskID)
	require.Error(t, err)
	require.ErrorContains(t, err, "Path not found")

	dstZoneNBSClient := testcommon.NewNbsTestingClient(t, ctx, params.DstZoneID)

	err = dstZoneNBSClient.ValidateCrc32(
		ctx,
		params.DiskID,
		diskContentInfo,
	)
	require.NoError(t, err)
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

	successfullyMigrateDiskBetweenCells(t, ctx, client, params)

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
