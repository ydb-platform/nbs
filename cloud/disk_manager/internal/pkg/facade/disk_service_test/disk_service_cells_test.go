package disk_service_test

import (
	"context"
	"crypto/rand"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
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

	// TODO: Make freeze operation transactional.
	// See: https://github.com/ydb-platform/nbs/issues/4847#issuecomment-3656284845
	var confirmedDiskContentInfoCrc32, unconfirmedDiskContentInfoCrc32 uint32
	for {
		data := make([]byte, 4096)
		_, err := rand.Read(data)
		require.NoError(t, err)

		crc32 := crc32.ChecksumIEEE(data)

		logging.Info(
			ctx,
			"Writing zero block with crc32 %+v to disk %+v",
			crc32,
			params.DiskID,
		)
		unconfirmedDiskContentInfoCrc32 = crc32
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
			logging.Info(
				ctx,
				"Writed zero block with crc32 %+v to disk %+v",
				crc32,
				params.DiskID,
			)
			confirmedDiskContentInfoCrc32 = crc32
		}
	}

	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	_, err = srcZoneNBSClient.Describe(ctx, params.DiskID)
	require.Error(t, err)
	require.ErrorContains(t, err, "Path not found")

	dstZoneNBSClient := testcommon.NewNbsTestingClient(t, ctx, params.DstZoneID)

	diskContentInfo.BlockCrc32s[0] = confirmedDiskContentInfoCrc32
	err = dstZoneNBSClient.ValidateCrc32(
		ctx,
		params.DiskID,
		diskContentInfo,
	)
	logging.Info(
		ctx,
		"Error for confirmed disk content info crc32 %+v: %+v",
		confirmedDiskContentInfoCrc32,
		err,
	)

	if err != nil {
		logging.Info(
			ctx,
			"Trying to validate unconfirmed disk content info crc32 %+v",
			unconfirmedDiskContentInfoCrc32,
		)
		diskContentInfo.BlockCrc32s[0] = unconfirmedDiskContentInfoCrc32
		err = dstZoneNBSClient.ValidateCrc32(
			ctx,
			params.DiskID,
			diskContentInfo,
		)
		logging.Info(
			ctx,
			"Error for unconfirmed disk content info crc32 %+v: %+v",
			unconfirmedDiskContentInfoCrc32,
			err,
		)

		require.NoError(t, err)
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
