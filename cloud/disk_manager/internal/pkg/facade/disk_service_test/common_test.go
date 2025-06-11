package disk_service_test

import (
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks"
)

////////////////////////////////////////////////////////////////////////////////

const (
	defaultZoneID = "zone-a"
	shardedZoneID = "zone-d"
	cellID1       = "zone-d"
	cellID2       = "zone-d-shard1"
)

////////////////////////////////////////////////////////////////////////////////

func testDiskServiceCreateEmptyDiskWithZoneID(t *testing.T, zoneID string) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t)

	reqCtx := testcommon.GetRequestContext(t, ctx)
	request := disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: 4096,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: zoneID,
			DiskId: diskID,
		},
	}

	operation, err := client.CreateDisk(reqCtx, &request)
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	// Check idempotency.
	operation, err = client.CreateDisk(reqCtx, &request)
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

////////////////////////////////////////////////////////////////////////////////

func testCreateDiskFromImageWithZoneID(
	t *testing.T,
	diskKind disk_manager.DiskKind,
	imageSize uint64,
	pooled bool,
	diskSize uint64,
	diskFolderID string,
	encryptionDesc *disk_manager.EncryptionDesc,
	zoneID string,
) {

	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	imageID := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t) + "_image"

	diskContentInfo := testcommon.CreateImage(
		t,
		ctx,
		imageID,
		imageSize,
		"folder",
		pooled,
	)

	diskID := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t)

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcImageId{
			SrcImageId: imageID,
		},
		Size: int64(diskSize),
		Kind: diskKind,
		DiskId: &disk_manager.DiskId{
			ZoneId: zoneID,
			DiskId: diskID,
		},
		FolderId:       diskFolderID,
		EncryptionDesc: encryptionDesc,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	diskMeta, err := testcommon.GetDiskMeta(ctx, diskID)
	require.NoError(t, err)
	// We should provide correct zone for NBS client because only unsharded
	// zones and shards are configured in the NBS client config.
	nbsClient := testcommon.NewNbsTestingClient(t, ctx, diskMeta.ZoneID)

	if encryptionDesc != nil {
		encryption, err := disks.PrepareEncryptionDesc(encryptionDesc)
		require.NoError(t, err)

		err = nbsClient.ValidateCrc32WithEncryption(
			ctx,
			diskID,
			diskContentInfo,
			encryption,
		)
		require.NoError(t, err)
	} else {
		err = nbsClient.ValidateCrc32(
			ctx,
			diskID,
			diskContentInfo,
		)
		require.NoError(t, err)
	}

	diskParams, err := nbsClient.Describe(ctx, diskID)
	require.NoError(t, err)
	if pooled {
		// Check that disk is overlay.
		require.NotEmpty(t, diskParams.BaseDiskID)
	} else {
		// Check that disk is not overlay.
		require.Empty(t, diskParams.BaseDiskID)
	}

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.DeleteDisk(reqCtx, &disk_manager.DeleteDiskRequest{
		DiskId: &disk_manager.DiskId{
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

////////////////////////////////////////////////////////////////////////////////

func testDiskServiceCreateDiskFromSnapshotWithZoneID(
	t *testing.T,
	zoneID string,
) {

	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskSize := uint64(32 * 1024 * 4096)
	diskID1 := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t) + "1"

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: int64(diskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: defaultZoneID,
			DiskId: diskID1,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, defaultZoneID)
	diskContentInfo, err := nbsClient.FillDisk(ctx, diskID1, diskSize)
	require.NoError(t, err)

	snapshotID1 := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t) + "1"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: defaultZoneID,
			DiskId: diskID1,
		},
		SnapshotId: snapshotID1,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	snapshotMeta := disk_manager.CreateSnapshotMetadata{}
	err = internal_client.GetOperationMetadata(ctx, client, operation.Id, &snapshotMeta)
	require.NoError(t, err)
	require.Equal(t, float64(1), snapshotMeta.Progress)

	diskID2 := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t) + "2"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcSnapshotId{
			SrcSnapshotId: snapshotID1,
		},
		Size: int64(diskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: zoneID,
			DiskId: diskID2,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	createDiskMeta := disk_manager.CreateDiskMetadata{}
	err = internal_client.GetOperationMetadata(
		ctx,
		client,
		operation.Id,
		&createDiskMeta,
	)
	require.NoError(t, err)
	require.Equal(t, float64(1), createDiskMeta.Progress)

	err = nbsClient.ValidateCrc32(ctx, diskID1, diskContentInfo)
	require.NoError(t, err)

	diskMeta, err := testcommon.GetDiskMeta(ctx, diskID2)
	require.NoError(t, err)
	// We should provide correct zone for NBS client because only unsharded
	// zones and shards are configured in the NBS client config.
	nbsClient = testcommon.NewNbsTestingClient(t, ctx, diskMeta.ZoneID)
	err = nbsClient.ValidateCrc32(ctx, diskID2, diskContentInfo)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}
