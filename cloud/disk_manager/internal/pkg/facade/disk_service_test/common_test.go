package disk_service_test

import (
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks"
)

////////////////////////////////////////////////////////////////////////////////

const (
	defaultZoneId = "zone-a"
	shardedZoneId = "zone-d"
	shardId1      = "zone-d-1"
	shardId2      = "zone-d-2"
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

func testDiskServiceCreateDiskFromImageWithForceNotLayeredWithZoneID(
	t *testing.T,
	zoneID string,
) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	imageID := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t)

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateImage(reqCtx, &disk_manager.CreateImageRequest{
		Src: &disk_manager.CreateImageRequest_SrcUrl{
			SrcUrl: &disk_manager.ImageUrl{
				Url: testcommon.GetRawImageFileURL(),
			},
		},
		DstImageId: imageID,
		FolderId:   "folder",
		Pooled:     true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	diskID := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcImageId{
			SrcImageId: imageID,
		},
		Size: 134217728,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: zoneID,
			DiskId: diskID,
		},
		ForceNotLayered: true,
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
	err = nbsClient.ValidateCrc32(
		ctx,
		diskID,
		nbs.DiskContentInfo{
			ContentSize: testcommon.GetRawImageSize(t),
			Crc32:       testcommon.GetRawImageCrc32(t),
			BlockCrc32s: []uint32{}, // We do not know blockCrc32s for image.
		},
	)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func testDiskServiceCancelCreateDiskFromImageWithZoneID(
	t *testing.T,
	zoneID string,
) {

	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	imageID := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t)
	imageSize := uint64(64 * 1024 * 1024)

	_ = testcommon.CreateImage(
		t,
		ctx,
		imageID,
		imageSize,
		"folder",
		true, // pooled
	)

	diskID := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t)
	diskSize := 2 * imageSize

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcImageId{
			SrcImageId: imageID,
		},
		Size: int64(diskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: zoneID,
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	testcommon.CancelOperation(t, ctx, client, operation.Id)

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
			ZoneId: defaultZoneId,
			DiskId: diskID1,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, defaultZoneId)
	diskContentInfo, err := nbsClient.FillDisk(ctx, diskID1, diskSize)
	require.NoError(t, err)

	snapshotID1 := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t) + "1"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: defaultZoneId,
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

func testCreateDiskFromIncrementalSnapshot(
	t *testing.T,
	diskKind disk_manager.DiskKind,
	diskSize uint64,
	zoneID string,
) {

	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID1 := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t) + "1"

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: int64(diskSize),
		Kind: diskKind,
		DiskId: &disk_manager.DiskId{
			ZoneId: defaultZoneId,
			DiskId: diskID1,
		},
		FolderId: "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, defaultZoneId)
	_, err = nbsClient.FillDisk(ctx, diskID1, diskSize)
	require.NoError(t, err)

	snapshotID1 := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t) + "1"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: defaultZoneId,
			DiskId: diskID1,
		},
		SnapshotId: snapshotID1,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	_, err = nbsClient.FillDisk(ctx, diskID1, diskSize)
	require.NoError(t, err)

	snapshotID2 := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t) + "2"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: defaultZoneId,
			DiskId: diskID1,
		},
		SnapshotId: snapshotID2,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	diskID2 := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t) + "2"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcSnapshotId{
			SrcSnapshotId: snapshotID2,
		},
		Size: int64(diskSize),
		Kind: diskKind,
		DiskId: &disk_manager.DiskId{
			ZoneId: zoneID,
			DiskId: diskID2,
		},
		FolderId: "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	diskContentInfo, err := nbsClient.CalculateCrc32(diskID1, diskSize)
	require.NoError(t, err)

	diskMeta, err := testcommon.GetDiskMeta(ctx, diskID2)
	require.NoError(t, err)
	// We should provide correct zone for NBS client because only unsharded
	// zones and shards are configured in the NBS client config.
	nbsClient = testcommon.NewNbsTestingClient(t, ctx, diskMeta.ZoneID)
	err = nbsClient.ValidateCrc32(ctx, diskID2, diskContentInfo)
	require.NoError(t, err)

	testcommon.DeleteDisk(t, ctx, client, diskID1)
	testcommon.DeleteDisk(t, ctx, client, diskID2)

	testcommon.CheckConsistency(t, ctx)
}

func testDiskServiceCreateDiskFromSnapshotOfOverlayDiskInZone(
	t *testing.T,
	zoneID string,
) {

	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	imageSize := uint64(64 * 1024 * 1024)

	diskID1 := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t)

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: int64(imageSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: defaultZoneId,
			DiskId: diskID1,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, defaultZoneId)

	diskContentInfo, err := nbsClient.FillDisk(ctx, diskID1, imageSize)
	require.NoError(t, err)

	imageID := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t) + "_image"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateImage(reqCtx, &disk_manager.CreateImageRequest{
		Src: &disk_manager.CreateImageRequest_SrcDiskId{
			SrcDiskId: &disk_manager.DiskId{
				ZoneId: defaultZoneId,
				DiskId: diskID1,
			},
		},
		DstImageId: imageID,
		FolderId:   "folder",
		Pooled:     false,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	privateClient, err := testcommon.NewPrivateClient(ctx)
	require.NoError(t, err)
	defer privateClient.Close()

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = privateClient.ConfigurePool(reqCtx, &api.ConfigurePoolRequest{
		ImageId:      imageID,
		ZoneId:       defaultZoneId,
		Capacity:     1,
		UseImageSize: true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	largeDiskSize := uint64(128 * 1024 * 1024 * 1024)
	diskID2 := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t) + "2"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcImageId{
			SrcImageId: imageID,
		},
		Size: int64(largeDiskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: defaultZoneId,
			DiskId: diskID2,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	snapshotID := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t) + "_snapshot"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: defaultZoneId,
			DiskId: diskID2,
		},
		SnapshotId: snapshotID,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	diskID3 := testcommon.ReplaceUnacceptableSymbolsFromResourceID(t) + "3"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcSnapshotId{
			SrcSnapshotId: snapshotID,
		},
		Size: int64(largeDiskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: zoneID,
			DiskId: diskID3,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	err = nbsClient.ValidateCrc32(ctx, diskID1, diskContentInfo)
	require.NoError(t, err)

	err = nbsClient.ValidateCrc32(ctx, diskID2, diskContentInfo)
	require.NoError(t, err)

	diskMeta, err := testcommon.GetDiskMeta(ctx, diskID3)
	require.NoError(t, err)
	// We should provide correct zone for NBS client because only unsharded
	// zones and shards are configured in the NBS client config.
	nbsClient = testcommon.NewNbsTestingClient(t, ctx, diskMeta.ZoneID)
	err = nbsClient.ValidateCrc32(ctx, diskID3, diskContentInfo)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}
