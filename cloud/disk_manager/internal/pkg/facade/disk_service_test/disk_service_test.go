package disk_service_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceCreateEmptyDisk(t *testing.T) {
	testDiskServiceCreateEmptyDiskWithZoneID(t, defaultZoneID)
}

func TestDiskServiceShouldCreateSsdNonreplIfFolderIsInAllowedList(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: 262144 * 4096,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		FolderId: "another-folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceShouldFailToCreateSsdNonreplIfNotAllowed(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: 262144 * 4096,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		FolderId: "unallowed",
	})
	require.Error(t, err)
	require.Empty(t, operation)
	require.ErrorContains(t, err, "not allowed for the \"unallowed\" folder")

	testcommon.CheckConsistency(t, ctx)
}

// NBS-3424: TODO: enable this test.
func TestDiskServiceShouldFailCreateDiskFromNonExistingImage(t *testing.T) {
	/*
		ctx := testcommon.NewContext()

		client, err := testcommon.NewClient(ctx)
		require.NoError(t, err)
		defer client.Close()

		diskID := t.Name()

		reqCtx := testcommon.GetRequestContext(t, ctx)
		operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
			Src: &disk_manager.CreateDiskRequest_SrcImageId{
				SrcImageId: "xxx",
			},
			Size: 134217728,
			Kind: disk_manager.DiskKind_DISK_KIND_SSD,
			DiskId: &disk_manager.DiskId{
				ZoneId: "zone-a",
				DiskId: diskID,
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, operation)

		err = internal_client.WaitOperation(ctx, client, operation.Id)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")

		testcommon.CheckErrorDetails(t, err, codes.BadSource, "", false)

		testcommon.CheckConsistency(t, ctx)
	*/
}

func TestDiskServiceCreateDiskFromImageWithForceNotLayered(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	imageID := t.Name()

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

	diskID := t.Name()

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcImageId{
			SrcImageId: imageID,
		},
		Size: 134217728,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		ForceNotLayered: true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, "zone-a")
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

func TestDiskServiceCancelCreateDiskFromImage(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	imageID := t.Name()
	imageSize := uint64(64 * 1024 * 1024)

	_ = testcommon.CreateImage(
		t,
		ctx,
		imageID,
		imageSize,
		"folder",
		true, // pooled
	)

	diskID := t.Name()
	diskSize := 2 * imageSize

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcImageId{
			SrcImageId: imageID,
		},
		Size: int64(diskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	testcommon.CancelOperation(t, ctx, client, operation.Id)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceDeleteDiskWhenCreationIsInFlight(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	imageID := t.Name()
	imageSize := uint64(64 * 1024 * 1024)

	_ = testcommon.CreateImage(
		t,
		ctx,
		imageID,
		imageSize,
		"folder",
		true, // pooled
	)

	// Need to add some variance for better testing.
	common.WaitForRandomDuration(time.Millisecond, 2*time.Second)

	diskID := t.Name()
	diskSize := 2 * imageSize

	reqCtx := testcommon.GetRequestContext(t, ctx)
	createOp, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcImageId{
			SrcImageId: imageID,
		},
		Size: int64(diskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, createOp)

	<-time.After(time.Second)

	testcommon.DeleteDisk(t, ctx, client, diskID)

	_ = internal_client.WaitOperation(ctx, client, createOp.Id)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceCreateDisksFromImageWithConfiguredPool(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	imageID := t.Name()
	imageSize := uint64(64 * 1024 * 1024)

	diskContentInfo := testcommon.CreateImage(
		t,
		ctx,
		imageID,
		imageSize,
		"folder",
		false, // pooled
	)

	privateClient, err := testcommon.NewPrivateClient(ctx)
	require.NoError(t, err)
	defer privateClient.Close()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := privateClient.ConfigurePool(reqCtx, &api.ConfigurePoolRequest{
		ImageId:      imageID,
		ZoneId:       "zone-a",
		Capacity:     12,
		UseImageSize: true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	diskSize := 2 * imageSize

	var operations []*disk_manager.Operation
	for i := 0; i < 20; i++ {
		diskID := fmt.Sprintf("%v%v", t.Name(), i)

		reqCtx = testcommon.GetRequestContext(t, ctx)
		operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
			Src: &disk_manager.CreateDiskRequest_SrcImageId{
				SrcImageId: imageID,
			},
			Size: int64(diskSize),
			Kind: disk_manager.DiskKind_DISK_KIND_SSD,
			DiskId: &disk_manager.DiskId{
				ZoneId: "zone-a",
				DiskId: diskID,
			},
			EncryptionDesc: &disk_manager.EncryptionDesc{},
		})
		require.NoError(t, err)
		require.NotEmpty(t, operation)
		operations = append(operations, operation)
	}

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, "zone-a")

	for i, operation := range operations {
		err := internal_client.WaitOperation(ctx, client, operation.Id)
		require.NoError(t, err)

		diskID := fmt.Sprintf("%v%v", t.Name(), i)
		err = nbsClient.ValidateCrc32(
			ctx,
			diskID,
			diskContentInfo,
		)
		require.NoError(t, err)

		diskParams, err := nbsClient.Describe(ctx, diskID)
		require.NoError(t, err)
		require.NotEmpty(t, diskParams.BaseDiskID)
	}

	operations = nil
	for i := 0; i < 20; i++ {
		diskID := fmt.Sprintf("%v%v", t.Name(), i)

		reqCtx = testcommon.GetRequestContext(t, ctx)
		operation, err = client.DeleteDisk(reqCtx, &disk_manager.DeleteDiskRequest{
			DiskId: &disk_manager.DiskId{
				DiskId: diskID,
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, operation)
		operations = append(operations, operation)
	}

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.DeleteImage(reqCtx, &disk_manager.DeleteImageRequest{
		ImageId: imageID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	operations = append(operations, operation)

	for _, operation := range operations {
		err := internal_client.WaitOperation(ctx, client, operation.Id)
		require.NoError(t, err)
	}

	testcommon.CheckConsistency(t, ctx)
}

func testCreateDiskFromIncrementalSnapshot(
	t *testing.T,
	diskKind disk_manager.DiskKind,
	diskSize uint64,
) {

	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID1 := t.Name() + "1"

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: int64(diskSize),
		Kind: diskKind,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID1,
		},
		FolderId: "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, "zone-a")
	_, err = nbsClient.FillDisk(ctx, diskID1, diskSize)
	require.NoError(t, err)

	snapshotID1 := t.Name() + "1"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: "zone-a",
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

	snapshotID2 := t.Name() + "2"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID1,
		},
		SnapshotId: snapshotID2,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	diskID2 := t.Name() + "2"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcSnapshotId{
			SrcSnapshotId: snapshotID2,
		},
		Size: int64(diskSize),
		Kind: diskKind,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
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

	err = nbsClient.ValidateCrc32(ctx, diskID2, diskContentInfo)
	require.NoError(t, err)

	testcommon.DeleteDisk(t, ctx, client, diskID1)
	testcommon.DeleteDisk(t, ctx, client, diskID2)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceCreateDiskFromIncrementalSnapshot(t *testing.T) {
	testCreateDiskFromIncrementalSnapshot(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD,
		128*1024*1024,
	)
}

func TestDiskServiceCreateSsdNonreplDiskFromIncrementalSnapshot(t *testing.T) {
	testCreateDiskFromIncrementalSnapshot(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		262144*4096,
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceCreateDiskFromSnapshot(t *testing.T) {
	testDiskServiceCreateDiskFromSnapshotWithZoneID(t, defaultZoneID)
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceCreateDiskFromImage(t *testing.T) {
	testCreateDiskFromImage(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD,
		32*1024*4096, // imageSize
		false,        // pooled
		32*1024*4096, // diskSize
		"folder",
		nil, // encryptionDesc
		defaultZoneID,
	)
}

func TestDiskServiceCreateSsdNonreplDiskFromPooledImage(t *testing.T) {
	testCreateDiskFromImage(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		32*1024*4096, // imageSize
		true,         // pooled
		262144*4096,  // diskSize
		"folder",
		nil, // encryptionDesc
		defaultZoneID,
	)
}

/*
// TODO: enable after issue #3071 has been completed
func TestDiskServiceCreateSsdNonreplDiskWithDefaultEncryptionFromPooledImage(
	t *testing.T,
) {
	testCreateDiskFromImage(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		32*1024*4096, // imageSize
		true,         // pooled
		262144*4096,  // diskSize
		"encrypted-folder",
		nil, // encryptionDesc
	)
}

func TestDiskServiceCreateEncryptedSsdNonreplDiskFromPooledImage(t *testing.T) {
	testCreateDiskFromImage(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		32*1024*4096, // imageSize
		true,         // pooled
		262144*4096,  // diskSize
		"folder",
		&disk_manager.EncryptionDesc{
			Mode: disk_manager.EncryptionMode_ENCRYPTION_AES_XTS,
			Key: &disk_manager.EncryptionDesc_KmsKey{
				KmsKey: &disk_manager.KmsKey{
					KekId:        "kekid",
					EncryptedDek: []byte("encrypteddek"),
					TaskId:       "taskid",
				},
			},
		},
	)
}
*/

func TestDiskServiceCreateEncryptedSsdNonreplDiskFromImage(t *testing.T) {
	testCreateDiskFromImage(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		32*1024*4096, // imageSize
		false,        // pooled
		262144*4096,  // diskSize
		"folder",
		&disk_manager.EncryptionDesc{
			Mode: disk_manager.EncryptionMode_ENCRYPTION_AES_XTS,
			Key: &disk_manager.EncryptionDesc_KmsKey{
				KmsKey: &disk_manager.KmsKey{
					KekId:        "kekid",
					EncryptedDek: []byte("encrypteddek"),
					TaskId:       "taskid",
				},
			},
		},
		defaultZoneID,
	)
}

func TestDiskServiceCreateSsdNonreplDiskWithDefaultEncryptionFromImage(
	t *testing.T,
) {

	testCreateDiskFromImage(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		32*1024*4096, // imageSize
		false,        // pooled
		262144*4096,  // diskSize
		"encrypted-folder",
		nil, // encryptionDesc
		defaultZoneID,
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceCreateDiskFromSnapshotOfOverlayDisk(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	imageSize := uint64(64 * 1024 * 1024)

	diskID1 := t.Name() + "1"

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: int64(imageSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID1,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, "zone-a")

	diskContentInfo, err := nbsClient.FillDisk(ctx, diskID1, imageSize)
	require.NoError(t, err)

	imageID := t.Name() + "_image"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateImage(reqCtx, &disk_manager.CreateImageRequest{
		Src: &disk_manager.CreateImageRequest_SrcDiskId{
			SrcDiskId: &disk_manager.DiskId{
				ZoneId: "zone-a",
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
		ZoneId:       "zone-a",
		Capacity:     1,
		UseImageSize: true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	largeDiskSize := uint64(128 * 1024 * 1024 * 1024)
	diskID2 := t.Name() + "2"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcImageId{
			SrcImageId: imageID,
		},
		Size: int64(largeDiskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID2,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	snapshotID := t.Name() + "_snapshot"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID2,
		},
		SnapshotId: snapshotID,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	diskID3 := t.Name() + "3"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcSnapshotId{
			SrcSnapshotId: snapshotID,
		},
		Size: int64(largeDiskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
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

	err = nbsClient.ValidateCrc32(ctx, diskID3, diskContentInfo)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceCreateZonalTaskInAnotherZone(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskSize := uint64(32 * 1024)
	diskID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: int64(diskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "no_dataplane",
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	snapshotID := t.Name()

	// zoneId of DM test server is "zone-a", dataPlane requests for another zone shouldn't be executed
	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: "no_dataplane",
			DiskId: diskID,
		},
		SnapshotId: snapshotID,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = internal_client.WaitOperation(opCtx, client, operation.Id)
	require.Error(t, err)
	require.Contains(t, err.Error(), "context deadline exceeded")

	imageID := t.Name() + "_image"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateImage(reqCtx, &disk_manager.CreateImageRequest{
		Src: &disk_manager.CreateImageRequest_SrcDiskId{
			SrcDiskId: &disk_manager.DiskId{
				ZoneId: "no_dataplane",
				DiskId: diskID,
			},
		},
		DstImageId: imageID,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	opCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = internal_client.WaitOperation(opCtx, client, operation.Id)
	require.Error(t, err)
	require.Contains(t, err.Error(), "context deadline exceeded")

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceResizeDisk(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: 4096,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.ResizeDisk(reqCtx, &disk_manager.ResizeDiskRequest{
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		Size: 40960,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceAlterDisk(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: 4096,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		CloudId:  "cloud",
		FolderId: "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.AlterDisk(reqCtx, &disk_manager.AlterDiskRequest{
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		CloudId:  "newCloud",
		FolderId: "newFolder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceAssignDisk(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: 4096,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.AssignDisk(reqCtx, &disk_manager.AssignDiskRequest{
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		InstanceId: "InstanceId",
		Host:       "Host",
		Token:      "Token",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceDescribeDiskModel(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	model, err := client.DescribeDiskModel(ctx, &disk_manager.DescribeDiskModelRequest{
		ZoneId:        "zone-a",
		BlockSize:     4096,
		Size:          1000000 * 4096,
		Kind:          disk_manager.DiskKind_DISK_KIND_SSD,
		TabletVersion: 1,
	})
	require.NoError(t, err)
	require.Equal(t, int64(4096), model.BlockSize)
	require.Equal(t, int64(1000000*4096), model.Size)
	require.Equal(t, disk_manager.DiskKind_DISK_KIND_SSD, model.Kind)

	model, err = client.DescribeDiskModel(ctx, &disk_manager.DescribeDiskModelRequest{
		BlockSize:     4096,
		Size:          1000000 * 4096,
		Kind:          disk_manager.DiskKind_DISK_KIND_SSD,
		TabletVersion: 1,
	})
	require.NoError(t, err)
	require.Equal(t, int64(4096), model.BlockSize)
	require.Equal(t, int64(1000000*4096), model.Size)
	require.Equal(t, disk_manager.DiskKind_DISK_KIND_SSD, model.Kind)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceCreateEncryptedDiskFromSnapshot(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskSize := uint64(32 * 1024 * 4096)
	diskID1 := t.Name() + "1"
	encryption := &disk_manager.EncryptionDesc{
		Mode: disk_manager.EncryptionMode_ENCRYPTION_AES_XTS,
		Key: &disk_manager.EncryptionDesc_KmsKey{
			KmsKey: &disk_manager.KmsKey{
				KekId:        "kekid",
				EncryptedDek: []byte("encrypteddek"),
				TaskId:       "taskid",
			},
		},
	}

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: int64(diskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID1,
		},
		EncryptionDesc: &disk_manager.EncryptionDesc{
			Mode: disk_manager.EncryptionMode_ENCRYPTION_AES_XTS,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, "zone-a")
	diskParams1, err := nbsClient.Describe(ctx, diskID1)
	require.NoError(t, err)

	require.Equal(t, types.EncryptionMode_ENCRYPTION_AES_XTS, diskParams1.EncryptionDesc.Mode)
	diskKeyHash1, err := testcommon.GetEncryptionKeyHash(diskParams1.EncryptionDesc)
	require.NoError(t, err)
	require.Equal(t, []byte(nil), diskKeyHash1)

	encryptionDesc, err := disks.PrepareEncryptionDesc(encryption)
	require.NoError(t, err)

	diskContentInfo, err := nbsClient.FillEncryptedDisk(
		ctx,
		diskID1,
		diskSize,
		encryptionDesc,
	)
	require.NoError(t, err)

	diskParams1, err = nbsClient.Describe(ctx, diskID1)
	require.NoError(t, err)

	require.Equal(t, types.EncryptionMode_ENCRYPTION_AES_XTS, diskParams1.EncryptionDesc.Mode)
	diskKeyHash1, err = testcommon.GetEncryptionKeyHash(diskParams1.EncryptionDesc)
	require.NoError(t, err)
	require.Equal(t, testcommon.DefaultKeyHash, diskKeyHash1)

	snapshotID := t.Name()

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID1,
		},
		SnapshotId: snapshotID,
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

	diskID2 := t.Name() + "2"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcSnapshotId{
			SrcSnapshotId: snapshotID,
		},
		Size: int64(diskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID2,
		},
		EncryptionDesc: &disk_manager.EncryptionDesc{
			Mode: disk_manager.EncryptionMode_ENCRYPTION_AES_XTS,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	diskParams2, err := nbsClient.Describe(ctx, diskID2)
	require.NoError(t, err)

	require.Equal(t, types.EncryptionMode_ENCRYPTION_AES_XTS, diskParams2.EncryptionDesc.Mode)
	diskKeyHash2, err := testcommon.GetEncryptionKeyHash(diskParams2.EncryptionDesc)
	require.NoError(t, err)
	require.Equal(t, testcommon.DefaultKeyHash, diskKeyHash2)

	diskMeta := disk_manager.CreateDiskMetadata{}
	err = internal_client.GetOperationMetadata(ctx, client, operation.Id, &diskMeta)
	require.NoError(t, err)
	require.Equal(t, float64(1), diskMeta.Progress)

	diskID3 := t.Name() + "3"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcSnapshotId{
			SrcSnapshotId: snapshotID,
		},
		Size: int64(diskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID3,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.Error(t, err)
	require.ErrorContains(t, err, "encryption mode should be the same")

	err = nbsClient.ValidateCrc32WithEncryption(
		ctx,
		diskID1,
		diskContentInfo,
		encryptionDesc,
	)
	require.NoError(t, err)
	err = nbsClient.ValidateCrc32WithEncryption(
		ctx,
		diskID2,
		diskContentInfo,
		encryptionDesc,
	)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceCreateEncryptedDiskFromImage(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	imageID := t.Name() + "_image"

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateImage(reqCtx, &disk_manager.CreateImageRequest{
		Src: &disk_manager.CreateImageRequest_SrcUrl{
			SrcUrl: &disk_manager.ImageUrl{
				Url: testcommon.GetRawImageFileURL(),
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

	diskID := t.Name()

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcImageId{
			SrcImageId: imageID,
		},
		Size: 134217728,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		ForceNotLayered: true,
		EncryptionDesc: &disk_manager.EncryptionDesc{
			Mode: disk_manager.EncryptionMode_ENCRYPTION_AES_XTS,
			Key: &disk_manager.EncryptionDesc_KmsKey{
				KmsKey: &disk_manager.KmsKey{
					KekId:        "kekid",
					EncryptedDek: []byte("encrypteddek"),
					TaskId:       "taskid",
				},
			},
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func testCreateSsdNonreplWithEncryptionAtRest(
	t *testing.T,
	folderID string,
	encryptionDesc *disk_manager.EncryptionDesc,
) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: 262144 * 4096,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		FolderId:       folderID,
		EncryptionDesc: encryptionDesc,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, "zone-a")

	session, err := nbsClient.MountRO(
		ctx,
		diskID,
		nil, // encryption
	)
	require.NoError(t, err)

	encryption, err := session.EncryptionDesc()
	require.NoError(t, err)

	require.NotEmpty(t, encryption)
	require.Equal(t, types.EncryptionMode_ENCRYPTION_AT_REST, encryption.Mode)

	key := encryption.Key.(*types.EncryptionDesc_KmsKey)
	require.NotEmpty(t, key)
	require.NotEmpty(t, key.KmsKey.KekId)
	require.NotEmpty(t, key.KmsKey.EncryptedDEK)

	session.Close(ctx)

	testcommon.DeleteDisk(t, ctx, client, diskID)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceShouldCreateSsdNonreplWithEncryptionAtRestByEncryptionDesc(
	t *testing.T,
) {
	testCreateSsdNonreplWithEncryptionAtRest(
		t,
		"folder",
		&disk_manager.EncryptionDesc{
			Mode: disk_manager.EncryptionMode_ENCRYPTION_AT_REST,
		},
	)
}

func TestDiskServiceShouldCreateSsdNonreplWithEncryptionAtRestByFolderID(
	t *testing.T,
) {
	testCreateSsdNonreplWithEncryptionAtRest(
		t,
		"encrypted-folder",
		nil, // encryptionDesc
	)
}
