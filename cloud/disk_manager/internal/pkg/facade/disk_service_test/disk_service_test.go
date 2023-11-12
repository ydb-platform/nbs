package disk_service_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
	operation_proto "github.com/ydb-platform/nbs/cloud/disk_manager/api/operation"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

const migrationTestsDiskSize = 4096 * 4096

type migrationTestParams struct {
	SrcZoneID string
	DstZoneID string
	DiskID    string
	DiskKind  disk_manager.DiskKind
	DiskSize  int64
	FillDisk  bool
}

func setupMigrationTest(
	t *testing.T,
	params migrationTestParams,
) (context.Context, sdk_client.Client) {

	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: params.DiskSize,
		Kind: params.DiskKind,
		DiskId: &disk_manager.DiskId{
			DiskId: params.DiskID,
			ZoneId: params.SrcZoneID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	if params.FillDisk {
		nbsClient := testcommon.NewNbsClient(t, ctx, params.SrcZoneID)
		_, _, err = testcommon.FillDisk(
			nbsClient,
			params.DiskID,
			uint64(params.DiskSize),
		)
		require.NoError(t, err)
	}

	return ctx, client
}

func successfullyMigrateDisk(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	params migrationTestParams,
) {

	srcZoneNBSClient := testcommon.NewNbsClient(t, ctx, params.SrcZoneID)

	// Writing some additional data to disk in parallel with migration.
	waitForWrite, err := testcommon.GoWriteRandomBlocksToNbsDisk(
		ctx,
		srcZoneNBSClient,
		params.DiskID,
	)
	require.NoError(t, err)

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.MigrateDisk(reqCtx, &disk_manager.MigrateDiskRequest{
		DiskId: &disk_manager.DiskId{
			DiskId: params.DiskID,
			ZoneId: params.SrcZoneID,
		},
		DstZoneId: params.DstZoneID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	waitForMigrationStatus(t, ctx, client, operation, disk_manager.MigrateDiskMetadata_REPLICATING)

	err = waitForWrite()
	require.NoError(t, err)

	// TODO: NBS-4487 - uncomment this code.
	/* writeCtx, cancelWrite := context.WithCancel(ctx)
	waitForWrite, err = testcommon.GoWriteRandomBlocksToNbsDisk(
		writeCtx,
		srcZoneNBSClient,
		params.DiskID,
	)
	require.NoError(t, err)
	defer func() {
		cancelWrite()
		_ = waitForWrite()
	}() */

	err = client.SendMigrationSignal(ctx, &disk_manager.SendMigrationSignalRequest{
		OperationId: operation.Id,
		Signal:      disk_manager.SendMigrationSignalRequest_FINISH_REPLICATION,
	})
	require.NoError(t, err)

	waitForMigrationStatus(t, ctx, client, operation, disk_manager.MigrateDiskMetadata_REPLICATION_FINISHED)

	diskSize := uint64(params.DiskSize)

	srcCrc32, err := srcZoneNBSClient.CalculateCrc32(params.DiskID, diskSize)
	require.NoError(t, err)

	err = client.SendMigrationSignal(ctx, &disk_manager.SendMigrationSignalRequest{
		OperationId: operation.Id,
		Signal:      disk_manager.SendMigrationSignalRequest_FINISH_REPLICATION,
	})
	require.NoError(t, err, "migration signal should be idempotent")

	metadata := &disk_manager.MigrateDiskMetadata{}
	err = internal_client.GetOperationMetadata(ctx, client, operation.Id, metadata)
	require.NoError(t, err)
	require.Equal(t, float64(1), metadata.Progress)
	require.Equal(t, int64(0), metadata.SecondsRemaining)

	err = client.SendMigrationSignal(ctx, &disk_manager.SendMigrationSignalRequest{
		OperationId: operation.Id,
		Signal:      disk_manager.SendMigrationSignalRequest_FINISH_MIGRATION,
	})
	require.NoError(t, err)

	waitForMigrationStatus(t, ctx, client, operation, disk_manager.MigrateDiskMetadata_FINISHED)

	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	_, err = srcZoneNBSClient.Describe(ctx, params.DiskID)
	require.Error(t, err)
	require.ErrorContains(t, err, "Path not found")

	dstZoneNBSClient := testcommon.NewNbsClient(t, ctx, params.DstZoneID)
	dstCrc32, err := dstZoneNBSClient.CalculateCrc32(params.DiskID, diskSize)
	require.NoError(t, err)
	require.Equal(t, srcCrc32, dstCrc32)
}

func startAndCancelMigration(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	srcDiskID string,
	srcZoneID string,
	dstZoneID string,
) {

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.MigrateDisk(reqCtx, &disk_manager.MigrateDiskRequest{
		DiskId: &disk_manager.DiskId{
			DiskId: srcDiskID,
			ZoneId: srcZoneID,
		},
		DstZoneId: dstZoneID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	// Need to add some variance for better testing.
	testcommon.WaitForRandomDuration(time.Millisecond, time.Second)

	_, err = client.CancelOperation(ctx, &disk_manager.CancelOperationRequest{
		OperationId: operation.Id,
	})
	require.NoError(t, err)
}

// TODO: NBS-4487 - uncomment this code.
/*
func checkFreezeAndUnfreezeDuringMigration(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	srcDiskID string,
	srcZoneID string,
	dstZoneID string,
) {

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.MigrateDisk(reqCtx, &disk_manager.MigrateDiskRequest{
		DiskId: &disk_manager.DiskId{
			DiskId: srcDiskID,
			ZoneId: srcZoneID,
		},
		DstZoneId: dstZoneID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	waitForMigrationStatus(t, ctx, client, operation, disk_manager.MigrateDiskMetadata_REPLICATING)

	err = client.SendMigrationSignal(ctx, &disk_manager.SendMigrationSignalRequest{
		OperationId: operation.Id,
		Signal:      disk_manager.SendMigrationSignalRequest_FINISH_REPLICATION,
	})
	require.NoError(t, err)

	waitForMigrationStatus(t, ctx, client, operation, disk_manager.MigrateDiskMetadata_REPLICATION_FINISHED)

	srcZoneNBSClient := testcommon.NewNbsClient(t, ctx, srcZoneID)

	waitForWrite, err := testcommon.GoWriteRandomBlocksToNbsDisk(
		ctx,
		srcZoneNBSClient,
		srcDiskID,
	)
	require.NoError(t, err)

	err = waitForWrite()
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))

	_, err = client.CancelOperation(ctx, &disk_manager.CancelOperationRequest{
		OperationId: operation.Id,
	})
	require.NoError(t, err)

	waitForWrite, err = testcommon.GoWriteRandomBlocksToNbsDisk(
		ctx,
		srcZoneNBSClient,
		srcDiskID,
	)
	require.NoError(t, err)

	err = waitForWrite()
	require.NoError(t, err)

	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.Error(t, err)
	require.ErrorContains(t, err, "Cancelled by client")

	waitForMigrationStatus(t, ctx, client, operation, disk_manager.MigrateDiskMetadata_STATUS_UNSPECIFIED)
}
*/

func successfullyMigrateEmptyOverlayDisk(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	withAliveSrcImage bool,
) {

	imageID := t.Name()
	diskSize := migrationTestsDiskSize
	imageSize := diskSize / 2
	_, storageSize := testcommon.CreateImage(
		t,
		ctx,
		imageID,
		uint64(imageSize),
		"folder",
		true, // pooled
	)

	params := migrationTestParams{
		SrcZoneID: "zone",
		DstZoneID: "other",
		DiskID:    t.Name(),
		DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD,
		DiskSize:  migrationTestsDiskSize,
		FillDisk:  false,
	}

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcImageId{
			SrcImageId: imageID,
		},
		Size: params.DiskSize,
		Kind: params.DiskKind,
		DiskId: &disk_manager.DiskId{
			ZoneId: params.SrcZoneID,
			DiskId: params.DiskID,
		},
	})
	require.NoError(t, err)

	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	if !withAliveSrcImage {
		reqCtx = testcommon.GetRequestContext(t, ctx)
		operation, err = client.DeleteImage(reqCtx, &disk_manager.DeleteImageRequest{
			ImageId: imageID,
		})
		require.NoError(t, err)
		require.NotEmpty(t, operation)

		err = internal_client.WaitOperation(ctx, client, operation.Id)
		require.NoError(t, err)
	}

	srcZoneNBSClient := testcommon.NewNbsClient(t, ctx, params.SrcZoneID)
	changedBytes, err := srcZoneNBSClient.GetChangedBytes(
		ctx,
		params.DiskID,
		"",
		"",
		true, // ignoreBaseDisk
	)
	require.NoError(t, err)
	require.Equal(t, uint64(0), changedBytes)

	successfullyMigrateEmptyDisk(
		t,
		ctx,
		client,
		params,
	)

	dstZoneNBSClient := testcommon.NewNbsClient(t, ctx, params.DstZoneID)
	changedBytes, err = dstZoneNBSClient.GetChangedBytes(
		ctx,
		params.DiskID,
		"",
		"",
		true, // ignoreBaseDisk
	)
	require.NoError(t, err)

	if withAliveSrcImage {
		require.Equal(t, uint64(0), changedBytes)
	} else {
		require.Equal(t, storageSize, changedBytes)
	}
}

func successfullyMigrateEmptyDisk(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	params migrationTestParams,
) {

	srcZoneNBSClient := testcommon.NewNbsClient(t, ctx, params.SrcZoneID)

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.MigrateDisk(reqCtx, &disk_manager.MigrateDiskRequest{
		DiskId: &disk_manager.DiskId{
			DiskId: params.DiskID,
			ZoneId: params.SrcZoneID,
		},
		DstZoneId: params.DstZoneID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	waitForMigrationStatus(t, ctx, client, operation, disk_manager.MigrateDiskMetadata_REPLICATING)

	err = client.SendMigrationSignal(ctx, &disk_manager.SendMigrationSignalRequest{
		OperationId: operation.Id,
		Signal:      disk_manager.SendMigrationSignalRequest_FINISH_REPLICATION,
	})
	require.NoError(t, err)

	waitForMigrationStatus(t, ctx, client, operation, disk_manager.MigrateDiskMetadata_REPLICATION_FINISHED)

	diskSize := uint64(migrationTestsDiskSize)

	srcCrc32, err := srcZoneNBSClient.CalculateCrc32(params.DiskID, diskSize)
	require.NoError(t, err)

	metadata := &disk_manager.MigrateDiskMetadata{}
	err = internal_client.GetOperationMetadata(ctx, client, operation.Id, metadata)
	require.NoError(t, err)
	require.Equal(t, float64(1), metadata.Progress)
	require.Equal(t, int64(0), metadata.SecondsRemaining)

	err = client.SendMigrationSignal(ctx, &disk_manager.SendMigrationSignalRequest{
		OperationId: operation.Id,
		Signal:      disk_manager.SendMigrationSignalRequest_FINISH_MIGRATION,
	})
	require.NoError(t, err)

	waitForMigrationStatus(t, ctx, client, operation, disk_manager.MigrateDiskMetadata_FINISHED)

	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	_, err = srcZoneNBSClient.Describe(ctx, params.SrcZoneID)
	require.Error(t, err)
	require.ErrorContains(t, err, "Path not found")

	dstZoneNBSClient := testcommon.NewNbsClient(t, ctx, params.DstZoneID)
	dstCrc32, err := dstZoneNBSClient.CalculateCrc32(params.DiskID, diskSize)
	require.NoError(t, err)
	require.Equal(t, srcCrc32, dstCrc32)
}

func waitForMigrationStatus(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	operation *operation_proto.Operation,
	status disk_manager.MigrateDiskMetadata_Status,
) {

	for i := 0; ; i++ {
		_, err := internal_client.GetOperationResponse(ctx, client, operation.Id, nil)
		require.NoError(t, err)

		metadata := &disk_manager.MigrateDiskMetadata{}
		err = internal_client.GetOperationMetadata(ctx, client, operation.Id, metadata)
		require.NoError(t, err)

		if metadata.Status != status {
			if i%10 == 0 && i != 0 {
				logging.Info(
					ctx,
					"Still waiting for migration status %v, actual status is %v, request %v, progress %v, seconds_remaining: %v",
					status,
					metadata.Status,
					i,
					metadata.Progress,
					metadata.SecondsRemaining,
				)
			}
			time.Sleep(time.Second)
			continue
		}

		return
	}
}

func waitForMigrationStatusOrError(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	operation *operation_proto.Operation,
	status disk_manager.MigrateDiskMetadata_Status,
) {

	for i := 0; ; i++ {
		done, err := internal_client.GetOperationResponse(ctx, client, operation.Id, nil)
		if done && err != nil {
			return
		}

		require.NoError(t, err)

		metadata := &disk_manager.MigrateDiskMetadata{}
		err = internal_client.GetOperationMetadata(ctx, client, operation.Id, metadata)
		require.NoError(t, err)

		if metadata.Status != status {
			if i%10 == 0 && i != 0 {
				logging.Info(
					ctx,
					"Still waiting for migration status %v, actual status is %v, request %v, progress %v, seconds_remaining: %v",
					status,
					metadata.Status,
					i,
					metadata.Progress,
					metadata.SecondsRemaining,
				)
			}
			time.Sleep(time.Second)
			continue
		}

		return
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceCreateEmptyDisk(t *testing.T) {
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
			ZoneId: "zone",
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

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
				ZoneId: "zone",
				DiskId: diskID,
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, operation)

		err = internal_client.WaitOperation(ctx, client, operation.Id)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")

		status, ok := grpc_status.FromError(err)
		require.True(t, ok)

		statusDetails := status.Details()
		require.Equal(t, 1, len(statusDetails))

		errorDetails, ok := statusDetails[0].(*disk_manager.ErrorDetails)
		require.True(t, ok)

		require.Equal(t, int64(codes.BadSource), errorDetails.Code)
		require.False(t, errorDetails.Internal)

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
			ZoneId: "zone",
			DiskId: diskID,
		},
		ForceNotLayered: true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsClient(t, ctx, "zone")
	err = nbsClient.ValidateCrc32(
		diskID,
		testcommon.GetRawImageSize(t),
		testcommon.GetRawImageCrc32(t),
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

	_, _ = testcommon.CreateImage(
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
			ZoneId: "zone",
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

	_, _ = testcommon.CreateImage(
		t,
		ctx,
		imageID,
		imageSize,
		"folder",
		true, // pooled
	)

	// Need to add some variance for better testing.
	testcommon.WaitForRandomDuration(time.Millisecond, 2*time.Second)

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
			ZoneId: "zone",
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, createOp)

	<-time.After(time.Second)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err := client.DeleteDisk(reqCtx, &disk_manager.DeleteDiskRequest{
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone",
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

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

	imageCrc32, _ := testcommon.CreateImage(
		t,
		ctx,
		imageID,
		imageSize,
		"folder",
		false, // pooled
	)

	privateClient, err := testcommon.CreatePrivateClient(ctx)
	require.NoError(t, err)
	defer privateClient.Close()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := privateClient.ConfigurePool(reqCtx, &api.ConfigurePoolRequest{
		ImageId:      imageID,
		ZoneId:       "zone",
		Capacity:     12,
		UseImageSize: true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	diskSize := 2 * imageSize

	var operations []*operation_proto.Operation
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
				ZoneId: "zone",
				DiskId: diskID,
			},
			EncryptionDesc: &disk_manager.EncryptionDesc{},
		})
		require.NoError(t, err)
		require.NotEmpty(t, operation)
		operations = append(operations, operation)
	}

	nbsClient := testcommon.NewNbsClient(t, ctx, "zone")

	for i, operation := range operations {
		err := internal_client.WaitOperation(ctx, client, operation.Id)
		require.NoError(t, err)

		diskID := fmt.Sprintf("%v%v", t.Name(), i)
		err = nbsClient.ValidateCrc32(diskID, imageSize, imageCrc32)
		require.NoError(t, err)

		diskParams, err := nbsClient.Describe(ctx, diskID)
		require.NoError(t, err)
		require.NotEqual(t, "", diskParams.BaseDiskID)
	}

	operations = nil
	for i := 0; i < 20; i++ {
		diskID := fmt.Sprintf("%v%v", t.Name(), i)

		reqCtx = testcommon.GetRequestContext(t, ctx)
		operation, err = client.DeleteDisk(reqCtx, &disk_manager.DeleteDiskRequest{
			DiskId: &disk_manager.DiskId{
				ZoneId: "zone",
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
			ZoneId: "zone",
			DiskId: diskID1,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsClient(t, ctx, "zone")
	crc32, _, err := testcommon.FillDisk(nbsClient, diskID1, diskSize)
	require.NoError(t, err)

	snapshotID1 := t.Name() + "1"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: "zone",
			DiskId: diskID1,
		},
		SnapshotId: snapshotID1,
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
			SrcSnapshotId: snapshotID1,
		},
		Size: int64(diskSize),
		Kind: diskKind,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone",
			DiskId: diskID2,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	err = nbsClient.ValidateCrc32(diskID1, diskSize, crc32)
	require.NoError(t, err)

	err = nbsClient.ValidateCrc32(diskID2, diskSize, crc32)
	require.NoError(t, err)

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

func TestDiskServiceCreateDiskFromSnapshot(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskSize := uint64(32 * 1024 * 4096)
	diskID1 := t.Name() + "1"

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: int64(diskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone",
			DiskId: diskID1,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsClient(t, ctx, "zone")
	crc32, _, err := testcommon.FillDisk(nbsClient, diskID1, diskSize)
	require.NoError(t, err)

	snapshotID1 := t.Name() + "1"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: "zone",
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

	diskID2 := t.Name() + "2"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcSnapshotId{
			SrcSnapshotId: snapshotID1,
		},
		Size: int64(diskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone",
			DiskId: diskID2,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	diskMeta := disk_manager.CreateDiskMetadata{}
	err = internal_client.GetOperationMetadata(ctx, client, operation.Id, &diskMeta)
	require.NoError(t, err)
	require.Equal(t, float64(1), diskMeta.Progress)

	err = nbsClient.ValidateCrc32(diskID1, diskSize, crc32)
	require.NoError(t, err)

	err = nbsClient.ValidateCrc32(diskID2, diskSize, crc32)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceCreateDiskFromImage(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskSize := uint64(32 * 1024 * 4096)
	diskID1 := t.Name() + "1"

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: int64(diskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone",
			DiskId: diskID1,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsClient(t, ctx, "zone")
	crc32, _, err := testcommon.FillDisk(nbsClient, diskID1, diskSize)
	require.NoError(t, err)

	imageID := t.Name() + "_image"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateImage(reqCtx, &disk_manager.CreateImageRequest{
		Src: &disk_manager.CreateImageRequest_SrcDiskId{
			SrcDiskId: &disk_manager.DiskId{
				ZoneId: "zone",
				DiskId: diskID1,
			},
		},
		DstImageId: imageID,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	meta := disk_manager.CreateImageMetadata{}
	err = internal_client.GetOperationMetadata(ctx, client, operation.Id, &meta)
	require.NoError(t, err)
	require.Equal(t, float64(1), meta.Progress)

	diskID2 := t.Name() + "2"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcImageId{
			SrcImageId: imageID,
		},
		Size: int64(diskSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone",
			DiskId: diskID2,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	err = nbsClient.ValidateCrc32(diskID1, diskSize, crc32)
	require.NoError(t, err)

	err = nbsClient.ValidateCrc32(diskID2, diskSize, crc32)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

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
			ZoneId: "zone",
			DiskId: diskID1,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsClient(t, ctx, "zone")

	crc32, _, err := testcommon.FillDisk(nbsClient, diskID1, imageSize)
	require.NoError(t, err)

	imageID := t.Name() + "_image"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateImage(reqCtx, &disk_manager.CreateImageRequest{
		Src: &disk_manager.CreateImageRequest_SrcDiskId{
			SrcDiskId: &disk_manager.DiskId{
				ZoneId: "zone",
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

	privateClient, err := testcommon.CreatePrivateClient(ctx)
	require.NoError(t, err)
	defer privateClient.Close()

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = privateClient.ConfigurePool(reqCtx, &api.ConfigurePoolRequest{
		ImageId:      imageID,
		ZoneId:       "zone",
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
			ZoneId: "zone",
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
			ZoneId: "zone",
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
			ZoneId: "zone",
			DiskId: diskID3,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	err = nbsClient.ValidateCrc32(diskID1, imageSize, crc32)
	require.NoError(t, err)

	err = nbsClient.ValidateCrc32(diskID2, imageSize, crc32)
	require.NoError(t, err)

	err = nbsClient.ValidateCrc32(diskID3, imageSize, crc32)
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

	// zoneId of DM test server is "zone", dataPlane requests for another zone shouldn't be executed
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
			ZoneId: "zone",
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
			ZoneId: "zone",
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
			ZoneId: "zone",
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
			ZoneId: "zone",
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
			ZoneId: "zone",
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
			ZoneId: "zone",
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
		ZoneId:        "zone",
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

func TestDiskServiceUnauthorizedGetOperation(t *testing.T) {
	ctx := testcommon.NewContextWithToken("TestTokenDisksOnly")

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	// Creating disk should have completed successfully.
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: 4096,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone",
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	_, err = client.GetOperation(reqCtx, &disk_manager.GetOperationRequest{
		OperationId: operation.Id,
	})
	require.Error(t, err)
	require.Equalf(
		t,
		grpc_codes.PermissionDenied,
		grpc_status.Code(err),
		"actual error %v",
		err,
	)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceUnauthenticatedCreateDisk(t *testing.T) {
	ctx := testcommon.NewContextWithToken("NoTestToken")

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	_, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: 4096,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone",
			DiskId: diskID,
		},
	})
	require.Error(t, err)
	require.Equalf(
		t,
		grpc_codes.Unauthenticated,
		grpc_status.Code(err),
		"actual error %v",
		err,
	)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceMigrateNonreplDisk(t *testing.T) {
	params := migrationTestParams{
		SrcZoneID: "zone",
		DstZoneID: "other",
		DiskID:    t.Name(),
		DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		DiskSize:  1073741824,
		FillDisk:  false,
	}

	ctx, client := setupMigrationTest(t, params)
	defer client.Close()

	successfullyMigrateDisk(t, ctx, client, params)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceMigrateHddNonreplDisk(t *testing.T) {
	params := migrationTestParams{
		SrcZoneID: "zone",
		DstZoneID: "other",
		DiskID:    t.Name(),
		DiskKind:  disk_manager.DiskKind_DISK_KIND_HDD_NONREPLICATED,
		DiskSize:  1073741824,
		FillDisk:  false,
	}

	ctx, client := setupMigrationTest(t, params)
	defer client.Close()

	successfullyMigrateDisk(t, ctx, client, params)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceMigrateDisk(t *testing.T) {
	diskID := t.Name()
	params := migrationTestParams{
		SrcZoneID: "zone",
		DstZoneID: "other",
		DiskID:    diskID,
		DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD,
		DiskSize:  migrationTestsDiskSize,
		FillDisk:  true,
	}

	ctx, client := setupMigrationTest(t, params)
	defer client.Close()

	successfullyMigrateDisk(t, ctx, client, params)

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.DeleteDisk(reqCtx, &disk_manager.DeleteDiskRequest{
		DiskId: &disk_manager.DiskId{
			ZoneId: "any_string", // deprecated field
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	// Check that disk is deleted.
	srcZoneNBSClient := testcommon.NewNbsClient(t, ctx, params.SrcZoneID)
	_, err = srcZoneNBSClient.Describe(ctx, params.DiskID)
	require.Error(t, err)
	require.ErrorContains(t, err, "Path not found")

	dstZoneNBSClient := testcommon.NewNbsClient(t, ctx, params.DstZoneID)
	_, err = dstZoneNBSClient.Describe(ctx, params.DiskID)
	require.Error(t, err)
	require.ErrorContains(t, err, "Path not found")

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceCancelMigrateDisk(t *testing.T) {
	params := migrationTestParams{
		SrcZoneID: "zone",
		DstZoneID: "other",
		DiskID:    t.Name(),
		DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD,
		DiskSize:  migrationTestsDiskSize,
	}

	ctx, client := setupMigrationTest(t, params)
	defer client.Close()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.MigrateDisk(reqCtx, &disk_manager.MigrateDiskRequest{
		DiskId: &disk_manager.DiskId{
			DiskId: params.DiskID,
			ZoneId: params.SrcZoneID,
		},
		DstZoneId: params.DstZoneID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	waitForMigrationStatus(t, ctx, client, operation, disk_manager.MigrateDiskMetadata_REPLICATING)

	_, err = client.CancelOperation(ctx, &disk_manager.CancelOperationRequest{
		OperationId: operation.Id,
	})
	require.NoError(t, err)

	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.Error(t, err)
	require.ErrorContains(t, err, "Cancelled by client")
}

func TestDiskServiceMigrateDiskAfterCancel(t *testing.T) {
	params := migrationTestParams{
		SrcZoneID: "zone",
		DstZoneID: "other",
		DiskID:    t.Name(),
		DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD,
		DiskSize:  migrationTestsDiskSize,
		FillDisk:  true,
	}

	ctx, client := setupMigrationTest(t, params)
	defer client.Close()

	for i := 0; i < 30; i++ {
		startAndCancelMigration(
			t,
			ctx,
			client,
			params.DiskID,
			params.SrcZoneID,
			params.DstZoneID,
		)
	}

	successfullyMigrateDisk(
		t,
		ctx,
		client,
		params,
	)

	testcommon.CheckConsistency(t, ctx)
}

// TODO: NBS-4487 - uncomment this code.
/* func TestDiskServiceMigrateDiskFreezeAndUnfreeze(t *testing.T) {
	params := migrationTestParams{
		SrcZoneID: "zone",
		DstZoneID: "other",
		DiskID:    t.Name(),
		DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD,
		DiskSize:  migrationTestsDiskSize,
		FillDisk:  true,
	}

	ctx, client := setupMigrationTest(t, params)
	defer client.Close()

	checkFreezeAndUnfreezeDuringMigration(
		t,
		ctx,
		client,
		params.DiskID,
		params.SrcZoneID,
		params.DstZoneID,
	)

	testcommon.CheckConsistency(t, ctx)
} */

func TestDiskServiceMigrateOverlayDisk(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	imageID := t.Name()
	diskSize := migrationTestsDiskSize
	imageSize := diskSize / 2
	_, _ = testcommon.CreateImage(
		t,
		ctx,
		imageID,
		uint64(imageSize),
		"folder",
		true, // pooled
	)

	params := migrationTestParams{
		SrcZoneID: "zone",
		DstZoneID: "other",
		DiskID:    t.Name(),
		DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD,
		DiskSize:  migrationTestsDiskSize,
		FillDisk:  false,
	}

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcImageId{
			SrcImageId: imageID,
		},
		Size: params.DiskSize,
		Kind: params.DiskKind,
		DiskId: &disk_manager.DiskId{
			ZoneId: params.SrcZoneID,
			DiskId: params.DiskID,
		},
	})
	require.NoError(t, err)

	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	successfullyMigrateDisk(t, ctx, client, params)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceMigrateEmptyOverlayDiskWithAliveSrcImage(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	successfullyMigrateEmptyOverlayDisk(
		t,
		ctx,
		client,
		true, // withAliveSrcImage
	)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceMigrateEmptyOverlayDiskWithoutAliveSrcImage(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	successfullyMigrateEmptyOverlayDisk(
		t,
		ctx,
		client,
		false, // withAliveSrcImage
	)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceMigrateEmptyDisk(t *testing.T) {
	params := migrationTestParams{
		SrcZoneID: "zone",
		DstZoneID: "other",
		DiskID:    t.Name(),
		DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD,
		DiskSize:  migrationTestsDiskSize,
		FillDisk:  false,
	}

	ctx, client := setupMigrationTest(t, params)
	defer client.Close()

	successfullyMigrateEmptyDisk(
		t,
		ctx,
		client,
		params,
	)

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
			ZoneId: "zone",
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

	nbsClient := testcommon.NewNbsClient(t, ctx, "zone")
	diskParams1, err := nbsClient.Describe(ctx, diskID1)
	require.NoError(t, err)

	require.Equal(t, types.EncryptionMode_ENCRYPTION_AES_XTS, diskParams1.EncryptionDesc.Mode)
	diskKeyHash1, err := testcommon.GetEncryptionKeyHash(diskParams1.EncryptionDesc)
	require.NoError(t, err)
	require.Equal(t, []byte(nil), diskKeyHash1)

	encryptionDesc, err := disks.PrepareEncryptionDesc(encryption)
	require.NoError(t, err)

	crc32, _, err := testcommon.FillEncryptedDisk(nbsClient, diskID1, diskSize, encryptionDesc)
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
			ZoneId: "zone",
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
			ZoneId: "zone",
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
			ZoneId: "zone",
			DiskId: diskID3,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.Error(t, err)
	require.ErrorContains(t, err, "encryption mode should be the same")

	err = nbsClient.ValidateCrc32WithEncryption(diskID1, diskSize, encryptionDesc, crc32)
	require.NoError(t, err)
	err = nbsClient.ValidateCrc32WithEncryption(diskID2, diskSize, encryptionDesc, crc32)
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
			ZoneId: "zone",
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

func TestDiskServiceMigrateDiskInParallel(t *testing.T) {
	params := migrationTestParams{
		SrcZoneID: "zone",
		DstZoneID: "other",
		DiskID:    t.Name(),
		DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD,
		DiskSize:  migrationTestsDiskSize,
		FillDisk:  true,
	}

	ctx, client := setupMigrationTest(t, params)
	defer client.Close()

	srcZoneNBSClient := testcommon.NewNbsClient(t, ctx, params.SrcZoneID)

	// Writing some additional data to disk in parallel with migrations.
	waitForWrite, err := testcommon.GoWriteRandomBlocksToNbsDisk(
		ctx,
		srcZoneNBSClient,
		params.DiskID,
	)
	require.NoError(t, err)

	var operations []*operation_proto.Operation

	for i := 0; i < 3; i++ {
		reqCtx := testcommon.GetRequestContext(t, ctx)
		operation, err := client.MigrateDisk(reqCtx, &disk_manager.MigrateDiskRequest{
			DiskId: &disk_manager.DiskId{
				DiskId: params.DiskID,
				ZoneId: params.SrcZoneID,
			},
			DstZoneId: params.DstZoneID,
		})
		require.NoError(t, err)
		require.NotEmpty(t, operation)

		operations = append(operations, operation)
	}

	// Need to add some variance for better testing.
	testcommon.WaitForRandomDuration(time.Millisecond, time.Second)

	err = waitForWrite()
	require.NoError(t, err)

	diskSize := uint64(params.DiskSize)

	srcCrc32, err := srcZoneNBSClient.CalculateCrc32(params.DiskID, diskSize)
	require.NoError(t, err)

	for _, operation := range operations {
		waitForMigrationStatusOrError(
			t,
			ctx,
			client,
			operation,
			disk_manager.MigrateDiskMetadata_REPLICATING,
		)
	}

	for _, operation := range operations {
		_ = client.SendMigrationSignal(ctx, &disk_manager.SendMigrationSignalRequest{
			OperationId: operation.Id,
			Signal:      disk_manager.SendMigrationSignalRequest_FINISH_REPLICATION,
		})
	}

	// Need to add some variance for better testing.
	testcommon.WaitForRandomDuration(time.Millisecond, time.Second)

	for _, operation := range operations {
		_ = client.SendMigrationSignal(ctx, &disk_manager.SendMigrationSignalRequest{
			OperationId: operation.Id,
			Signal:      disk_manager.SendMigrationSignalRequest_FINISH_MIGRATION,
		})
	}

	successCount := 0

	for _, operation := range operations {
		err = internal_client.WaitOperation(ctx, client, operation.Id)
		if err == nil {
			successCount++
		}
	}

	dstZoneNBSClient := testcommon.NewNbsClient(t, ctx, params.DstZoneID)

	if successCount > 0 {
		_, err = srcZoneNBSClient.Describe(ctx, params.DiskID)
		require.Error(t, err)
		require.ErrorContains(t, err, "Path not found")

		dstCrc32, err := dstZoneNBSClient.CalculateCrc32(params.DiskID, diskSize)
		require.NoError(t, err)
		require.Equal(t, srcCrc32, dstCrc32)
	} else {
		// All migrations are cancelled. Check that src disk is not affected.
		crc32, err := srcZoneNBSClient.CalculateCrc32(params.DiskID, diskSize)
		require.NoError(t, err)
		require.Equal(t, srcCrc32, crc32)
	}

	testcommon.CheckConsistency(t, ctx)
}
