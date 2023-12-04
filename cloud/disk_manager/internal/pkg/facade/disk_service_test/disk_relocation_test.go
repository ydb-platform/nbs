package disk_service_test

import (
	"context"
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
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
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
	// We do not check metadata.SecondsRemaining == 0 here because Compute
	// will not check SecondsRemaining after sending REPLICATION_FINISHED signal.
	// Moreover, SecondsRemaining might be nonzero if disk is Disk Registry based
	// and volume tablet reboots.

	err = client.SendMigrationSignal(ctx, &disk_manager.SendMigrationSignalRequest{
		OperationId: operation.Id,
		Signal:      disk_manager.SendMigrationSignalRequest_FINISH_MIGRATION,
	})
	require.NoError(t, err)

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

// Returns size in bytes that was transferred during replication.
func setupMigrateEmptyOverlayDiskTest(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	params migrationTestParams,
	withAliveSrcImage bool,
) uint64 {

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

	if withAliveSrcImage {
		// storageSize is 0 because we should not copy base disk data.
		return 0
	}

	return storageSize
}

func successfullyMigrateEmptyOverlayDisk(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	params migrationTestParams,
	expectedStorageSize uint64,
) {

	successfullyMigrateEmptyDisk(
		t,
		ctx,
		client,
		params,
	)

	dstZoneNBSClient := testcommon.NewNbsClient(t, ctx, params.DstZoneID)
	changedBytes, err := dstZoneNBSClient.GetChangedBytes(
		ctx,
		params.DiskID,
		"",
		"",
		true, // ignoreBaseDisk
	)
	require.NoError(t, err)

	require.Equal(t, expectedStorageSize, changedBytes)
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

	err = client.SendMigrationSignal(ctx, &disk_manager.SendMigrationSignalRequest{
		OperationId: operation.Id,
		Signal:      disk_manager.SendMigrationSignalRequest_FINISH_MIGRATION,
	})
	require.NoError(t, err)

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

func TestDiskServiceMigrateNonreplDisk(t *testing.T) {
	params := migrationTestParams{
		SrcZoneID: "zone-a",
		DstZoneID: "zone-b",
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
		SrcZoneID: "zone-a",
		DstZoneID: "zone-b",
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

func TestDiskServiceMigrateMirroredDisk(t *testing.T) {
	/*
		TODO: NBS-4747: Fix creation of mirrored disks in large tests on Disk Manager.
		param := migrationTestParams{
			SrcZoneID: "zone-a",
			DstZoneID: "zone-b",
			DiskID:    t.Name(),
			DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD_MIRROR3,
			DiskSize:  1073741824,
			FillDisk:  false,
		}

		ctx, client := setupMigrationTest(t, param)
		defer client.Close()

		successfullyMigrateDisk(t, ctx, client, param)

		testcommon.CheckConsistency(t, ctx)
	*/
}

func TestDiskServiceMigrateDisk(t *testing.T) {
	diskID := t.Name()
	params := migrationTestParams{
		SrcZoneID: "zone-a",
		DstZoneID: "zone-b",
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
		SrcZoneID: "zone-a",
		DstZoneID: "zone-b",
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
		SrcZoneID: "zone-a",
		DstZoneID: "zone-b",
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
		SrcZoneID: "zone-a",
		DstZoneID: "zone-b",
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
		SrcZoneID: "zone-a",
		DstZoneID: "zone-b",
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

	params := migrationTestParams{
		SrcZoneID: "zone-a",
		DstZoneID: "zone-b",
		DiskID:    t.Name(),
		DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD,
		DiskSize:  migrationTestsDiskSize,
		FillDisk:  false,
	}

	expectedStorageSize := setupMigrateEmptyOverlayDiskTest(
		t,
		ctx,
		client,
		params,
		true, // withAliveSrcImage
	)

	successfullyMigrateEmptyOverlayDisk(
		t,
		ctx,
		client,
		params,
		expectedStorageSize,
	)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceMigrateEmptyOverlayDiskWithoutAliveSrcImage(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	params := migrationTestParams{
		SrcZoneID: "zone-a",
		DstZoneID: "zone-b",
		DiskID:    t.Name(),
		DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD,
		DiskSize:  migrationTestsDiskSize,
		FillDisk:  false,
	}

	expectedStorageSize := setupMigrateEmptyOverlayDiskTest(
		t,
		ctx,
		client,
		params,
		false, // withAliveSrcImage
	)

	successfullyMigrateEmptyOverlayDisk(
		t,
		ctx,
		client,
		params,
		expectedStorageSize,
	)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceMigrateEmptyOverlayDiskWithAliveSrcImageAfterCancel(
	t *testing.T,
) {

	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	params := migrationTestParams{
		SrcZoneID: "zone-a",
		DstZoneID: "zone-b",
		DiskID:    t.Name(),
		DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD,
		DiskSize:  migrationTestsDiskSize,
		FillDisk:  false,
	}

	expectedStorageSize := setupMigrateEmptyOverlayDiskTest(
		t,
		ctx,
		client,
		params,
		true, // withAliveSrcImage
	)

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

	successfullyMigrateEmptyOverlayDisk(
		t,
		ctx,
		client,
		params,
		expectedStorageSize,
	)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceMigrateEmptyOverlayDiskWithoutAliveSrcImageAfterCancel(
	t *testing.T,
) {

	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	params := migrationTestParams{
		SrcZoneID: "zone-a",
		DstZoneID: "zone-b",
		DiskID:    t.Name(),
		DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD,
		DiskSize:  migrationTestsDiskSize,
		FillDisk:  false,
	}

	expectedStorageSize := setupMigrateEmptyOverlayDiskTest(
		t,
		ctx,
		client,
		params,
		false, // withAliveSrcImage
	)

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

	successfullyMigrateEmptyOverlayDisk(
		t,
		ctx,
		client,
		params,
		expectedStorageSize,
	)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceMigrateEmptyOverlayDiskInParallelWithRetireBaseDisks(
	t *testing.T,
) {

	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	privateClient, err := testcommon.NewPrivateClient(ctx)
	require.NoError(t, err)
	defer privateClient.Close()

	params := migrationTestParams{
		SrcZoneID: "zone-a",
		DstZoneID: "zone-b",
		DiskID:    t.Name(),
		DiskKind:  disk_manager.DiskKind_DISK_KIND_SSD,
		DiskSize:  migrationTestsDiskSize,
		FillDisk:  false,
	}

	expectedStorageSize := setupMigrateEmptyOverlayDiskTest(
		t,
		ctx,
		client,
		params,
		true, // withAliveSrcImage
	)

	// Need to schedule RetireBaseDisks task after migration was started.
	retireErr := make(chan error)
	retireOperation := make(chan *operation_proto.Operation)
	go func() {
		// Need to add some variance for better testing.
		testcommon.WaitForRandomDuration(1*time.Second, 2*time.Second)

		reqCtx := testcommon.GetRequestContext(t, ctx)
		operation, err := privateClient.RetireBaseDisks(reqCtx, &api.RetireBaseDisksRequest{
			ImageId: t.Name(),
			ZoneId:  "zone-a",
		})

		retireErr <- err
		retireOperation <- operation
	}()

	successfullyMigrateEmptyOverlayDisk(
		t,
		ctx,
		client,
		params,
		expectedStorageSize,
	)

	err = <-retireErr
	operation := <-retireOperation
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)
	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceMigrateEmptyDisk(t *testing.T) {
	params := migrationTestParams{
		SrcZoneID: "zone-a",
		DstZoneID: "zone-b",
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

func TestDiskServiceMigrateDiskInParallel(t *testing.T) {
	params := migrationTestParams{
		SrcZoneID: "zone-a",
		DstZoneID: "zone-b",
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
