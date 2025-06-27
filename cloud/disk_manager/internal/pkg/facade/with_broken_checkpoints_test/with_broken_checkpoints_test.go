package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
)

////////////////////////////////////////////////////////////////////////////////

func startCreateSnapshotOrImageFromDiskOperation(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	srcDiskID string,
	dstSnapshotOrImageID string,
	isImage bool,
) *disk_manager.Operation {

	reqCtx := testcommon.GetRequestContext(t, ctx)

	var operation *disk_manager.Operation
	var err error

	if isImage {
		operation, err = client.CreateImage(reqCtx, &disk_manager.CreateImageRequest{
			Src: &disk_manager.CreateImageRequest_SrcDiskId{
				SrcDiskId: &disk_manager.DiskId{
					ZoneId: "zone-a",
					DiskId: srcDiskID,
				},
			},
			DstImageId: dstSnapshotOrImageID,
			FolderId:   "folder",
			Pooled:     true,
		})
	} else {
		operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
			Src: &disk_manager.DiskId{
				ZoneId: "zone-a",
				DiskId: srcDiskID,
			},
			SnapshotId: dstSnapshotOrImageID,
			FolderId:   "folder",
		})
	}
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	return operation
}

func startCreateDiskFromSnapshotOrImageOperation(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	srcSnapshotOrImageID string,
	dstDiskID string,
	dstDiskSize int64,
	isImage bool,
) *disk_manager.Operation {

	reqCtx := testcommon.GetRequestContext(t, ctx)

	var operation *disk_manager.Operation
	var err error

	if isImage {
		operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
			Src: &disk_manager.CreateDiskRequest_SrcImageId{
				SrcImageId: srcSnapshotOrImageID,
			},
			Size: int64(dstDiskSize),
			Kind: disk_manager.DiskKind_DISK_KIND_SSD,
			DiskId: &disk_manager.DiskId{
				ZoneId: "zone-a",
				DiskId: dstDiskID,
			},
		})
	} else {
		operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
			Src: &disk_manager.CreateDiskRequest_SrcSnapshotId{
				SrcSnapshotId: srcSnapshotOrImageID,
			},
			Size: int64(dstDiskSize),
			Kind: disk_manager.DiskKind_DISK_KIND_SSD,
			DiskId: &disk_manager.DiskId{
				ZoneId: "zone-a",
				DiskId: dstDiskID,
			},
		})
	}
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	return operation
}

func makeBlankResponse(isImage bool) proto.Message {
	if isImage {
		return &disk_manager.CreateImageResponse{}
	}
	return &disk_manager.CreateSnapshotResponse{}
}

func getSizeFromResponse(response proto.Message, isImage bool) int64 {
	if isImage {
		return response.(*disk_manager.CreateImageResponse).Size
	}
	return response.(*disk_manager.CreateSnapshotResponse).Size
}

func getProgress(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	operation *disk_manager.Operation,
	isImage bool,
) float64 {

	var meta proto.Message
	if isImage {
		meta = &disk_manager.CreateImageMetadata{}
	} else {
		meta = &disk_manager.CreateSnapshotMetadata{}
	}

	err := internal_client.GetOperationMetadata(ctx, client, operation.Id, meta)
	require.NoError(t, err)

	if isImage {
		return meta.(*disk_manager.CreateImageMetadata).Progress
	}
	return meta.(*disk_manager.CreateSnapshotMetadata).Progress
}

////////////////////////////////////////////////////////////////////////////////

func disableDevicesOfShadowDisk(
	t *testing.T,
	ctx context.Context,
	diskRegistryStateBackup *nbs.DiskRegistryStateBackup,
	shadowDisk *nbs.DiskRegistryBasedDisk,
	nbsClient nbs.TestingClient,
) {

	deviceUUIDs := shadowDisk.DeviceUUIDs
	require.Equal(t, 1, len(deviceUUIDs))
	agentID := diskRegistryStateBackup.GetAgentIDByDeviceUUID(deviceUUIDs[0])
	require.NotEmpty(t, agentID)

	err := nbsClient.DisableDevices(ctx, agentID, deviceUUIDs, t.Name())
	require.NoError(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func testCreateSnapshotOrImageFromDiskWithFailedShadowDisk(
	t *testing.T,
	diskKind disk_manager.DiskKind,
	diskSize uint64,
	waitDuration time.Duration,
	shouldCancelOperation bool,
	isImage bool,
) {

	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name() + "1"
	diskBlockSize := 4096

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: int64(diskSize),
		Kind: diskKind,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		BlockSize: int64(diskBlockSize),
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, "zone-a")
	_, err = nbsClient.FillDisk(ctx, diskID, diskSize)
	require.NoError(t, err)

	diskContentInfo, err := nbsClient.CalculateCrc32(diskID, diskSize)
	require.NoError(t, err)

	snapshotOrImageID := t.Name()
	operation = startCreateSnapshotOrImageFromDiskOperation(
		t,
		ctx,
		client,
		diskID,
		snapshotOrImageID,
		isImage,
	)

	// Wait before disable device of the shadow disk.
	time.Sleep(waitDuration)

	// Disabling device of the shadow disk to enforce checkpoint status ERROR.
	for {
		diskRegistryStateBackup, err := nbsClient.BackupDiskRegistryState(ctx)
		require.NoError(t, err)
		shadowDisk := diskRegistryStateBackup.GetShadowDisk(diskID)

		if shadowDisk == nil {
			progress := getProgress(t, ctx, client, operation, isImage)
			if progress == float64(1) {
				// Shadow disk is already deleted. Exiting.
				return
			}
			// Shadow disk is not created yet. Waiting.
			continue
		}

		disableDevicesOfShadowDisk(
			t,
			ctx,
			diskRegistryStateBackup,
			shadowDisk,
			nbsClient)
		break
	}

	if shouldCancelOperation {
		// Wait before cancel the create snapshot/image from disk operation.
		time.Sleep(waitDuration)

		_, err = client.CancelOperation(ctx, &disk_manager.CancelOperationRequest{
			OperationId: operation.Id,
		})
		require.NoError(t, err)
	}

	checkOperationError := func(err error) {
		if err == nil {
			return
		}

		if shouldCancelOperation && strings.Contains(err.Error(), "Cancelled by client") {
			return
		}

		if strings.Contains(err.Error(), "Device disabled") ||
			strings.Contains(err.Error(), "Can't GetChangedBlocks when shadow disk is broken") {
			// Dataplane task failed with 'Device disabled' error, but shadow
			// disk was filled successfully.
			// TODO: improve this test after https://github.com/ydb-platform/nbs/issues/1950#issuecomment-2541530203
			return
		}

		require.Fail(t, "Unexpected error", err.Error())
	}

	response := makeBlankResponse(isImage)
	operationErr := internal_client.WaitResponse(
		ctx,
		client,
		operation.Id,
		response,
	)
	checkOperationError(operationErr)

	// Should wait here: if the operation was cancelled, then the checkpoint
	// was deleted on cancel (and exact time of this event is unknown).
	testcommon.WaitForNoCheckpointsExist(t, ctx, diskID)

	if operationErr != nil {
		// Nothing more to check.
		return
	}

	size := getSizeFromResponse(response, isImage)
	require.Equal(t, int64(diskSize), size)

	progress := getProgress(t, ctx, client, operation, isImage)
	require.Equal(t, float64(1), progress)

	// If snapshot/image was created successfully, should create disk from this
	// snapshot/image.
	diskID2 := t.Name() + "2"

	operation = startCreateDiskFromSnapshotOrImageOperation(
		t,
		ctx,
		client,
		snapshotOrImageID,
		diskID2,
		int64(diskSize),
		isImage,
	)

	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	err = nbsClient.ValidateCrc32(ctx, diskID2, diskContentInfo)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestSnapshotServiceCreateSnapshotFromDiskWithFailedShadowDisk(
	t *testing.T,
) {

	testCreateSnapshotOrImageFromDiskWithFailedShadowDisk(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		262144*4096, // diskSize
		// Need to add some variance for better testing.
		common.RandomDuration(0*time.Second, 20*time.Second), // waitDuration
		false, // WithCancel
		false, // isImage
	)
}

func TestSnapshotServiceCreateSnapshotFromDiskWithFailedShadowDiskAndOperationCancel(
	t *testing.T,
) {

	testCreateSnapshotOrImageFromDiskWithFailedShadowDisk(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		262144*4096, // diskSize
		// Need to add some variance for better testing.
		common.RandomDuration(0*time.Second, 20*time.Second), // waitDuration
		true,  // WithCancel
		false, // isImage
	)
}

func TestImageServiceCreateImageFromDiskWithFailedShadowDisk(
	t *testing.T,
) {

	testCreateSnapshotOrImageFromDiskWithFailedShadowDisk(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		262144*4096, // diskSize
		// Need to add some variance for better testing.
		common.RandomDuration(0*time.Second, 20*time.Second), // waitDuration
		false, // WithCancel
		true,  // isImage
	)
}

func TestImageServiceCreateImageFromDiskWithFailedShadowDiskAndOperationCancel(
	t *testing.T,
) {

	testCreateSnapshotOrImageFromDiskWithFailedShadowDisk(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		262144*4096, // diskSize
		// Need to add some variance for better testing.
		common.RandomDuration(0*time.Second, 20*time.Second), // waitDuration
		true, // WithCancel
		true, // isImage
	)
}

func TestCheckpointCreationFailsIfCheckpointIterationLimitExceeded(
	t *testing.T,
) {

	diskKind := disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED
	diskSize := uint64(262144 * 4096)

	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name() + "1"
	diskBlockSize := 4096

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: int64(diskSize),
		Kind: diskKind,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		BlockSize: int64(diskBlockSize),
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, "zone-a")
	_, err = nbsClient.FillDisk(ctx, diskID, diskSize)
	require.NoError(t, err)

	// Retries of broken checkpoints creation is incapsulated in dataplane task,
	// so it not necessary to check both image and snapshot creation.
	isImage := true
	imageID := t.Name()
	operation = startCreateSnapshotOrImageFromDiskOperation(
		t,
		ctx,
		client,
		diskID,
		imageID,
		isImage,
	)

	var firstShadowDiskID string

	// Breaking the first checkpoint.
	for {
		diskRegistryStateBackup, err := nbsClient.BackupDiskRegistryState(ctx)
		require.NoError(t, err)
		shadowDisk := diskRegistryStateBackup.GetShadowDisk(diskID)

		if shadowDisk == nil {
			// The first shadow disk is not created yet. Waiting.
			continue
		}

		firstShadowDiskID = shadowDisk.DiskID

		disableDevicesOfShadowDisk(
			t,
			ctx,
			diskRegistryStateBackup,
			shadowDisk,
			nbsClient)
		break
	}

	// Breaking the second checkpoint.
	for {
		diskRegistryStateBackup, err := nbsClient.BackupDiskRegistryState(ctx)
		require.NoError(t, err)
		shadowDisk := diskRegistryStateBackup.GetShadowDisk(diskID)

		if shadowDisk == nil || shadowDisk.DiskID == firstShadowDiskID {
			// The second shadow disk is not created yet. Waiting.
			continue
		}

		disableDevicesOfShadowDisk(
			t,
			ctx,
			diskRegistryStateBackup,
			shadowDisk,
			nbsClient)
		break
	}

	response := makeBlankResponse(isImage)
	err = internal_client.WaitResponse(
		ctx,
		client,
		operation.Id,
		response,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "Non retriable error")
	require.ErrorContains(t, err, "Too many failed checkpoint iterations: 2")

	// Should wait here because second checkpoint is deleted on imageID creation
	// operation cancel (and exact time of this event is unknown).
	testcommon.WaitForNoCheckpointsExist(t, ctx, diskID)
}
