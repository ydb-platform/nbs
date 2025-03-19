package tests

import (
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
)

////////////////////////////////////////////////////////////////////////////////

func testCreateSnapshotFromDiskWithFailedShadowDisk(
	t *testing.T,
	diskKind disk_manager.DiskKind,
	diskSize uint64,
	waitBeforeDisablingDeviceDuration time.Duration,
	shouldCancelOperation bool,
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

	snapshotID := t.Name()

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		SnapshotId: snapshotID,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	// Waiting before disabling device of shadow disk.
	// Need to add some variance for better testing.
	time.Sleep(waitBeforeDisablingDeviceDuration)

	// Finding agent and devices of shadow disk.
	diskRegistryStateBackup, err := nbsClient.BackupDiskRegistryState(ctx)
	require.NoError(t, err)
	shadowDisk := diskRegistryStateBackup.GetShadowDisk(diskID)

	// No shadow disk is OK: shadow disk could have not been created yet or it
	// might be already deleted.
	if shadowDisk != nil {
		deviceUUIDs := shadowDisk.DeviceUUIDs
		require.Equal(t, 1, len(deviceUUIDs))
		agentID := diskRegistryStateBackup.GetAgentIDByDeviceUUID(deviceUUIDs[0])
		require.NotEmpty(t, agentID)

		// Disabling device to enforce checkpoint status ERROR.
		err = nbsClient.DisableDevices(ctx, agentID, deviceUUIDs, t.Name())
		require.NoError(t, err)
	}

	if shouldCancelOperation {
		// Waiting before cancelling operation.
		// Need to add some variance for better testing.
		time.Sleep(waitBeforeDisablingDeviceDuration / 2)

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

		if strings.Contains(err.Error(), "Device disabled") {
			// OK: dataplane task failed with 'Device disabled' error, but shadow
			// disk was filled successfully.
			// TODO: improve this test after https://github.com/ydb-platform/nbs/issues/1950#issuecomment-2541530203
			return
		}

		require.Fail(t, "Unexpected error", err.Error())
	}

	response := disk_manager.CreateSnapshotResponse{}
	operationErr := internal_client.WaitResponse(
		ctx,
		client,
		operation.Id,
		&response,
	)
	checkOperationError(operationErr)

	// Should wait here because checkpoint could be deleted on operation cancel
	// (and exact time of this event is unknown).
	testcommon.WaitForCheckpointsDoNotExist(t, ctx, diskID)

	if operationErr != nil {
		// Nothing more to check.
		return
	}

	require.Equal(t, int64(diskSize), response.Size)

	meta := disk_manager.CreateSnapshotMetadata{}
	err = internal_client.GetOperationMetadata(ctx, client, operation.Id, &meta)
	require.NoError(t, err)
	require.Equal(t, float64(1), meta.Progress)

	// If snapshot was created successfully, should create disk from this snapshot.
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
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	err = nbsClient.ValidateCrc32(ctx, diskID2, diskContentInfo)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestSnapshotServiceCreateSnapshotFromDiskWithFailedShadowDisk(
	t *testing.T,
) {

	testCreateSnapshotFromDiskWithFailedShadowDisk(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		262144*4096, // diskSize
		common.RandomDuration(0*time.Second, 20*time.Second), // waitBeforeDisablingDeviceDuration
		false, // WithCancel
	)
}

func TestSnapshotServiceCreateSnapshotFromDiskWithFailedShadowDiskAndOperationCancel(
	t *testing.T,
) {

	testCreateSnapshotFromDiskWithFailedShadowDisk(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		262144*4096, // diskSize
		common.RandomDuration(0*time.Second, 20*time.Second), // waitBeforeDisablingDeviceDuration
		true, // WithCancel
	)
}
