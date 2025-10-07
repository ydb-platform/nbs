package tests

import (
	"hash/crc32"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

func testCreateSnapshotFromDisk(
	t *testing.T,
	diskKind disk_manager.DiskKind,
	diskBlockSize uint32,
	diskSize uint64,
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
	_, err = nbsClient.FillDisk(ctx, diskID, 64*4096)
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

	response := disk_manager.CreateSnapshotResponse{}
	err = internal_client.WaitResponse(ctx, client, operation.Id, &response)
	require.NoError(t, err)
	require.Equal(t, int64(diskSize), response.Size)

	meta := disk_manager.CreateSnapshotMetadata{}
	err = internal_client.GetOperationMetadata(ctx, client, operation.Id, &meta)
	require.NoError(t, err)
	require.Equal(t, float64(1), meta.Progress)

	diskParams, err := nbsClient.Describe(ctx, diskID)
	require.NoError(t, err)

	if diskParams.IsDiskRegistryBasedDisk {
		testcommon.RequireCheckpointsDoNotExist(t, ctx, diskID)
	} else {
		testcommon.RequireCheckpoint(t, ctx, diskID, snapshotID)
	}

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.DeleteSnapshot(reqCtx, &disk_manager.DeleteSnapshotRequest{
		SnapshotId: snapshotID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestSnapshotServiceCreateSnapshotFromDisk(t *testing.T) {
	testCreateSnapshotFromDisk(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD,
		4096,         // diskBlockSize
		32*1024*4096, // diskSize
	)
}

func TestSnapshotServiceCreateSnapshotFromLargeDisk(t *testing.T) {
	testCreateSnapshotFromDisk(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD,
		65536,         // diskBlockSize
		1000000*65536, // diskSize
	)
}

func TestSnapshotServiceCreateSnapshotFromSsdNonreplicatedDisk(t *testing.T) {
	testCreateSnapshotFromDisk(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		4096,        // diskBlockSize
		262144*4096, // diskSize
	)
}

func TestSnapshotServiceCreateSnapshotFromHddNonreplicatedDisk(t *testing.T) {
	testCreateSnapshotFromDisk(
		t,
		disk_manager.DiskKind_DISK_KIND_HDD_NONREPLICATED,
		4096,        // diskBlockSize
		262144*4096, // diskSize
	)
}

func TestSnapshotServiceCancelCreateSnapshotFromDisk(t *testing.T) {
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
		Size: 4194304,
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
	testcommon.CancelOperation(t, ctx, client, operation.Id)
	testcommon.WaitOperationEnded(t, ctx, operation.Id)

	snapshotMeta, err := testcommon.GetSnapshotMeta(ctx, snapshotID)

	// If snapshot creation was cancelled, checkpoint should be deleted.
	// Otherwise, there should be a checkpoint from snapshot.
	if snapshotMeta == nil {
		// Two possible cases here: GetSnapshotMeta returned nil because
		// snapshot does not exist, or the snapshot is no longer
		// ready (its status has changed to Deleting or Deleted).
		require.NoError(t, err)
		testcommon.RequireCheckpointsDoNotExist(t, ctx, diskID)
	} else if !snapshotMeta.Ready {
		require.NoError(t, err)
		testcommon.RequireCheckpointsDoNotExist(t, ctx, diskID)
	} else {
		require.NoError(t, err)
		testcommon.RequireCheckpoint(t, ctx, diskID, snapshotID)
	}

	testcommon.CheckConsistency(t, ctx)
}

////////////////////////////////////////////////////////////////////////////////

func testCreateIncrementalSnapshotFromDisk(
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
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, "zone-a")
	contentSize := 134217728

	bytes := make([]byte, contentSize)
	for i := 0; i < len(bytes); i++ {
		bytes[i] = 1
	}

	err = nbsClient.Write(diskID1, 0, bytes)
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

	for i := 0; i < len(bytes)/2; i++ {
		bytes[i] = 2
	}

	err = nbsClient.Write(diskID1, 0, bytes[0:len(bytes)/2])
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

	// Create disk in order to validate last incremental snapshot.
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
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	acc := crc32.NewIEEE()
	_, err = acc.Write(bytes)
	require.NoError(t, err)

	err = nbsClient.ValidateCrc32(ctx, diskID2, nbs.DiskContentInfo{
		ContentSize: uint64(contentSize),
		Crc32:       acc.Sum32(),
	})
	require.NoError(t, err)

	testcommon.CheckBaseSnapshot(t, ctx, snapshotID2, snapshotID1)
	testcommon.CheckConsistency(t, ctx)
}

func TestSnapshotServiceCreateIncrementalSnapshotFromDisk(t *testing.T) {
	testCreateIncrementalSnapshotFromDisk(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD,
		36*1024*4096, // diskSize
	)
}

func TestSnapshotServiceCreateIncrementalSnapshotFromSsdNonreplicatedDisk(t *testing.T) {
	testCreateIncrementalSnapshotFromDisk(
		t,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		262144*4096, // diskSize
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestSnapshotServiceCreateIncrementalSnapshotAfterDeletionOfBaseSnapshot(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID1 := t.Name() + "1"
	diskSize := 134217728

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
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, "zone-a")

	bytes := make([]byte, diskSize)
	for i := 0; i < len(bytes); i++ {
		bytes[i] = 1
	}

	err = nbsClient.Write(diskID1, 0, bytes)
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

	// Next snapshot should be a full copy of disk.
	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.DeleteSnapshot(reqCtx, &disk_manager.DeleteSnapshotRequest{
		SnapshotId: snapshotID1,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
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

	// Create disk in order to validate snapshot.
	diskID2 := t.Name() + "2"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcSnapshotId{
			SrcSnapshotId: snapshotID2,
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

	acc := crc32.NewIEEE()
	_, err = acc.Write(bytes)
	require.NoError(t, err)

	err = nbsClient.ValidateCrc32(ctx, diskID2, nbs.DiskContentInfo{
		ContentSize: uint64(diskSize),
		Crc32:       acc.Sum32(),
	})
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestSnapshotServiceCreateIncrementalSnapshotWhileDeletingBaseSnapshot(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID1 := t.Name() + "1"
	diskSize := 134217728

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
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, "zone-a")

	_, err = nbsClient.FillDisk(ctx, diskID1, uint64(diskSize))
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

	waitForWrite, err := nbsClient.GoWriteRandomBlocksToNbsDisk(ctx, diskID1)
	require.NoError(t, err)
	err = waitForWrite()
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

	// Need to add some variance for better testing.
	common.WaitForRandomDuration(time.Millisecond, time.Second)
	reqCtx = testcommon.GetRequestContext(t, ctx)
	deleteOperation, err := client.DeleteSnapshot(reqCtx, &disk_manager.DeleteSnapshotRequest{
		SnapshotId: snapshotID1,
	})
	require.NoError(t, err)
	require.NotEmpty(t, deleteOperation)

	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	err = internal_client.WaitOperation(ctx, client, deleteOperation.Id)
	require.NoError(t, err)

	// Create disk in order to validate snapshot.
	diskID2 := t.Name() + "2"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcSnapshotId{
			SrcSnapshotId: snapshotID2,
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

	diskContentInfo, err := nbsClient.CalculateCrc32(diskID1, uint64(diskSize))
	require.NoError(t, err)

	err = nbsClient.ValidateCrc32(ctx, diskID2, diskContentInfo)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestSnapshotServiceDeleteIncrementalSnapshotBeforeCreating(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	diskSize := 134217728

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
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
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	baseSnapshotID := t.Name() + "_base"
	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		SnapshotId: baseSnapshotID,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	snapshotID1 := t.Name() + "1"
	deleteRequest := disk_manager.DeleteSnapshotRequest{
		SnapshotId: snapshotID1,
	}
	reqCtx = testcommon.GetRequestContext(t, ctx)
	deleteOperation, err := client.DeleteSnapshot(reqCtx, &deleteRequest)
	require.NoError(t, err)
	require.NotEmpty(t, deleteOperation)
	err = internal_client.WaitOperation(ctx, client, deleteOperation.Id)
	require.NoError(t, err)
	testcommon.RequireCheckpoint(t, ctx, diskID, baseSnapshotID)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	createOperation, err := client.CreateSnapshot(
		reqCtx,
		&disk_manager.CreateSnapshotRequest{
			Src: &disk_manager.DiskId{
				ZoneId: "zone-a",
				DiskId: diskID,
			},
			SnapshotId: snapshotID1,
			FolderId:   "folder",
		})
	require.NoError(t, err)
	require.NotEmpty(t, createOperation)
	err = internal_client.WaitOperation(ctx, client, createOperation.Id)
	require.NoError(t, err)
	testcommon.RequireCheckpoint(t, ctx, diskID, snapshotID1)

	snapshotID2 := t.Name() + "2"
	// Check that it's possible to create another incremental snapshot.
	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(
		reqCtx,
		&disk_manager.CreateSnapshotRequest{
			Src: &disk_manager.DiskId{
				ZoneId: "zone-a",
				DiskId: diskID,
			},
			SnapshotId: snapshotID2,
			FolderId:   "folder",
		})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestSnapshotServiceDeleteIncrementalSnapshotWhileCreating(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	diskSize := 134217728

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
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
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	baseSnapshotID := t.Name() + "_base"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		SnapshotId: baseSnapshotID,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	snapshotID1 := t.Name() + "1"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	createOperation, err := client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		SnapshotId: snapshotID1,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, createOperation)

	// Need to add some variance for better testing.
	common.WaitForRandomDuration(1*time.Millisecond, 3*time.Second)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	deleteOperation, err := client.DeleteSnapshot(reqCtx, &disk_manager.DeleteSnapshotRequest{
		SnapshotId: snapshotID1,
	})
	require.NoError(t, err)
	require.NotEmpty(t, deleteOperation)
	creationErr := internal_client.WaitOperation(ctx, client, createOperation.Id)
	err = internal_client.WaitOperation(ctx, client, deleteOperation.Id)
	require.NoError(t, err)

	// If snapshot creation and it's deletion ends up successfuly it means
	// snapshot creation and deletion operations were performed sequentially.
	// These cases are checked in other tests:
	// TestSnapshotServiceDeleteIncrementalSnapshotBeforeCreating and
	// TestSnapshotServiceDeleteIncrementalSnapshotAfterCreating.
	if creationErr != nil {
		snapshotID, _, err := testcommon.GetIncremental(ctx, &types.Disk{
			ZoneId: "zone-a",
			DiskId: diskID,
		})
		require.NoError(t, err)

		// Should wait here because checkpoint is deleted on snapshotID1
		// creation operation cancel (and exact time of this event is unknown).
		testcommon.WaitForCheckpointDoesNotExist(t, ctx, diskID, snapshotID1)
		// In case of snapshot1 creation failure base snapshot may be already
		// deleted from incremental table and then checkpoint should not exist
		// on the disk. Otherwise base snapshot checkpoint should exist.
		if len(snapshotID) > 0 {
			require.Equal(t, baseSnapshotID, snapshotID)
			testcommon.RequireCheckpoint(t, ctx, diskID, baseSnapshotID)
		} else {
			testcommon.RequireCheckpointsDoNotExist(t, ctx, diskID)
		}
	}

	snapshotID2 := t.Name() + "2"
	// Check that it's possible to create another incremental snapshot
	// (NBS-3192).
	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		SnapshotId: snapshotID2,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestSnapshotServiceDeleteIncrementalSnapshotAfterCreating(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	diskSize := 134217728

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
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
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	baseSnapshotID := t.Name() + "_base"
	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		SnapshotId: baseSnapshotID,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	snapshotID1 := t.Name() + "1"
	reqCtx = testcommon.GetRequestContext(t, ctx)
	createOperation, err := client.CreateSnapshot(
		reqCtx,
		&disk_manager.CreateSnapshotRequest{
			Src: &disk_manager.DiskId{
				ZoneId: "zone-a",
				DiskId: diskID,
			},
			SnapshotId: snapshotID1,
			FolderId:   "folder",
		})
	require.NoError(t, err)
	require.NotEmpty(t, createOperation)
	err = internal_client.WaitOperation(ctx, client, createOperation.Id)
	require.NoError(t, err)
	testcommon.RequireCheckpoint(t, ctx, diskID, snapshotID1)

	deleteRequest := disk_manager.DeleteSnapshotRequest{
		SnapshotId: snapshotID1,
	}
	reqCtx = testcommon.GetRequestContext(t, ctx)
	deleteOperation, err := client.DeleteSnapshot(reqCtx, &deleteRequest)
	require.NoError(t, err)
	require.NotEmpty(t, deleteOperation)
	err = internal_client.WaitOperation(ctx, client, deleteOperation.Id)
	require.NoError(t, err)
	testcommon.RequireCheckpointsDoNotExist(t, ctx, diskID)

	snapshotID2 := t.Name() + "2"
	// Check that it's possible to create another incremental snapshot.
	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateSnapshot(
		reqCtx,
		&disk_manager.CreateSnapshotRequest{
			Src: &disk_manager.DiskId{
				ZoneId: "zone-a",
				DiskId: diskID,
			},
			SnapshotId: snapshotID2,
			FolderId:   "folder",
		})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestSnapshotServiceDeleteSnapshot(t *testing.T) {
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
		Size: 4194304,
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
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	// Check two operations in flight.

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation1, err := client.DeleteSnapshot(reqCtx, &disk_manager.DeleteSnapshotRequest{
		SnapshotId: snapshotID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation1)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation2, err := client.DeleteSnapshot(reqCtx, &disk_manager.DeleteSnapshotRequest{
		SnapshotId: snapshotID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation2)

	err = internal_client.WaitOperation(ctx, client, operation1.Id)
	require.NoError(t, err)

	err = internal_client.WaitOperation(ctx, client, operation2.Id)
	require.NoError(t, err)

	testcommon.RequireCheckpointsDoNotExist(t, ctx, diskID)
	testcommon.CheckConsistency(t, ctx)
}

func TestSnapshotServiceDeleteSnapshotWhenCreationIsInFlight(t *testing.T) {
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
		Size: 4194304,
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

	snapshotID := t.Name()

	reqCtx = testcommon.GetRequestContext(t, ctx)
	createOp, err := client.CreateSnapshot(reqCtx, &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
		SnapshotId: snapshotID,
		FolderId:   "folder",
	})
	require.NoError(t, err)
	require.NotEmpty(t, createOp)

	// Need to add some variance for better testing.
	common.WaitForRandomDuration(time.Millisecond, 2*time.Second)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.DeleteSnapshot(reqCtx, &disk_manager.DeleteSnapshotRequest{
		SnapshotId: snapshotID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	err = internal_client.WaitOperation(ctx, client, createOp.Id)

	// If snapshot creation ends up successfuly, there may be two cases:
	// Snapshot was created and then deleted, so should be no checkpoints left
	// or snapshot deletion ended up before creation, snapshot was not deleted,
	// so there should be a checkpoint.
	if err != nil {
		// Should wait here because checkpoint is deleted on |createOp|
		// operation cancel (and exact time of this event is unknown).
		testcommon.WaitForCheckpointDoesNotExist(t, ctx, diskID, snapshotID)
		testcommon.RequireCheckpointsDoNotExist(t, ctx, diskID)
	}

	testcommon.CheckConsistency(t, ctx)
}
