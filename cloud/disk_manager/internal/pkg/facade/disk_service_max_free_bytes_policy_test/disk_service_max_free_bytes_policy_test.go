package disk_service_test

import (
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	cells_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
)

////////////////////////////////////////////////////////////////////////////////

const (
	shardedZoneID = "zone-d"
	cellID1       = "zone-d"
	cellID2       = "zone-d-shard1"
)

////////////////////////////////////////////////////////////////////////////////

func TestCreateEmptyDiskWithLeastOccupiedPolicy(t *testing.T) {
	ctx := testcommon.NewContext()

	deleteOlderThan := time.Now()

	testcommon.UpdateClusterCapacities(
		ctx,
		[]cells_storage.ClusterCapacity{
			{
				ZoneID:     shardedZoneID,
				CellID:     cellID1,
				FreeBytes:  1024,
				TotalBytes: 2024,
			},
			{
				ZoneID:     shardedZoneID,
				CellID:     cellID2,
				FreeBytes:  0,
				TotalBytes: 2024,
			},
		},
		deleteOlderThan,
	)

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	request := disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: 4096,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: shardedZoneID,
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

	diskMeta, err := testcommon.GetDiskMeta(ctx, diskID)
	require.NoError(t, err)
	require.Equal(t, cellID1, diskMeta.ZoneID)

	testcommon.CheckConsistency(t, ctx)
}
