package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	db "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	ydb "github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceCreateDeletePlacementGroup(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	config := testcommon.GetServerConfig(t)

	ydbClient, err := ydb.NewYDBClient(
		ctx,
		config.GetPersistenceConfig(),
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	endedMigrationExpirationTimeout, err := time.ParseDuration(
		config.GetDisksConfig().GetEndedMigrationExpirationTimeout(),
	)
	require.NoError(t, err)

	storage, err := db.NewStorage(
		"",
		"",
		"",
		"",
		"",
		config.GetPlacementGroupConfig().GetStorageFolder(),
		ydbClient,
		endedMigrationExpirationTimeout,
		false, // backupEnabled
		nil,   // backupFolderIDs
	)
	require.NoError(t, err)

	groupID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreatePlacementGroup(reqCtx, &disk_manager.CreatePlacementGroupRequest{
		GroupId: &disk_manager.GroupId{
			ZoneId:  "zone-a",
			GroupId: groupID,
		},
		PlacementStrategy: disk_manager.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	_, err = storage.CheckPlacementGroupReady(ctx, groupID)
	require.NoError(t, err)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.DeletePlacementGroup(reqCtx, &disk_manager.DeletePlacementGroupRequest{
		GroupId: &disk_manager.GroupId{
			ZoneId:  "zone-a",
			GroupId: groupID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestDiskServiceCreatePartitionPlacementGroup(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	groupID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreatePlacementGroup(reqCtx, &disk_manager.CreatePlacementGroupRequest{
		GroupId: &disk_manager.GroupId{
			ZoneId:  "zone-a",
			GroupId: groupID,
		},
		PlacementStrategy:       disk_manager.PlacementStrategy_PLACEMENT_STRATEGY_PARTITION,
		PlacementPartitionCount: 2,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}
