package filesystem_snapshot_service_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
)

////////////////////////////////////////////////////////////////////////////////

func TestCreateFilesystemSnapshot(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateFilesystemSnapshot(
		reqCtx,
		&disk_manager.CreateFilesystemSnapshotRequest{
			Src: &disk_manager.FilesystemId{
				FilesystemId: "test-filesystem-id",
				ZoneId:       "test-zone-id",
			},
			FilesystemSnapshotId: "test-filesystem-snapshot-id",
		},
	)
	require.NoError(t, err)
	require.NotNil(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.DeleteFilesystemSnapshot(
		reqCtx,
		&disk_manager.DeleteFilesystemSnapshotRequest{
			FilesystemSnapshotId: "test-filesystem-snapshot-id",
		},
	)
	require.NoError(t, err)
	require.NotNil(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)
}
