package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
)

////////////////////////////////////////////////////////////////////////////////

func TestNfsClientReportsMetrics(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	nfsClient := testcommon.NewNfsTestingClient(t, ctx, "zone-a")
	defer nfsClient.Close()

	filesystemID := t.Name()

	operation, err := client.CreateFilesystem(
		testcommon.GetRequestContext(t, ctx),
		&disk_manager.CreateFilesystemRequest{
			FilesystemId: &disk_manager.FilesystemId{
				ZoneId:       "zone-a",
				FilesystemId: filesystemID,
			},
			BlockSize: 4096,
			Size:      1024 * 1024 * 1024,
			Kind:      disk_manager.FilesystemKind_FILESYSTEM_KIND_SSD,
			CloudId:   "cloud",
			FolderId:  "folder",
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)
	defer func() {
		err := nfsClient.Delete(ctx, filesystemID, true)
		require.NoError(t, err)
	}()

	nfsClient.FillFilesystemWithDefaultTree(
		ctx,
		filesystemID,
		1000,
		5,
		3,
	)

	taskID := testcommon.ScheduleFilesystemScrubbing(
		t,
		ctx,
		"zone-a",
		filesystemID,
	)

	testcommon.WaitOperationEnded(t, ctx, taskID, 60*time.Second)

	require.Greater(t, testcommon.GetCountersControlplane(
		t,
		"count",
		map[string]string{
			"component": "nfs_client",
			"request":   "CreateFileStore",
		},
	)[0], float64(0))

	require.Greater(t, testcommon.GetCountersDataplane(
		t,
		"count",
		map[string]string{
			"component": "nfs_client_dataplane",
			"request":   "CreateSession",
		},
	)[0], float64(0))

	require.Greater(t, testcommon.GetCountersDataplane(
		t,
		"count",
		map[string]string{
			"component":  "nfs_session_dataplane",
			"request":    "ListNodes",
			"filesystem": filesystemID,
			"checkpoint": "",
		},
	)[0], float64(0))

	testcommon.CheckConsistency(t, ctx)
}
