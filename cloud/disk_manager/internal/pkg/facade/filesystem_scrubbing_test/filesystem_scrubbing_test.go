package tests

import (
	"bufio"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
)

////////////////////////////////////////////////////////////////////////////////

func TestFilesystemScrubbingTraversesFilesystem(t *testing.T) {
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

	expectedNodes := nfsClient.FillFilesystemWithDefaultTree(
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

	testcommon.WaitOperationEnded(t, ctx, taskID)

	logPath := os.Getenv("DISK_MANAGER_RECIPE_LIST_NODES_LOG_PATH")
	require.NotEmpty(
		t,
		logPath,
		"DISK_MANAGER_RECIPE_LIST_NODES_LOG_PATH not set",
	)

	file, err := os.Open(logPath)
	require.NoError(t, err)
	defer file.Close()

	scrubbedNodes := tasks_common.NewStringSet()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		nodeName := scanner.Text()
		if nodeName != "" {
			scrubbedNodes.Add(nodeName)
		}
	}

	require.NoError(t, scanner.Err())
	require.True(t, expectedNodes.Equals(scrubbedNodes))
	testcommon.CheckConsistency(t, ctx)
}
