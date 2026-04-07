package tests

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	nfs_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/testing"
	scrubbing_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/config"
	scrubbing_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/util"
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

const (
	regularScrubTaskType = "dataplane.RegularScrubFilesystems"
	scrubTaskType        = "dataplane.ScrubFilesystem"
)

func getRegularScrubFilesystemIDs() []string {
	configPath := os.Getenv("SCRUBBING_CONFIG_PATH")
	if configPath == "" {
		return nil
	}

	cfg := &scrubbing_config.FilesystemScrubbingConfig{}
	if err := util.ParseProto(configPath, cfg); err != nil {
		return nil
	}

	filesystems := cfg.GetFilesystemsWithRegularScrubbingEnabled()
	ids := make([]string, len(filesystems))
	for i, fs := range filesystems {
		ids[i] = fs.GetFilesystemId()
	}

	return ids
}

// getRegularScrubTaskIDs finds the running regular scrub task, asserts there
// is exactly one, and returns its scheduled scrub task IDs from metadata.
// Polls until the task appears and its metadata is non-empty.
func getRegularScrubTaskIDs(
	t *testing.T,
	ctx context.Context,
	taskStorage tasks_storage.Storage,
) []string {

	for {
		runningTasks, err := taskStorage.ListTasksRunning(ctx, 1000)
		require.NoError(t, err)

		var found []tasks_storage.TaskInfo
		for _, taskInfo := range runningTasks {
			if taskInfo.TaskType == regularScrubTaskType {
				found = append(found, taskInfo)
			}
		}
		require.LessOrEqual(t, len(found), 1,
			"expected at most one regular scrub task")

		if len(found) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		metadataMessage := testcommon.GetTaskMetadata(t, ctx, found[0].ID)
		metadata, ok := metadataMessage.(*scrubbing_protos.RegularScrubFilesystemsMetadata)
		require.True(t, ok)

		taskIDs := metadata.GetTaskIds()
		if len(taskIDs) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		return taskIDs
	}
}

func resolveScrubbedFilesystemIDs(
	t *testing.T,
	ctx context.Context,
	taskStorage tasks_storage.Storage,
	taskIDs []string,
) tasks_common.StringSet {

	filesystemIDs := tasks_common.NewStringSet()
	for _, taskID := range taskIDs {
		taskState, err := taskStorage.GetTask(ctx, taskID)
		require.NoError(t, err)
		require.Equal(t, scrubTaskType, taskState.TaskType)

		request := &scrubbing_protos.ScrubFilesystemRequest{}
		err = proto.Unmarshal(taskState.Request, request)
		require.NoError(t, err)
		filesystemIDs.Add(request.GetFilesystem().GetFilesystemId())
	}
	return filesystemIDs
}

////////////////////////////////////////////////////////////////////////////////

func createFilesystemForScrubbing(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	nfsClient nfs_testing.TestingClient,
	filesystemID string,
) {
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

	nfsClient.FillFilesystemWithDefaultTree(
		ctx,
		filesystemID,
		100,
		0,
		1,
	)
}

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

	testcommon.WaitOperationEnded(t, ctx, taskID, 200*time.Second)
	testcommon.GetTaskError(t, ctx, taskID)
	testcommon.CheckConsistency(t, ctx)
}

func TestRegularFilesystemScrubbing(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	nfsClient := testcommon.NewNfsTestingClient(t, ctx, "zone-a")
	defer nfsClient.Close()

	regularScrubFilesystemIDs := getRegularScrubFilesystemIDs()
	require.NotEmpty(
		t,
		regularScrubFilesystemIDs,
		"regular scrub filesystem IDs must not be empty",
	)

	expectedFilesystems := tasks_common.NewStringSet(regularScrubFilesystemIDs...)

	for _, filesystemID := range regularScrubFilesystemIDs {
		createFilesystemForScrubbing(t, ctx, client, nfsClient, filesystemID)

		filesystemID := filesystemID
		defer func() {
			err := nfsClient.Delete(ctx, filesystemID, true)
			require.NoError(t, err)
		}()
	}

	taskStorage, err := testcommon.NewTaskStorage(ctx)
	require.NoError(t, err)

	// First iteration: wait for all filesystems to be scheduled.
	firstTaskIDs := getRegularScrubTaskIDs(t, ctx, taskStorage)
	require.True(
		t,
		expectedFilesystems.Equals(
			resolveScrubbedFilesystemIDs(t, ctx, taskStorage, firstTaskIDs),
		),
		"first round: scrub tasks must reference all configured filesystems",
	)

	for _, taskID := range firstTaskIDs {
		testcommon.WaitOperationEnded(t, ctx, taskID, 200*time.Second)
		testcommon.GetTaskError(t, ctx, taskID)
	}

	// Second iteration: ensure tasks are rescheduled.
	secondTaskIDs := getRegularScrubTaskIDs(t, ctx, taskStorage)
	require.True(
		t,
		expectedFilesystems.Equals(
			resolveScrubbedFilesystemIDs(t, ctx, taskStorage, secondTaskIDs),
		),
		"second round: scrub tasks must reference all configured filesystems",
	)

	for _, taskID := range secondTaskIDs {
		testcommon.WaitOperationEnded(t, ctx, taskID, 200*time.Second)
		testcommon.GetTaskError(t, ctx, taskID)
	}
}
