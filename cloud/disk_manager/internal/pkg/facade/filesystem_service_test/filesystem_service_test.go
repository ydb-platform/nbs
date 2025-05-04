package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
)

////////////////////////////////////////////////////////////////////////////////

func TestFilesystemServiceCreateEmptyFilesystem(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	filesystemID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateFilesystem(reqCtx, &disk_manager.CreateFilesystemRequest{
		FilesystemId: &disk_manager.FilesystemId{
			ZoneId:       "zone-a",
			FilesystemId: filesystemID,
		},
		BlockSize: 4096,
		Size:      4096000,
		Kind:      disk_manager.FilesystemKind_FILESYSTEM_KIND_HDD,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.DeleteFilesystem(reqCtx, &disk_manager.DeleteFilesystemRequest{
		FilesystemId: &disk_manager.FilesystemId{
			FilesystemId: filesystemID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	testcommon.CheckConsistency(t, ctx)
}

func TestFilesystemServiceCreateExternalFilesystem(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	taskStorage, err := testcommon.NewTaskStorage(ctx)
	require.NoError(t, err)

	filesystemID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateFilesystem(reqCtx, &disk_manager.CreateFilesystemRequest{
		FilesystemId: &disk_manager.FilesystemId{
			ZoneId:       "zone-a",
			FilesystemId: filesystemID,
		},
		BlockSize:  4096,
		Size:       4096000,
		Kind:       disk_manager.FilesystemKind_FILESYSTEM_KIND_HDD,
		IsExternal: true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	waitOperationWithTimeout := func(msg string, shouldTimeout bool, timeout time.Duration, opId string) {
		opCtx, _ := context.WithTimeout(ctx, timeout)
		err = internal_client.WaitOperation(opCtx, client, operation.Id)
		if shouldTimeout {
			require.Error(t, err, msg)
			require.Contains(t, err.Error(), "context deadline exceeded", msg)
		} else {
			require.NoError(t, err, msg)
		}

	}
	// validate CreateFilesystem task is stuck
	waitOperationWithTimeout("CreateFilesystem.1", true /* should timeout */, 60*time.Second, operation.Id)

	// Force finish CreateExternalFilesystem task
	externalTask, err := testcommon.FindRunningTaskByType(ctx, taskStorage, "filesystem.CreateExternalFilesystem", 100)
	require.NoError(t, err)

	err = taskStorage.ForceFinishTask(ctx, externalTask.ID)
	require.NoError(t, err)

	// validate CreateFilesystem finish succesfully
	waitOperationWithTimeout("CreateFilesystem.2", false /* should not timeout */, 30*time.Second, operation.Id)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.DeleteFilesystem(reqCtx, &disk_manager.DeleteFilesystemRequest{
		FilesystemId: &disk_manager.FilesystemId{
			FilesystemId: filesystemID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	// validate DeleteFilesystem task is stuck
	waitOperationWithTimeout("DeleteFilesystem.1", true /* should timeout */, 60*time.Second, operation.Id)

	externalTask, err = testcommon.FindRunningTaskByType(ctx, taskStorage, "filesystem.DeleteExternalFilesystem", 100)
	require.NoError(t, err)

	// Force finish DeleteExternalFilesystem task
	err = taskStorage.ForceFinishTask(ctx, externalTask.ID)
	require.NoError(t, err)

	// validate DeleteFilesystem finish succesfully
	waitOperationWithTimeout("DeleteFilesystem.2", false /* should not timeout */, 30*time.Second, operation.Id)

	testcommon.CheckConsistency(t, ctx)
}
