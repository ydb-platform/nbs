package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

func getAndCheckDependencyTask(
	t *testing.T,
	ctx context.Context,
	storage tasks_storage.Storage,
	operationID string,
	taskType string,
) string {

	dependentTask, err := storage.GetTask(ctx, operationID)
	require.NoError(t, err)

	dependencies := dependentTask.Dependencies.List()
	require.True(
		t,
		len(dependencies) == 1,
		"Invalid parent task dependencies, operationID=%s, taskType=%s, dependencies=%+v",
		operationID,
		taskType,
		dependencies,
	)

	task, err := storage.GetTask(ctx, dependencies[0])
	require.NoError(t, err)

	require.Equal(
		t,
		task.TaskType,
		taskType,
		"Dependency task has invalid type, operationID=%s, expectedType=%s, actualType=%s",
		operationID,
		taskType,
		task.TaskType,
	)

	return task.ID
}

////////////////////////////////////////////////////////////////////////////////

func TestFilesystemServiceCreateEmptyFilesystem(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	filesystemID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateFilesystem(
		reqCtx,
		&disk_manager.CreateFilesystemRequest{
			FilesystemId: &disk_manager.FilesystemId{
				ZoneId:       "zone-a",
				FilesystemId: filesystemID,
			},
			BlockSize: 4096,
			Size:      4096000,
			Kind:      disk_manager.FilesystemKind_FILESYSTEM_KIND_HDD,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.DeleteFilesystem(
		reqCtx,
		&disk_manager.DeleteFilesystemRequest{
			FilesystemId: &disk_manager.FilesystemId{
				FilesystemId: filesystemID,
			},
		},
	)
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

	storage, err := testcommon.NewTaskStorage(ctx)
	require.NoError(t, err)

	filesystemID := t.Name()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateFilesystem(
		reqCtx,
		&disk_manager.CreateFilesystemRequest{
			FilesystemId: &disk_manager.FilesystemId{
				ZoneId:       "zone-a",
				FilesystemId: filesystemID,
			},
			BlockSize:  4096,
			Size:       4096000,
			Kind:       disk_manager.FilesystemKind_FILESYSTEM_KIND_HDD,
			IsExternal: true,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	waitOperationOrTimeout := func(
		errorMesssage string,
		shouldTimeout bool,
		timeout time.Duration,
		operationID string,
	) {

		opCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		err = internal_client.WaitOperation(opCtx, client, operationID)
		if shouldTimeout {
			require.Error(t, err, errorMesssage)
			require.Contains(
				t,
				err.Error(),
				"context deadline exceeded",
				errorMesssage,
			)
		} else {
			require.NoError(t, err, errorMesssage)
		}
	}

	// Validate CreateFilesystem task is stuck
	waitOperationOrTimeout(
		"CreateFilesystem.1",
		true, /* should timeout */
		10*time.Second,
		operation.Id,
	)

	// Force finish CreateExternalFilesystem task
	externalTaskId := getAndCheckDependencyTask(
		t,
		ctx,
		storage,
		operation.Id,
		"filesystem.CreateExternalFilesystem",
	)

	err = storage.ForceFinishTask(ctx, externalTaskId)
	require.NoError(t, err)

	// Validate CreateFilesystem finish succesfully
	waitOperationOrTimeout(
		"CreateFilesystem.2",
		false, /* should not timeout */
		60*time.Second,
		operation.Id,
	)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.DeleteFilesystem(
		reqCtx,
		&disk_manager.DeleteFilesystemRequest{
			FilesystemId: &disk_manager.FilesystemId{
				FilesystemId: filesystemID,
			},
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	// Validate DeleteFilesystem task is stuck
	waitOperationOrTimeout(
		"DeleteFilesystem.1",
		true, /* should timeout */
		10*time.Second,
		operation.Id,
	)

	externalTaskId = getAndCheckDependencyTask(
		t,
		ctx,
		storage,
		operation.Id,
		"filesystem.DeleteExternalFilesystem",
	)

	// Force finish DeleteExternalFilesystem task
	err = storage.ForceFinishTask(ctx, externalTaskId)
	require.NoError(t, err)

	// Validate DeleteFilesystem finish succesfully
	waitOperationOrTimeout(
		"DeleteFilesystem.2",
		false, /* should not timeout */
		60*time.Second,
		operation.Id,
	)

	testcommon.CheckConsistency(t, ctx)
}
