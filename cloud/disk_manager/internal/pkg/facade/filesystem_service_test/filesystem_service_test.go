package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_api "github.com/ydb-platform/nbs/cloud/disk_manager/internal/api"
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
			CloudId:   "cloud",
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
		60*time.Second,
		operation.Id,
	)

	privateClient, err := testcommon.NewPrivateClient(ctx)
	require.NoError(t, err)
	defer privateClient.Close()

	err = privateClient.FinishExternalFilesystemCreation(
		ctx,
		&internal_api.FinishExternalFilesystemCreationRequest{
			FilesystemId:               filesystemID,
			ExternalStorageClusterName: "external-cluster-1",
		},
	)
	require.NoError(t, err)

	response := disk_manager.CreateFilesystemResponse{}
	err = internal_client.WaitResponse(ctx, client, operation.Id, &response)
	require.NoError(t, err)
	require.Equal(t, "external-cluster-1", response.ExternalStorageClusterName)

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
		60*time.Second,
		operation.Id,
	)

	err = privateClient.FinishExternalFilesystemDeletion(
		ctx,
		&internal_api.FinishExternalFilesystemDeletionRequest{
			FilesystemId: filesystemID,
		},
	)
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
