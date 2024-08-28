package tests

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func getEndpoint() string {
	return fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_NFS_PORT"),
	)
}

func newFactory(t *testing.T, ctx context.Context) nfs.Factory {
	insecure := true
	return nfs.NewFactory(
		ctx,
		&config.ClientConfig{
			Zones: map[string]*config.Zone{
				"zone": {
					Endpoints: []string{getEndpoint(), getEndpoint()},
				},
			},
			Insecure: &insecure,
		},
		metrics.NewEmptyRegistry(),
	)
}

func newClient(t *testing.T, ctx context.Context) nfs.Client {
	factory := newFactory(t, ctx)
	client, err := factory.NewClient(ctx, "zone")
	require.NoError(t, err)
	return client
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateFilesystem(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	filesystemID := t.Name()

	err := client.Create(ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)

	// Creating the same filesystem twice is not an error
	err = client.Create(ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)
}

func TestDeleteFilesystem(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	filesystemID := t.Name()

	err := client.Create(ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)

	err = client.Delete(ctx, filesystemID)
	require.NoError(t, err)

	// Deleting the same filesystem twice is not an error
	err = client.Delete(ctx, filesystemID)
	require.NoError(t, err)

	// Deleting non-existent filesystem is also not an error
	err = client.Delete(ctx, filesystemID+"_does_not_exist")
	require.NoError(t, err)
}
