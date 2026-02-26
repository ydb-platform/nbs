package testing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
)

////////////////////////////////////////////////////////////////////////////////

type TestingClient interface {
	nfs.Client

	FillFilesystemWithDefaultTree(
		ctx context.Context,
		filesystemID string,
	) *tasks_common.StringSet
}

////////////////////////////////////////////////////////////////////////////////

type testingClient struct {
	nfs.Client
	t *testing.T
}

func (c *testingClient) FillFilesystemWithDefaultTree(
	ctx context.Context,
	filesystemID string,
) *tasks_common.StringSet {

	session, err := c.CreateSession(ctx, filesystemID, "", false)
	require.NoError(c.t, err)
	defer func() {
		err := c.DestroySession(ctx, session)
		require.NoError(c.t, err)
	}()

	layers := []FilesystemLayerConfig{
		{DirsCount: 3, FilesCount: 100},
		{DirsCount: 10, FilesCount: 1000},
		{DirsCount: 10, FilesCount: 1000},
	}
	tree := HomogeneousDirectoryTree(layers)

	model := NewParallelFilesystemModel(c.t, ctx, c.Client, session, tree)
	model.CreateAllNodesRecursively()

	return model.ExpectedNodeNames()
}

////////////////////////////////////////////////////////////////////////////////

func NewTestingClient(t *testing.T, client nfs.Client) TestingClient {
	return &testingClient{
		Client: client,
		t:      t,
	}
}
