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
	rootCertsFile := os.Getenv("DISK_MANAGER_RECIPE_ROOT_CERTS_FILE")
	return nfs.NewFactory(
		ctx,
		&config.ClientConfig{
			Zones: map[string]*config.Zone{
				"zone": {
					Endpoints: []string{getEndpoint(), getEndpoint()},
				},
			},
			RootCertsFile: &rootCertsFile,
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


func listAllFilest(
	ctx context.Context,
	client nfs.Client,
	session nfs.Session,
	parentNodeID uint64
) ([]nfs.Node, error) {
	var (
		nodes      []nfs.Node
		cookie     string
	)

	for {
		batch, nextCookie, err := client.ListNodes(ctx, session, parentNodeID, cookie)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, batch...)
		if nextCookie == "" {
			break
		}

		cookie = nextCookie
	}

	return nodes, nil
}

func dfsNodesTraversal(
	ctx context.Context,
	client nfs.Client,
	session nfs.Session,
	parentNodeID uint64,
	destination []nfs.Node,
) ([]nfs.Node, error) {

	nodes, err := listAllFilest(ctx, client, session, parentNodeID)
	if err != nil {
		return err
	}

	for _, node := range nodes {
		destination = append(destination, node)
		if node.Type == types.NodeType_NODE_TYPE_DIRECTORY {
			err := dfsNodesTraversal(ctx, client, session, node.ID, destination)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func nodeNames(nodes []nfs.Node) []string {
	result := make([]string, len(nodes))
	for i := range nodes {
		result[i] = nodes[i].Name
	}
	return result
}


func TestRecreateFilesystem(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	filesystemID1 := t.Name() + "_1"
	err := client.Create(ctx, filesystemID1, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)
	defer client.Delete(ctx, filesystemID1)

	session1, err := client.CreateSession(ctx, filesystemID1, false)
	require.NoError(t, err)
	defer client.DestroySession(ctx, session1)

	type file struct {
		name string
		children []file
		t types.
	}

	func createFile(parent uint64, name string) uint64{
		node := nfs.Node{
			ParentID: parent,
			Name:     name,
			Type:     types.NodeType_NODE_TYPE_FILE,
			Mode:     0o644,
		}
		id, err = client.CreateNode(ctx, session1, node)
		require.NoError(t, err)

		return id
	}

	func createDir(parent uint64, name string) uint64{
		node := nfs.Node{
			ParentID: parent,
			Name:     name,
			// todo replace here
			Type:     types.NodeType_NODE_TYPE_DIRECTORY,
			Mode:     0o755,
		}
		id, err = client.CreateNode(ctx, session1, node)
		require.NoError(t, err)

		return id
	}

	func createSymlink(parent uint64, name string, target string) uint64{
		node := nfs.Node{
			ParentID: parent,
			Name:     name,
			Type:     types.NodeType_NODE_TYPE_SYMLINK,
			Mode:     0o755,
			Target:   target,
		}
		id, err = client.CreateNode(ctx, session1, node)
		require.NoError(t, err)

		return id
	}

	rootID := uint64(1)

}
