package tests

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"

	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"

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

////////////////////////////////////////////////////////////////////////////////

type fileNode struct {
	name     string
	children []fileNode
	fileType nfs_client.NodeType
	target   string
	root     bool
}

func (f fileNode) create(
	t *testing.T,
	ctx context.Context,
	client nfs.Client,
	session nfs.Session,
	parentId uint64,
) {
	mode := uint32(0o644)
	if f.fileType == nfs_client.NodeType_DIR {
		mode = 0o755
	}

	id := uint64(1)
	if !f.root {
		var err error
		id, err = client.CreateNode(ctx, session, nfs.Node{
			ParentID:   parentId,
			Name:       f.name,
			Type:       f.fileType,
			Mode:       mode,
			UID:        1,
			GID:        1,
			LinkTarget: f.target,
		})
		require.NoError(t, err)
	}
	if f.fileType != nfs_client.NodeType_DIR {
		return
	}

	for _, child := range f.children {
		child.create(t, ctx, client, session, id)
	}
}

func dir(name string, children ...fileNode) fileNode {
	return fileNode{
		name:     name,
		fileType: nfs_client.NodeType_DIR,
		children: children,
		root:     false,
	}
}

func file(name string) fileNode {
	return fileNode{
		name:     name,
		fileType: nfs_client.NodeType_FILE,
		root:     false,
	}
}

func symlink(name string, target string) fileNode {
	return fileNode{
		name:     name,
		fileType: nfs_client.NodeType_SYMLINK,
		target:   target,
		root:     false,
	}
}

func root(children ...fileNode) fileNode {
	return fileNode{
		name:     "/",
		fileType: nfs_client.NodeType_DIR,
		children: children,
		root:     true,
	}
}

////////////////////////////////////////////////////////////////////////////////

func listAllFilest(
	ctx context.Context,
	client nfs.Client,
	session nfs.Session,
	parentNodeID uint64,
) ([]nfs.Node, error) {
	var (
		nodes  []nfs.Node
		cookie string
	)

	for {
		batch, nextCookie, err := client.ListNodes(ctx, session, parentNodeID, cookie)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, batch...)
		if len(batch) == 0 {
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
) ([]nfs.Node, error) {

	nodes, err := listAllFilest(ctx, client, session, parentNodeID)
	slices.SortFunc(nodes, func(i, j nfs.Node) int {
		if i.Name < j.Name {
			return -1
		}

		if i.Name > j.Name {
			return 1
		}

		return 0
	})

	if err != nil {
		return []nfs.Node{}, err
	}
	result := make([]nfs.Node, 0)
	for _, node := range nodes {
		result = append(result, node)
		if node.Type == nfs_client.NodeType_DIR {
			children, err := dfsNodesTraversal(ctx, client, session, node.NodeID)
			if err != nil {
				return []nfs.Node{}, err
			}

			result = append(result, children...)
		}
	}

	return result, nil
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

	filesystemID := t.Name()
	err := client.Create(ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)
	defer client.Delete(ctx, filesystemID)

	session1, err := client.CreateSession(ctx, filesystemID, false)
	require.NoError(t, err)
	defer client.DestroySession(ctx, session1)

	rootDir := root(
		dir("etc",
			file("passwd"),
			file("hosts"),
		),
		dir(
			"var",
			dir(
				"log",
			),
		),
		symlink("bin", "/usr/bin"),
		dir("usr",
			dir("bin", file("bash"), file("ls")),
			dir("lib", file("libc.so")),
		),
	)
	rootDir.create(t, ctx, client, session1, 1)
	nodes, err := dfsNodesTraversal(ctx, client, session1, 1)
	require.NoError(t, err)

	expectedNames := []string{
		"bin",
		"etc",
		"hosts",
		"passwd",
		"usr",
		"bin",
		"bash",
		"ls",
		"lib",
		"libc.so",
		"var",
		"log",
	}
	require.Equal(t, expectedNames, nodeNames(nodes))
}
