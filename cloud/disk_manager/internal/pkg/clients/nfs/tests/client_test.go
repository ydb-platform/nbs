package tests

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
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

type node struct {
	name     string
	children []node
	fileType nfs_client.NodeType
	target   string
}

func dir(name string, children ...node) node {
	return node{
		name:     name,
		fileType: nfs_client.NODE_KIND_DIR,
		children: children,
	}
}

func file(name string) node {
	return node{
		name:     name,
		fileType: nfs_client.NODE_KIND_FILE,
	}
}

func symlink(name string, target string) node {
	return node{
		name:     name,
		fileType: nfs_client.NODE_KIND_SYMLINK,
		target:   target,
	}
}

func root(children ...node) node {
	return node{
		name:     "/",
		fileType: nfs_client.NODE_KIND_DIR,
		children: children,
	}
}

////////////////////////////////////////////////////////////////////////////////

type fileSystemModel struct {
	root          node
	t             *testing.T
	ctx           context.Context
	client        nfs.Client
	session       nfs.Session
	defaultUid    uint32
	defaultGid    uint32
	directoryMode uint32
	fileMode      uint32
	expectedNodes []nfs.Node
}

func (f *fileSystemModel) createNodes(parentID uint64, nodeToCreate node) {
	mode := f.fileMode
	if nodeToCreate.fileType.IsDirectory() {
		mode = f.directoryMode
	}

	expectedNode := nfs.Node{
		ParentID:   parentID,
		Name:       nodeToCreate.name,
		Type:       nodeToCreate.fileType,
		Mode:       mode,
		UID:        1,
		GID:        1,
		LinkTarget: nodeToCreate.target,
	}
	id, err := f.client.CreateNode(f.ctx, f.session, expectedNode)
	require.NoError(f.t, err)
	f.expectedNodes = append(f.expectedNodes, expectedNode)
	if !nodeToCreate.fileType.IsDirectory() {
		return
	}

	// Sort nodes by name to have a deterministic order
	slices.SortFunc(
		nodeToCreate.children,
		func(i, j node) int {
			return strings.Compare(i.name, j.name)
		},
	)
	for _, child := range nodeToCreate.children {
		f.createNodes(id, child)
	}
}

func (f *fileSystemModel) create() {
	slices.SortFunc(
		f.root.children,
		func(i, j node) int {
			return strings.Compare(i.name, j.name)
		},
	)
	for _, child := range f.root.children {
		f.createNodes(1, child)
	}
}

func (f *fileSystemModel) listAllNodes(parentNodeID uint64) []nfs.Node {
	var (
		nodes  []nfs.Node
		cookie string
	)

	for {
		batch, nextCookie, err := f.client.ListNodes(
			f.ctx,
			f.session,
			parentNodeID,
			cookie,
		)
		require.NoError(f.t, err)
		nodes = append(nodes, batch...)
		if len(batch) == 0 {
			break
		}

		cookie = nextCookie
	}

	return nodes
}
func (f *fileSystemModel) listNodesRecursively(parentNodeID uint64) []nfs.Node {
	nodes := f.listAllNodes(parentNodeID)
	// Sort nodes by name to have a deterministic order
	slices.SortFunc(
		nodes,
		func(i, j nfs.Node) int {
			return strings.Compare(i.Name, j.Name)
		},
	)
	result := make([]nfs.Node, 0)
	for _, node := range nodes {
		result = append(result, node)
		if !node.Type.IsDirectory() {
			continue
		}

		children := f.listNodesRecursively(node.NodeID)
		result = append(result, children...)
	}

	return result
}

func (f *fileSystemModel) listAllNodesRecursively() []nfs.Node {
	return f.listNodesRecursively(1)
}

func newFileSystemModel(
	t *testing.T,
	ctx context.Context,
	client nfs.Client,
	session nfs.Session,
	root node,
) *fileSystemModel {
	return &fileSystemModel{
		root:          root,
		t:             t,
		ctx:           ctx,
		client:        client,
		session:       session,
		defaultUid:    1,
		defaultGid:    1,
		directoryMode: 0o755,
		fileMode:      0o644,
	}
}

////////////////////////////////////////////////////////////////////////////////

func nodeNames(nodes []nfs.Node) []string {
	names := make([]string, len(nodes))
	for i, node := range nodes {
		names[i] = node.Name
	}

	return names
}

////////////////////////////////////////////////////////////////////////////////

func TestListNodesFileSystem(t *testing.T) {
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

	session, err := client.CreateSession(ctx, filesystemID, false)
	require.NoError(t, err)
	defer client.DestroySession(ctx, session)

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
	model := newFileSystemModel(t, ctx, client, session, rootDir)
	model.create()

	nodes := model.listAllNodesRecursively()
	require.Equal(t, len(model.expectedNodes), len(nodes))
	for index, node := range nodes {
		expectedNode := model.expectedNodes[index]
		require.Equal(t, expectedNode.ParentID, node.ParentID)
		require.Equal(t, expectedNode.Name, node.Name)
		require.Equal(t, expectedNode.Type, node.Type)
		require.Equal(t, expectedNode.Mode, node.Mode)
		require.Equal(t, expectedNode.UID, node.UID)
		require.Equal(t, expectedNode.GID, node.GID)
		require.Equal(t, expectedNode.LinkTarget, node.LinkTarget)
	}

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
