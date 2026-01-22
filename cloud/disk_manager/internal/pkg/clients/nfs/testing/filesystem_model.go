package testing

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
)

////////////////////////////////////////////////////////////////////////////////

const RootNodeID = uint64(1)

////////////////////////////////////////////////////////////////////////////////

type Node struct {
	Name     string
	Children []Node
	FileType nfs_client.NodeType
	Target   string
}

func Dir(name string, children ...Node) Node {
	return Node{
		Name:     name,
		FileType: nfs_client.NODE_KIND_DIR,
		Children: children,
	}
}

func File(name string) Node {
	return Node{
		Name:     name,
		FileType: nfs_client.NODE_KIND_FILE,
	}
}

func Symlink(name string, target string) Node {
	return Node{
		Name:     name,
		FileType: nfs_client.NODE_KIND_SYMLINK,
		Target:   target,
	}
}

func Root(children ...Node) Node {
	return Node{
		Name:     "/",
		FileType: nfs_client.NODE_KIND_DIR,
		Children: children,
	}
}

////////////////////////////////////////////////////////////////////////////////

type FileSystemModel struct {
	root          Node
	t             *testing.T
	ctx           context.Context
	client        nfs.Client
	session       nfs.Session
	defaultUid    uint32
	defaultGid    uint32
	directoryMode uint32
	fileMode      uint32
	ExpectedNodes []nfs.Node
}

func (f *FileSystemModel) CreateNodes(parentID uint64, nodeToCreate Node) {
	mode := f.fileMode
	if nodeToCreate.FileType.IsDirectory() {
		mode = f.directoryMode
	}

	expectedNode := nfs.Node{
		ParentID:   parentID,
		Name:       nodeToCreate.Name,
		Type:       nodeToCreate.FileType,
		Mode:       mode,
		UID:        1,
		GID:        1,
		LinkTarget: nodeToCreate.Target,
	}
	id, err := f.client.CreateNode(f.ctx, f.session, expectedNode)
	require.NoError(f.t, err)
	f.ExpectedNodes = append(f.ExpectedNodes, expectedNode)
	if !nodeToCreate.FileType.IsDirectory() {
		return
	}

	slices.SortFunc(
		nodeToCreate.Children,
		func(i, j Node) int {
			return strings.Compare(i.Name, j.Name)
		},
	)
	for _, child := range nodeToCreate.Children {
		f.CreateNodes(id, child)
	}
}

func (f *FileSystemModel) Create() {
	slices.SortFunc(
		f.root.Children,
		func(i, j Node) int {
			return strings.Compare(i.Name, j.Name)
		},
	)
	for _, child := range f.root.Children {
		f.CreateNodes(RootNodeID, child)
	}
}

func (f *FileSystemModel) ListAllNodes(parentNodeID uint64) []nfs.Node {
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
		for index := range batch {
			if !batch[index].Type.IsSymlink() {
				continue
			}

			target, err := f.client.ReadLink(
				f.ctx,
				f.session,
				batch[index].NodeID,
			)
			require.NoError(f.t, err)
			batch[index].LinkTarget = string(target)
		}

		nodes = append(nodes, batch...)
		if len(batch) == 0 {
			break
		}

		if nextCookie == "" {
			break
		}

		cookie = nextCookie
	}

	return nodes
}

func (f *FileSystemModel) ListNodesRecursively(parentNodeID uint64) []nfs.Node {
	nodes := f.ListAllNodes(parentNodeID)
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

		children := f.ListNodesRecursively(node.NodeID)
		result = append(result, children...)
	}

	return result
}

func (f *FileSystemModel) ListAllNodesRecursively() []nfs.Node {
	return f.ListNodesRecursively(RootNodeID)
}

func (f *FileSystemModel) SetSession(session nfs.Session) {
	f.session = session
}

func NewFileSystemModel(
	t *testing.T,
	ctx context.Context,
	client nfs.Client,
	session nfs.Session,
	root Node,
) *FileSystemModel {

	return &FileSystemModel{
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

func NodeNames(nodes []nfs.Node) []string {
	names := make([]string, len(nodes))
	for i, node := range nodes {
		names[i] = node.Name
	}

	return names
}
