package testing

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

func NewContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func GetEndpoint() string {
	return fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_NFS_PORT"),
	)
}

////////////////////////////////////////////////////////////////////////////////

func NewFactory(ctx context.Context) nfs.Factory {
	clientTimeout := "60s"
	rootCertsFile := os.Getenv("DISK_MANAGER_RECIPE_ROOT_CERTS_FILE")
	return nfs.NewFactory(
		ctx,
		&config.ClientConfig{
			Zones: map[string]*config.Zone{
				"zone": {
					Endpoints: []string{GetEndpoint()},
				},
			},
			RootCertsFile:        &rootCertsFile,
			DurableClientTimeout: &clientTimeout,
		},
		metrics.NewEmptyRegistry(),
	)
}

func NewClient(t *testing.T, ctx context.Context) nfs.Client {
	factory := NewFactory(ctx)
	client, err := factory.NewClient(ctx, "zone")
	require.NoError(t, err)
	return client
}

////////////////////////////////////////////////////////////////////////////////

type FilesystemLayerConfig struct {
	DirsCount  int
	FilesCount int
}

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

// TODO (jkuradobery): support sockets, devices, etc.
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

func generateDirectoryRecursive(
	name string,
	maxDepth int,
	countFunc func(maxDepth int) (nDirs int, nFiles int),
	parentNumber int,
) Node {

	if maxDepth <= 0 {
		return Dir(name)
	}

	nDirs, nFiles := countFunc(maxDepth)
	children := make([]Node, 0, nDirs+nFiles)
	for i := 0; i < nDirs; i++ {
		children = append(
			children,
			generateDirectoryRecursive(
				fmt.Sprintf("dir_%d_%d_parent_%d", i, maxDepth, parentNumber),
				maxDepth-1,
				countFunc,
				i,
			),
		)
	}

	for i := 0; i < nFiles; i++ {
		children = append(
			children,
			File(
				fmt.Sprintf(
					"file_%d_%d_parent_%d",
					i,
					maxDepth,
					parentNumber,
				),
			),
		)
	}

	return Dir(name, children...)
}

func RandomDirectoryTree(maxDepth, maxDirsPerDir, maxFilesPerDir int) Node {
	return Root(
		generateDirectoryRecursive(
			"base",
			maxDepth,
			func(maxDepth int) (nDirs int, nFiles int) {
				return rand.Intn(maxDirsPerDir + 1), rand.Intn(maxFilesPerDir + 1)
			},
			0,
		),
	)
}

////////////////////////////////////////////////////////////////////////////////

func HomogeneousDirectoryTree(layers []FilesystemLayerConfig) Node {
	maxDepth := len(layers)
	return Root(
		generateDirectoryRecursive(
			"base",
			maxDepth,
			func(maxDepth int) (nDirs int, nFiles int) {
				layerIndex := len(layers) - maxDepth
				if layerIndex < 0 || layerIndex >= len(layers) {
					return 0, 0
				}

				layer := layers[layerIndex]
				return layer.DirsCount, layer.FilesCount
			},
			0,
		),
	)
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

func (f *FileSystemModel) CreateNodesRecursively(
	parentID uint64,
	nodeToCreate Node,
) {

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

	// Sort nodes by name to have a deterministic order
	slices.SortFunc(
		nodeToCreate.Children,
		func(i, j Node) int {
			return strings.Compare(i.Name, j.Name)
		},
	)
	for _, child := range nodeToCreate.Children {
		f.CreateNodesRecursively(id, child)
	}
}

func (f *FileSystemModel) CreateAllNodesRecursively() {
	slices.SortFunc(
		f.root.Children,
		func(i, j Node) int {
			return strings.Compare(i.Name, j.Name)
		},
	)
	for _, child := range f.root.Children {
		f.CreateNodesRecursively(nfs.RootNodeID, child)
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

		children := f.ListNodesRecursively(node.NodeID)
		result = append(result, children...)
	}

	return result
}

func (f *FileSystemModel) ListAllNodesRecursively() []nfs.Node {
	return f.ListNodesRecursively(nfs.RootNodeID)
}

func (f *FileSystemModel) SetSession(session nfs.Session) {
	f.session = session
}

func (f *FileSystemModel) Close() {
	err := f.client.DestroySession(f.ctx, f.session)
	require.NoError(f.t, err)
}

func (f *FileSystemModel) ExpectedNodeNames() *tasks_common.StringSet {
	result := tasks_common.NewStringSet()
	for _, node := range f.ExpectedNodes {
		result.Add(node.Name)
	}

	return &result
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

////////////////////////////////////////////////////////////////////////////////

type ParallelFilesystemModel struct {
	ctx           context.Context
	client        nfs.Client
	session       nfs.Session
	t             *testing.T
	rootNode      Node
	mu            sync.Mutex
	ExpectedNames *tasks_common.StringSet
}

func (m *ParallelFilesystemModel) createChildren(
	parentID uint64,
	children []Node,
) {
	var childDirectories []struct {
		parent   uint64
		children []Node
	}
	eg, _ := errgroup.WithContext(m.ctx)
	eg.SetLimit(100)
	for _, child := range children {
		child := child
		eg.Go(func() error {
			id, err := m.client.CreateNode(
				m.ctx,
				m.session,
				nfs.Node{
					ParentID:   parentID,
					Name:       child.Name,
					Type:       child.FileType,
					Mode:       0o777,
					UID:        1,
					GID:        1,
					LinkTarget: child.Target,
				},
			)
			if err != nil {
				return err
			}

			m.mu.Lock()
			defer m.mu.Unlock()
			m.ExpectedNames.Add(child.Name)
			if child.FileType.IsDirectory() {
				childDirectories = append(
					childDirectories,
					struct {
						parent   uint64
						children []Node
					}{
						parent:   id,
						children: child.Children,
					},
				)
			}

			return nil
		})
	}
	require.NoError(m.t, eg.Wait())

	for _, dir := range childDirectories {
		m.createChildren(dir.parent, dir.children)
	}
}

func (m *ParallelFilesystemModel) ExpectedNodeNames() *tasks_common.StringSet {
	return m.ExpectedNames
}

func (m *ParallelFilesystemModel) CreateAllNodesRecursively() {
	m.createChildren(nfs.RootNodeID, m.rootNode.Children)
}

func (m *ParallelFilesystemModel) Close() {
	err := m.client.DestroySession(m.ctx, m.session)
	require.NoError(m.t, err)
}

func NewParallelFilesystemModel(
	t *testing.T,
	ctx context.Context,
	client nfs.Client,
	session nfs.Session,
	rootDir Node,
) *ParallelFilesystemModel {
	set := tasks_common.NewStringSet()
	return &ParallelFilesystemModel{
		t:             t,
		ctx:           ctx,
		client:        client,
		session:       session,
		rootNode:      rootDir,
		ExpectedNames: &set,
	}
}

////////////////////////////////////////////////////////////////////////////////

type FilesystemModelInterface interface {
	CreateAllNodesRecursively()
	ExpectedNodeNames() *tasks_common.StringSet
	Close()
}
