package nodes

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

////////////////////////////////////////////////////////////////////////////////

func newYDB(ctx context.Context) (*persistence.YDBClient, error) {
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
	)
	database := "/Root"
	rootPath := "disk_manager"
	connectionTimeout := "10s"

	return persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint:          &endpoint,
			Database:          &database,
			RootPath:          &rootPath,
			ConnectionTimeout: &connectionTimeout,
		},
		metrics.NewEmptyRegistry(),
	)
}

func newStorage(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	storageFolder string,
	deleteLimit int,
) Storage {

	err := schema.Create(ctx, storageFolder, db, false)
	require.NoError(t, err)

	storage := NewStorage(db, storageFolder, deleteLimit)
	require.NotNil(t, storage)

	return storage
}

////////////////////////////////////////////////////////////////////////////////

type fixture struct {
	t             *testing.T
	ctx           context.Context
	cancel        context.CancelFunc
	db            *persistence.YDBClient
	storageFolder string
	storage       Storage
}

func (f *fixture) teardown() {
	err := f.db.Close(f.ctx)
	require.NoError(f.t, err)
	f.cancel()
}

func createFixture(t *testing.T, deleteLimit int) *fixture {
	ctx, cancel := context.WithCancel(test.NewContext())

	db, err := newYDB(ctx)
	require.NoError(t, err)

	storageFolder := fmt.Sprintf(
		"nodes_storage_ydb_test/%v",
		t.Name(),
	)
	storage := newStorage(t, ctx, db, storageFolder, deleteLimit)

	return &fixture{
		t:             t,
		ctx:           ctx,
		cancel:        cancel,
		db:            db,
		storageFolder: storageFolder,
		storage:       storage,
	}
}

////////////////////////////////////////////////////////////////////////////////

func sortNodes(nodes []nfs.Node) {
	sort.Slice(nodes, func(i, j int) bool {
		ni := nfs_client.Node(nodes[i])
		nj := nfs_client.Node(nodes[j])
		return ni.Name < nj.Name
	})
}

func makeNode(
	parentID uint64,
	nodeID uint64,
	name string,
	nodeType nfs_client.NodeType,
) nfs.Node {

	return nfs.Node(nfs_client.Node{
		ParentID: parentID,
		NodeID:   nodeID,
		Name:     name,
		Type:     nodeType,
		Mode:     0o755,
		UID:      1000,
		GID:      1000,
		Atime:    100,
		Mtime:    200,
		Ctime:    300,
		Size:     4096,
		Links:    1,
	})
}

////////////////////////////////////////////////////////////////////////////////

func TestSavedNodesAreListed(t *testing.T) {
	f := createFixture(t, 100)
	defer f.teardown()

	snapshotID := "snapshot-1"
	parentNodeID := uint64(1)

	expected := []nfs.Node{
		makeNode(parentNodeID, 10, "alpha", nfs_client.NODE_KIND_DIR),
		makeNode(parentNodeID, 11, "beta", nfs_client.NODE_KIND_FILE),
		makeNode(parentNodeID, 12, "gamma", nfs_client.NODE_KIND_DIR),
		makeNode(parentNodeID, 13, "delta", nfs_client.NODE_KIND_FILE),
		makeNode(parentNodeID, 14, "epsilon", nfs_client.NODE_KIND_DIR),
		makeNode(parentNodeID, 15, "zeta", nfs_client.NODE_KIND_FILE),
		makeNode(parentNodeID, 16, "eta", nfs_client.NODE_KIND_DIR),
		makeNode(parentNodeID, 17, "theta", nfs_client.NODE_KIND_FILE),
		makeNode(parentNodeID, 18, "iota", nfs_client.NODE_KIND_DIR),
		makeNode(parentNodeID, 19, "kappa", nfs_client.NODE_KIND_FILE),
		makeNode(parentNodeID, 20, "lambda", nfs_client.NODE_KIND_DIR),
		makeNode(parentNodeID, 21, "mu", nfs_client.NODE_KIND_FILE),
	}

	err := f.storage.SaveNodes(f.ctx, snapshotID, expected)
	require.NoError(t, err)

	// List with small limit (5) in a loop using cookie pagination.
	limit := 5
	var collected []nfs.Node
	cookie := ""
	for {
		nodes, nextCookie, err := f.storage.ListNodes(
			f.ctx,
			snapshotID,
			parentNodeID,
			cookie,
			limit,
		)
		require.NoError(t, err)
		collected = append(collected, nodes...)

		if len(nextCookie) == 0 {
			break
		}

		require.LessOrEqual(t, len(nodes), limit)
		cookie = nextCookie
	}

	// Sort both slices and compare.
	sortNodes(expected)
	sortNodes(collected)

	require.Equal(t, len(expected), len(collected))
	for i := range expected {
		e := nfs_client.Node(expected[i])
		c := nfs_client.Node(collected[i])
		require.Equal(t, e.ParentID, c.ParentID)
		require.Equal(t, e.NodeID, c.NodeID)
		require.Equal(t, e.Name, c.Name)
		require.Equal(t, e.Type, c.Type)
		require.Equal(t, e.Mode, c.Mode)
		require.Equal(t, e.UID, c.UID)
		require.Equal(t, e.GID, c.GID)
		require.Equal(t, e.Atime, c.Atime)
		require.Equal(t, e.Mtime, c.Mtime)
		require.Equal(t, e.Ctime, c.Ctime)
		require.Equal(t, e.Size, c.Size)
		require.Equal(t, e.Links, c.Links)
		require.Equal(t, e.LinkTarget, c.LinkTarget)
	}
}

func TestDeleteSnapshotData(t *testing.T) {
	deleteLimit := 2
	f := createFixture(t, deleteLimit)
	defer f.teardown()

	snapshotID := "snapshot-delete"
	parentNodeID := uint64(1)

	nodes := make([]nfs.Node, 0, 10)
	for i := 0; i < 10; i++ {
		nodes = append(
			nodes,
			makeNode(
				parentNodeID,
				uint64(100+i),
				fmt.Sprintf("node-%02d", i),
				nfs_client.NODE_KIND_FILE,
			),
		)
	}

	err := f.storage.SaveNodes(f.ctx, snapshotID, nodes)
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		done, err := f.storage.DeleteSnapshotData(f.ctx, snapshotID)
		require.NoError(t, err)
		require.False(t, done)
	}

	done, err := f.storage.DeleteSnapshotData(f.ctx, snapshotID)
	require.NoError(t, err)
	require.True(t, done)

	// Verify listing returns nothing.
	listed, nextCookie, err := f.storage.ListNodes(
		f.ctx,
		snapshotID,
		parentNodeID,
		"",
		100,
	)
	require.NoError(t, err)
	require.Empty(t, listed)
	require.Empty(t, nextCookie)
}

func TestGetDestinationNodeID(t *testing.T) {
	f := createFixture(t, 100)
	defer f.teardown()

	srcSnapshotID := "src-snapshot"
	dstFilesystemID := "dst-filesystem"

	// Get from empty table returns ok=false.
	_, ok, err := f.storage.GetDestinationNodeID(
		f.ctx,
		srcSnapshotID,
		dstFilesystemID,
		42,
	)
	require.NoError(t, err)
	require.False(t, ok)

	// Save some mappings.
	srcNodeIds := []uint64{100, 200, 300}
	dstNodeIds := []uint64{1000, 2000, 3000}

	err = f.storage.UpdateRestorationNodeIDMapping(
		f.ctx,
		srcSnapshotID,
		dstFilesystemID,
		srcNodeIds,
		dstNodeIds,
	)
	require.NoError(t, err)

	// Get nonexistent id from populated table returns ok=false.
	_, ok, err = f.storage.GetDestinationNodeID(
		f.ctx,
		srcSnapshotID,
		dstFilesystemID,
		999,
	)
	require.NoError(t, err)
	require.False(t, ok)

	// Get each saved mapping returns correct destination node id.
	for i, srcNodeID := range srcNodeIds {
		dstNodeID, ok, err := f.storage.GetDestinationNodeID(
			f.ctx,
			srcSnapshotID,
			dstFilesystemID,
			srcNodeID,
		)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, dstNodeIds[i], dstNodeID)
	}
}
