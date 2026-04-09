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

	sortNodes(expected)
	sortNodes(collected)

	require.ElementsMatch(t, expected, collected)
}

func TestDeleteSnapshotData(t *testing.T) {
	deleteLimit := 2
	f := createFixture(t, deleteLimit)
	defer f.teardown()

	snapshotID := "snapshot-delete"
	dstFilesystemID := "dst-filesystem"
	parentNodeID := uint64(1)

	nodes := make([]nfs.Node, 0, 10)
	for i := 0; i < 8; i++ {
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

	nodes = append(nodes,
		makeHardlinkNode(parentNodeID, 200, "hardlink-a", 2),
		makeHardlinkNode(parentNodeID, 200, "hardlink-b", 2),
	)

	err := f.storage.SaveNodes(f.ctx, snapshotID, nodes)
	require.NoError(t, err)

	srcNodeIds := []uint64{100, 101}
	nodeIDMapping := map[uint64]uint64{
		100: 1000,
		101: 1001,
	}
	err = f.storage.UpdateRestorationNodeIDMapping(
		f.ctx,
		snapshotID,
		dstFilesystemID,
		nodeIDMapping,
	)
	require.NoError(t, err)

	hardlinks, err := f.storage.ListHardLinks(f.ctx, snapshotID, 100, 0)
	require.NoError(t, err)
	require.NotEmpty(t, hardlinks)

	mapping, err := f.storage.GetDestinationNodeIDs(
		f.ctx,
		snapshotID,
		dstFilesystemID,
		srcNodeIds,
	)
	require.NoError(t, err)
	require.NotEmpty(t, mapping)

	listed, _, err := f.storage.ListNodes(
		f.ctx,
		snapshotID,
		parentNodeID,
		"",
		100,
	)
	require.NoError(t, err)
	require.NotEmpty(t, listed)

	for i := 0; i < 5; i++ {
		done, err := f.storage.DeleteSnapshotData(f.ctx, snapshotID)
		require.NoError(t, err)
		require.False(t, done)
	}

	done, err := f.storage.DeleteSnapshotData(f.ctx, snapshotID)
	require.NoError(t, err)
	require.True(t, done)

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

	hardlinks, err = f.storage.ListHardLinks(f.ctx, snapshotID, 100, 0)
	require.NoError(t, err)
	require.Empty(t, hardlinks)

	mapping, err = f.storage.GetDestinationNodeIDs(
		f.ctx,
		snapshotID,
		dstFilesystemID,
		srcNodeIds,
	)
	require.NoError(t, err)
	require.Empty(t, mapping)
}

func TestGetDestinationNodeIDs(t *testing.T) {
	f := createFixture(t, 100)
	defer f.teardown()

	srcSnapshotID := "src-snapshot"
	dstFilesystemID := "dst-filesystem"

	result, err := f.storage.GetDestinationNodeIDs(
		f.ctx,
		srcSnapshotID,
		dstFilesystemID,
		[]uint64{42},
	)
	require.NoError(t, err)
	require.Empty(t, result)

	srcNodeIds := []uint64{100, 200, 300}
	dstNodeIds := []uint64{1000, 2000, 3000}
	nodeIDMapping := map[uint64]uint64{
		100: 1000,
		200: 2000,
		300: 3000,
	}

	err = f.storage.UpdateRestorationNodeIDMapping(
		f.ctx,
		srcSnapshotID,
		dstFilesystemID,
		nodeIDMapping,
	)
	require.NoError(t, err)

	result, err = f.storage.GetDestinationNodeIDs(
		f.ctx,
		srcSnapshotID,
		dstFilesystemID,
		[]uint64{999},
	)
	require.NoError(t, err)
	require.Empty(t, result)

	result, err = f.storage.GetDestinationNodeIDs(
		f.ctx,
		srcSnapshotID,
		dstFilesystemID,
		srcNodeIds,
	)
	require.NoError(t, err)
	require.Len(t, result, 3)
	for i, srcNodeID := range srcNodeIds {
		require.Equal(t, dstNodeIds[i], result[srcNodeID])
	}

	result, err = f.storage.GetDestinationNodeIDs(
		f.ctx,
		srcSnapshotID,
		dstFilesystemID,
		[]uint64{100, 999, 300},
	)
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, uint64(1000), result[100])
	require.Equal(t, uint64(3000), result[300])

	result, err = f.storage.GetDestinationNodeIDs(
		f.ctx,
		srcSnapshotID,
		dstFilesystemID,
		[]uint64{},
	)
	require.NoError(t, err)
	require.Empty(t, result)
}

func makeHardlinkNode(
	parentID uint64,
	nodeID uint64,
	name string,
	links uint32,
) nfs.Node {

	return nfs.Node(nfs_client.Node{
		ParentID: parentID,
		NodeID:   nodeID,
		Name:     name,
		Type:     nfs_client.NODE_KIND_FILE,
		Mode:     0o755,
		UID:      1000,
		GID:      1000,
		Atime:    100,
		Mtime:    200,
		Ctime:    300,
		Size:     4096,
		Links:    links,
	})
}

func compareNodes(
	t *testing.T,
	expected []nfs.Node,
	actual []nfs.Node,
) {

	require.Equal(t, len(expected), len(actual))
	for i := range expected {
		e := expected[i]
		// nodeType is not stored in the hardlinks table, it is deduced by
		// order of creation, so we won't check it in this test.
		e.Type = 0
		a := actual[i]
		a.Type = 0
		require.Equal(t, e, a)
	}
}

func TestListHardLinks(t *testing.T) {
	f := createFixture(t, 100)
	defer f.teardown()

	snapshotID := "snapshot-hardlinks"

	// List of nodes, ordered by (node_id, parent_node_id, name).
	nodes := []nfs.Node{
		makeHardlinkNode(1, 10, "hardlink_a", 2),
		makeHardlinkNode(2, 10, "hardlink_b", 2),
		makeHardlinkNode(1, 20, "hardlink_c", 3),
		makeHardlinkNode(2, 20, "hardlink_d", 3),
		makeHardlinkNode(3, 20, "hardlink_e", 3),
		makeHardlinkNode(1, 30, "regular_file", 1),
	}

	err := f.storage.SaveNodes(f.ctx, snapshotID, nodes)
	require.NoError(t, err)

	// Expected hardlinks: all nodes except the last one (links=1).
	expected := nodes[:len(nodes)-1]

	allHardlinks, err := f.storage.ListHardLinks(f.ctx, snapshotID, 100, 0)
	require.NoError(t, err)
	compareNodes(t, expected, allHardlinks)

	// Select one by one with limit=1 and increasing offset.
	var collected []nfs.Node
	for offset := 0; ; offset++ {
		batch, err := f.storage.ListHardLinks(f.ctx, snapshotID, 1, offset)
		require.NoError(t, err)

		if len(batch) == 0 {
			break
		}

		collected = append(collected, batch...)
	}

	compareNodes(t, expected, collected)
}
