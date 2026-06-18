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
	deleteLimit uint64,
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

func createFixture(t *testing.T, deleteLimit uint64) *fixture {
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

func makeNodeWithShard(
	parentID uint64,
	nodeID uint64,
	name string,
	nodeType nfs_client.NodeType,
	shardID string,
	shardNodeName string,
) nfs.Node {

	return nfs.Node(nfs_client.Node{
		ParentID:          parentID,
		NodeID:            nodeID,
		Name:              name,
		Type:              nodeType,
		Mode:              0o641,
		UID:               uint32(1000 + nodeID),
		GID:               uint32(2000 + nodeID),
		Atime:             10000 + nodeID,
		Mtime:             20000 + nodeID,
		Ctime:             30000 + nodeID,
		Size:              40000 + nodeID,
		Links:             1,
		ShardFileSystemID: shardID,
		ShardNodeName:     shardNodeName,
		// Parameters are unrealistic but used to ensure
		// that all the values are retrieved from storage correctly.
		LinkTarget: "link-target-" + name,
		DevID:      50000 + nodeID,
	})
}

func makeNodeRestoredAsRef(
	parentID uint64,
	nodeID uint64,
	name string,
	nodeType nfs_client.NodeType,
	shardID string,
	shardNodeName string,
) nfs.Node {

	result := makeNodeWithShard(
		parentID,
		nodeID,
		name,
		nodeType,
		shardID,
		shardNodeName,
	)
	result.NodeID = 0

	return result
}

func makeNodeRestoredAsNode(
	nodeID uint64,
	name string,
	nodeType nfs_client.NodeType,
	shardNodeName string,
) nfs.Node {

	result := makeNodeWithShard(
		nfs.RootNodeID,
		nodeID,
		name,
		nodeType,
		"",
		"",
	)
	result.Name = shardNodeName

	return result
}

////////////////////////////////////////////////////////////////////////////////

func TestSavedNodesAreListed(t *testing.T) {
	f := createFixture(t, 100)
	defer f.teardown()

	snapshotID := "snapshot-1"
	parentNodeID := uint64(1)

	expected := []nfs.Node{
		makeNode(parentNodeID, 10, "alpha", nfs_client.NODE_KIND_DIR),
		makeNode(parentNodeID, 11, "beta", nfs_client.NODE_KIND_BLOCKDEV),
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
	expected[1].DevID = 777
	expected[3].ShardNodeName = "shard-node"
	expected[3].ShardFileSystemID = "shard-42"

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

func TestListNodesByShardReturnsEmptyForNodesWithoutShard(t *testing.T) {
	f := createFixture(t, 100)
	defer f.teardown()

	snapshotID := "snapshot-without-shards"
	nodes := []nfs.Node{
		makeNodeWithShard(1, 10, "alpha", nfs_client.NODE_KIND_DIR, "", ""),
		makeNodeWithShard(1, 11, "beta", nfs_client.NODE_KIND_FILE, "", ""),
		makeNodeWithShard(10, 12, "gamma", nfs_client.NODE_KIND_FILE, "", ""),
	}
	err := f.storage.SaveNodes(f.ctx, snapshotID, nodes)
	require.NoError(t, err)

	nodesByShard, nextCookie, err := f.storage.ListNodesByShard(
		f.ctx,
		snapshotID,
		"",
		100,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, nodesByShard)
	require.Nil(t, nextCookie)
}

func TestListNodesByShardReturnsShardAndParentRefs(t *testing.T) {
	f := createFixture(t, 100)
	defer f.teardown()

	snapshotID := "snapshot-list-by-shard"

	parentA := makeNodeWithShard(
		1,
		10,
		"parent-a",
		nfs_client.NODE_KIND_DIR,
		"shard-1",
		"parent-a-in-shard",
	)
	parentB := makeNodeWithShard(
		1,
		20,
		"parent-b",
		nfs_client.NODE_KIND_DIR,
		"shard-2",
		"parent-b-in-shard",
	)

	childA := makeNodeWithShard(
		10,
		30,
		"child-a",
		nfs_client.NODE_KIND_FILE,
		"shard-3",
		"child-a-in-shard",
	)
	childSameShard := makeNodeWithShard(
		10,
		31,
		"child-same-shard",
		nfs_client.NODE_KIND_FILE,
		"shard-1",
		"child-same-shard-in-shard",
	)
	childB := makeNodeWithShard(
		20,
		40,
		"child-b",
		nfs_client.NODE_KIND_FILE,
		"shard-3",
		"child-b-in-shard",
	)

	err := f.storage.SaveNodes(
		f.ctx,
		snapshotID,
		[]nfs.Node{
			parentA,
			parentB,
		},
	)
	require.NoError(t, err)

	err = f.storage.SaveNodes(
		f.ctx,
		snapshotID,
		[]nfs.Node{
			childA,
			childSameShard,
			childB,
		},
	)
	require.NoError(t, err)

	expectedShard1 := []nfs.Node{
		makeNodeRestoredAsNode(
			10,
			"parent-a",
			nfs_client.NODE_KIND_DIR,
			"parent-a-in-shard",
		),
		makeNodeRestoredAsRef(
			10,
			30,
			"child-a",
			nfs_client.NODE_KIND_FILE,
			"shard-3",
			"child-a-in-shard",
		),
		makeNodeRestoredAsNode(
			31,
			"child-same-shard",
			nfs_client.NODE_KIND_FILE,
			"child-same-shard-in-shard",
		),
		makeNodeRestoredAsRef(
			10,
			31,
			"child-same-shard",
			nfs_client.NODE_KIND_FILE,
			"shard-1",
			"child-same-shard-in-shard",
		),
	}
	expectedShard2 := []nfs.Node{
		makeNodeRestoredAsNode(
			20,
			"parent-b",
			nfs_client.NODE_KIND_DIR,
			"parent-b-in-shard",
		),
		makeNodeRestoredAsRef(
			20,
			40,
			"child-b",
			nfs_client.NODE_KIND_FILE,
			"shard-3",
			"child-b-in-shard",
		),
	}
	expectedShard3 := []nfs.Node{
		makeNodeRestoredAsNode(
			30,
			"child-a",
			nfs_client.NODE_KIND_FILE,
			"child-a-in-shard",
		),
		makeNodeRestoredAsNode(
			40,
			"child-b",
			nfs_client.NODE_KIND_FILE,
			"child-b-in-shard",
		),
	}

	nodesByShard, nextCookie, err := f.storage.ListNodesByShard(
		f.ctx,
		snapshotID,
		"shard-1",
		100,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, expectedShard1, nodesByShard)
	require.Nil(t, nextCookie)

	nodesByShard, nextCookie, err = f.storage.ListNodesByShard(
		f.ctx,
		snapshotID,
		"shard-1",
		1,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, expectedShard1[:1], nodesByShard)
	require.Equal(t, &NodeRefsByShardCookie{
		ParentNodeID: 10,
		Name:         "child-a",
		StoreAsChild: true,
	}, nextCookie)

	nodesByShard, nextCookie, err = f.storage.ListNodesByShard(
		f.ctx,
		snapshotID,
		"shard-1",
		100,
		nextCookie,
	)
	require.NoError(t, err)
	require.Equal(t, expectedShard1[1:], nodesByShard)
	require.Nil(t, nextCookie)

	nodesByShard, nextCookie, err = f.storage.ListNodesByShard(
		f.ctx,
		snapshotID,
		"shard-2",
		100,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, expectedShard2, nodesByShard)
	require.Nil(t, nextCookie)

	nodesByShard, nextCookie, err = f.storage.ListNodesByShard(
		f.ctx,
		snapshotID,
		"shard-3",
		100,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, expectedShard3, nodesByShard)
	require.Nil(t, nextCookie)
}

func TestListNodeRefsByShard(t *testing.T) {
	f := createFixture(t, 100)
	defer f.teardown()

	snapshotID := "snapshot-list-node-refs-by-shard"
	shardFilesystemID := "shard-list-node-refs"
	nodeCount := 100000

	nodes := make([]nfs.Node, 0, nodeCount)
	expected := make([]nfs.Node, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodes = append(nodes, makeNodeWithShard(
			nfs.RootNodeID,
			uint64(i+10),
			fmt.Sprintf("node-%06d", i),
			nfs_client.NODE_KIND_FILE,
			shardFilesystemID,
			fmt.Sprintf("shard-node-%06d", i),
		))
		expected = append(expected, makeNodeRestoredAsNode(
			uint64(i+10),
			fmt.Sprintf("node-%06d", i),
			nfs_client.NODE_KIND_FILE,
			fmt.Sprintf("shard-node-%06d", i),
		))
	}

	err := f.storage.SaveNodes(f.ctx, snapshotID, nodes)
	require.NoError(t, err)

	pageSize := uint64(100)
	var collected []nfs.Node
	var cookie *NodeRefsByShardCookie
	for {
		nodesByShard, nextCookie, err := f.storage.ListNodesByShard(
			f.ctx,
			snapshotID,
			shardFilesystemID,
			pageSize,
			cookie,
		)
		require.NoError(t, err)

		collected = append(collected, nodesByShard...)
		if nextCookie == nil {
			break
		}

		require.Len(t, nodesByShard, int(pageSize))
		cookie = nextCookie
	}

	require.Equal(t, expected, collected)
}

func TestDeleteSnapshotData(t *testing.T) {
	deleteLimit := uint64(2)
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
	nodes[0].ShardFileSystemID = "delete-shard"
	nodes[0].ShardNodeName = "delete-shard-node"

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

	nodesByShard, nextShardCookie, err := f.storage.ListNodesByShard(
		f.ctx,
		snapshotID,
		"delete-shard",
		100,
		nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, nodesByShard)
	require.Nil(t, nextShardCookie)

	err = f.storage.DeleteSnapshotData(f.ctx, snapshotID)
	require.NoError(t, err)

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

	nodesByShard, nextShardCookie, err = f.storage.ListNodesByShard(
		f.ctx,
		snapshotID,
		"delete-shard",
		100,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, nodesByShard)
	require.Nil(t, nextShardCookie)
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

func TestCleanupRestorationNodeIDsMapping(t *testing.T) {
	f := createFixture(t, 100)
	defer f.teardown()

	snapshotID := "snapshot-cleanup"
	dstFilesystemID := "dst-filesystem"
	otherDstFilesystemID := "other-dst-filesystem"

	nodeIDMapping := make(map[uint64]uint64, 10000)
	for i := uint64(0); i < 10000; i++ {
		nodeIDMapping[i] = i + 100000
	}

	err := f.storage.UpdateRestorationNodeIDMapping(
		f.ctx,
		snapshotID,
		dstFilesystemID,
		nodeIDMapping,
	)
	require.NoError(t, err)

	err = f.storage.UpdateRestorationNodeIDMapping(
		f.ctx,
		snapshotID,
		otherDstFilesystemID,
		nodeIDMapping,
	)
	require.NoError(t, err)

	srcNodeIDs := make([]uint64, 10000)
	for i := uint64(0); i < 10000; i++ {
		srcNodeIDs[i] = i
	}

	mapping, err := f.storage.GetDestinationNodeIDs(
		f.ctx,
		snapshotID,
		dstFilesystemID,
		srcNodeIDs,
	)
	require.NoError(t, err)
	require.Len(t, mapping, 10000)

	err = f.storage.CleanupRestorationNodeIDsMapping(
		f.ctx,
		snapshotID,
		dstFilesystemID,
	)
	require.NoError(t, err)

	mapping, err = f.storage.GetDestinationNodeIDs(
		f.ctx,
		snapshotID,
		dstFilesystemID,
		srcNodeIDs,
	)
	require.NoError(t, err)
	require.Empty(t, mapping)

	mapping, err = f.storage.GetDestinationNodeIDs(
		f.ctx,
		snapshotID,
		otherDstFilesystemID,
		srcNodeIDs,
	)
	require.NoError(t, err)
	require.Len(t, mapping, 10000)
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
		a := actual[i]
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

func TestLister(t *testing.T) {
	const totalNodes = 10000
	const nodesPerListing = 2000

	f := createFixture(t, 100)
	defer f.teardown()

	snapshotID := "snapshot-lister"
	parentNodeID := uint64(1)

	var nodes []nfs.Node
	for nodeID := uint64(0); nodeID < totalNodes; nodeID++ {
		node := makeNode(
			parentNodeID,
			nodeID,
			fmt.Sprintf("node-%05d", nodeID),
			nfs_client.NODE_KIND_FILE,
		)
		nodes = append(nodes, node)
	}

	err := f.storage.SaveNodes(f.ctx, snapshotID, nodes)
	require.NoError(t, err)

	factory := NewNodeStorageListerFactory(f.storage, nodesPerListing)

	lister, err := factory.CreateLister(f.ctx, "", snapshotID)
	require.NoError(t, err)

	var collected []nfs.Node
	cookie := ""
	iterations := 0
	for {
		nodes, nextCookie, err := lister.ListNodes(f.ctx, parentNodeID, cookie)
		require.NoError(t, err)
		collected = append(collected, nodes...)
		iterations++

		if len(nextCookie) == 0 {
			break
		}
		cookie = nextCookie
	}

	require.Equal(t, totalNodes/nodesPerListing, iterations)
	require.Len(t, collected, totalNodes)
	for i, node := range collected {
		expectedNodeID := uint64(i)
		require.Equal(t, expectedNodeID, node.NodeID)
		require.Equal(t, fmt.Sprintf("node-%05d", expectedNodeID), node.Name)
	}
}
