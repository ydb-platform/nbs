package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/testing"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

func TestCreateFilesystem(t *testing.T) {
	ctx := nfs_testing.NewContext()
	client := nfs_testing.NewClient(t, ctx)

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
	ctx := nfs_testing.NewContext()
	client := nfs_testing.NewClient(t, ctx)

	filesystemID := t.Name()

	err := client.Create(ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)

	err = client.Delete(ctx, filesystemID, false)
	require.NoError(t, err)

	// Deleting the same filesystem twice is not an error
	err = client.Delete(ctx, filesystemID, false)
	require.NoError(t, err)

	// Deleting non-existent filesystem is also not an error
	err = client.Delete(ctx, filesystemID+"_does_not_exist", false)
	require.NoError(t, err)
}

func TestCreateCheckpoint(t *testing.T) {
	ctx := nfs_testing.NewContext()
	client := nfs_testing.NewClient(t, ctx)

	filesystemID := t.Name()

	err := client.Create(ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)
	defer client.Delete(ctx, filesystemID, false)

	checkpointID := "checkpoint_1"
	nodeID := uint64(0)
	session, err := client.CreateSession(ctx, filesystemID, "", false)
	require.NoError(t, err)
	defer session.Close(ctx)

	err = session.CreateCheckpoint(
		ctx,
		filesystemID,
		checkpointID,
		nodeID,
	)
	require.NoError(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func TestListNodesFileSystem(t *testing.T) {
	ctx := nfs_testing.NewContext()
	// With high CPU consumption on test vm createsession can take longer than
	// default timeout.
	client := nfs_testing.NewClient(t, ctx)

	filesystemID := t.Name()
	err := client.Create(ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)
	defer client.Delete(ctx, filesystemID, false)

	session, err := client.CreateSession(ctx, filesystemID, "", false)
	require.NoError(t, err)
	defer session.Close(ctx)

	model := nfs_testing.NewFileSystemModel(
		t,
		ctx,
		session,
		nfs_testing.StandardFilesystem,
	)
	model.CreateAllNodesRecursively()

	nodes := model.ListAllNodesRecursively(false)
	model.RequireNodesEqual(nodes, false)
	require.Equal(
		t,
		nfs_testing.StandardFilesystemExpectedNames,
		nfs_testing.NodeNames(nodes),
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestListNodesFileSystemUnsafe(t *testing.T) {
	ctx := nfs_testing.NewContext()
	client := nfs_testing.NewClient(t, ctx)

	filesystemID := t.Name()
	shardCount := uint32(5)
	err := client.Create(ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
		ShardCount:  shardCount,
	})
	require.NoError(t, err)
	defer client.Delete(ctx, filesystemID, false)

	err = client.EnableDirectoryCreationInShards(ctx, filesystemID, shardCount)
	require.NoError(t, err)

	session, err := client.CreateSession(ctx, filesystemID, "", false)
	require.NoError(t, err)
	defer session.Close(ctx)

	model := nfs_testing.NewFileSystemModel(
		t,
		ctx,
		session,
		nfs_testing.StandardFilesystem,
	)
	model.CreateAllNodesRecursively()

	nodes := model.ListAllNodesRecursively(true)
	model.RequireNodesEqual(nodes, false)
	require.Equal(
		t,
		nfs_testing.StandardFilesystemExpectedNames,
		nfs_testing.NodeNames(nodes),
	)

	// Open sessions to shard filesystems for GetNodeAttr verification.
	shardSessions := make(map[string]nfs.Session, shardCount)
	for i := uint32(1); i <= shardCount; i++ {
		shardFsID := fmt.Sprintf("%s_s%d", filesystemID, i)
		shardSession, err := client.CreateSession(ctx, shardFsID, "", true)
		require.NoError(t, err)
		defer shardSession.Close(ctx)
		shardSessions[shardFsID] = shardSession
	}

	for _, node := range nodes {
		require.NotEmpty(t, node.ShardNodeName)
		require.NotEmpty(t, node.ShardFileSystemID)
		require.NotEqual(t, filesystemID, node.ShardFileSystemID)

		shardSession, ok := shardSessions[node.ShardFileSystemID]
		require.True(t, ok, "shard session not found for %s", node.ShardFileSystemID)

		shardNode, err := shardSession.GetNodeAttr(
			ctx,
			nfs.RootNodeID,
			node.ShardNodeName,
		)
		require.NoError(t, err)
		require.Equal(t, node.NodeID, shardNode.NodeID)
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestListNodesMaxBytes(t *testing.T) {
	ctx := nfs_testing.NewContext()
	client := nfs_testing.NewClient(t, ctx)

	filesystemID := t.Name()
	err := client.Create(ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)
	defer client.Delete(ctx, filesystemID, false)

	session, err := client.CreateSession(
		ctx,
		filesystemID,
		"",    // checkpointID
		false, // readOnly
	)
	require.NoError(t, err)
	defer session.Close(ctx)

	rootDir := nfs_testing.Root(
		nfs_testing.File("1"),
		nfs_testing.File("2"),
		nfs_testing.File("3"),
	)
	model := nfs_testing.NewFileSystemModel(t, ctx, session, rootDir)
	model.CreateAllNodesRecursively()

	expectedNames := []string{"1", "2", "3"}
	// maxBytes=0 means no limit, all nodes should be returned.
	nodes, cookie, err := session.ListNodes(
		ctx,
		nfs.RootNodeID,
		"",    // cookie
		0,     // maxBytes
		false, // unsafe
	)
	require.NoError(t, err)
	require.Equal(t, expectedNames, nfs_testing.NodeNames(nodes))
	require.Empty(t, cookie)

	// maxBytes=1 limits response size, requiring pagination.
	cookie = ""
	for _, expectedName := range expectedNames {
		nodes, cookie, err = session.ListNodes(
			ctx,
			nfs.RootNodeID,
			cookie,
			1,     // maxBytes
			false, // unsafe
		)
		require.NoError(t, err)
		require.Equal(t, []string{expectedName}, nfs_testing.NodeNames(nodes))
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestListNodesFromCheckpoint(t *testing.T) {
	ctx := nfs_testing.NewContext()
	client := nfs_testing.NewClient(t, ctx)

	filesystemID := t.Name()
	err := client.Create(ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)
	defer client.Delete(ctx, filesystemID, false)

	session, err := client.CreateSession(ctx, filesystemID, "", false)
	require.NoError(t, err)
	defer session.Close(ctx)

	rootDir := nfs_testing.Root(nfs_testing.Dir("first"), nfs_testing.File("second"))
	model := nfs_testing.NewFileSystemModel(t, ctx, session, rootDir)
	model.CreateAllNodesRecursively()

	checkpointID := "checkpoint_1"
	nodeID := uint64(0)
	err = session.CreateCheckpoint(
		ctx,
		filesystemID,
		checkpointID,
		nodeID,
	)
	require.NoError(t, err)
	defer client.DestroyCheckpoint(
		ctx,
		filesystemID,
		checkpointID,
	)

	model.CreateNodesRecursively(nfs.RootNodeID, nfs_testing.Dir("after_checkpoint"))
	nodes := model.ListAllNodesRecursively(false)
	require.Equal(t, []string{
		"after_checkpoint",
		"first",
		"second",
	}, nfs_testing.NodeNames(nodes))

	// Create a session for the checkpoint
	checkpointSession, err := client.CreateSession(
		ctx,
		filesystemID,
		checkpointID,
		false,
	)
	require.NoError(t, err)
	defer checkpointSession.Close(ctx)
	model.SetSession(checkpointSession)

	// Verify that only files created before the checkpoint are visible
	nodes = model.ListAllNodesRecursively(false)
	require.Equal(t, []string{
		"first",
		"second",
	}, nfs_testing.NodeNames(nodes))
}

////////////////////////////////////////////////////////////////////////////////

func TestGetNodeAttr(t *testing.T) {
	ctx := nfs_testing.NewContext()
	client := nfs_testing.NewClient(t, ctx)

	filesystemID := t.Name()
	err := client.Create(ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)
	defer client.Delete(ctx, filesystemID, false)

	session, err := client.CreateSession(ctx, filesystemID, "", false)
	require.NoError(t, err)
	defer session.Close(ctx)

	rootDir := nfs_testing.Root(
		nfs_testing.File("testfile"),
		nfs_testing.Dir("testdir"),
	)
	model := nfs_testing.NewFileSystemModel(t, ctx, session, rootDir)
	model.CreateAllNodesRecursively()

	fileNode, err := session.GetNodeAttr(ctx, nfs.RootNodeID, "testfile")
	require.NoError(t, err)
	require.Equal(t, "testfile", fileNode.Name)
	require.False(t, fileNode.Type.IsDirectory())
	require.NotZero(t, fileNode.NodeID)

	dirNode, err := session.GetNodeAttr(ctx, nfs.RootNodeID, "testdir")
	require.NoError(t, err)
	require.Equal(t, "testdir", dirNode.Name)
	require.True(t, dirNode.Type.IsDirectory())
	require.NotZero(t, dirNode.NodeID)
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateNodeIdempotent(t *testing.T) {
	ctx := nfs_testing.NewContext()
	client := nfs_testing.NewClient(t, ctx)

	filesystemID := t.Name()
	err := client.Create(ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)
	defer client.Delete(ctx, filesystemID, false)

	session, err := client.CreateSession(ctx, filesystemID, "", false)
	require.NoError(t, err)
	defer session.Close(ctx)

	node := nfs.Node{
		ParentID: nfs.RootNodeID,
		Name:     "testfile",
		Mode:     0644,
		UID:      1,
		GID:      1,
		Type:     nfs.NODE_KIND_FILE,
	}

	createdID, err := session.CreateNode(ctx, node)
	require.NoError(t, err)
	require.NotZero(t, createdID)

	idempotentID, err := session.CreateNodeIdempotent(ctx, node)
	require.NoError(t, err)
	require.Equal(t, createdID, idempotentID)
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateSessionIdempotent(t *testing.T) {
	ctx := nfs_testing.NewContext()
	client := nfs_testing.NewClient(t, ctx)

	filesystemID := t.Name()
	err := client.Create(ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)
	defer client.Delete(ctx, filesystemID, false)

	clientID := t.Name()
	session1, err := client.CreateSessionWithClientID(
		ctx,
		filesystemID,
		clientID,
		"",
		false,
	)
	require.NoError(t, err)
	defer session1.Close(ctx)

	session2, err := client.CreateSessionWithClientID(
		ctx,
		filesystemID,
		clientID,
		"",
		false,
	)
	require.NoError(t, err)
	defer session2.Close(ctx)

	require.Equal(t, session1.GetID(), session2.GetID())

	session3, err := client.CreateSessionWithClientID(
		ctx,
		filesystemID,
		clientID+"_different",
		"",
		false,
	)
	require.NoError(t, err)
	defer session3.Close(ctx)

	require.NotEqual(t, session1.GetID(), session3.GetID())
}

////////////////////////////////////////////////////////////////////////////////

var unsafeCreateNodeFilesystem = nfs_testing.Root(
	nfs_testing.Dir("a",
		nfs_testing.Dir("a",
			nfs_testing.File("1"),
			nfs_testing.File("2"),
			nfs_testing.File("3"),
			nfs_testing.BlockDev("dev_1", 1),
		),
		nfs_testing.Dir("b",
			nfs_testing.File("1"),
			nfs_testing.File("2"),
			nfs_testing.File("3"),
			nfs_testing.CharDev("dev_2", 1),
			nfs_testing.CharDev("dev_5", 1),
		),
		nfs_testing.Dir("c",
			nfs_testing.File("1"),
			nfs_testing.File("2"),
			nfs_testing.File("3"),
			nfs_testing.BlockDev("dev_3", 1),
		),
		nfs_testing.File("1"),
		nfs_testing.File("2"),
		nfs_testing.File("3"),
	),
	nfs_testing.Dir("b",
		nfs_testing.Dir("a",
			nfs_testing.File("1"),
			nfs_testing.File("2"),
			nfs_testing.File("3"),
		),
		nfs_testing.Dir("b",
			nfs_testing.File("1"),
			nfs_testing.File("2"),
			nfs_testing.File("3"),
		),
		nfs_testing.Dir("c",
			nfs_testing.File("1"),
			nfs_testing.File("2"),
			nfs_testing.File("3"),
		),
		nfs_testing.File("1"),
		nfs_testing.File("2"),
		nfs_testing.File("3"),
	),
	nfs_testing.Dir("c",
		nfs_testing.Dir("a",
			nfs_testing.File("1"),
			nfs_testing.File("2"),
			nfs_testing.File("3"),
		),
		nfs_testing.Dir("b",
			nfs_testing.File("1"),
			nfs_testing.File("2"),
			nfs_testing.File("3"),
		),
		nfs_testing.Dir("c",
			nfs_testing.File("1"),
			nfs_testing.File("2"),
			nfs_testing.File("3"),
		),
		nfs_testing.File("1"),
		nfs_testing.File("2"),
		nfs_testing.File("3"),
	),
	nfs_testing.File("1"),
	nfs_testing.File("2"),
	nfs_testing.File("3"),
	nfs_testing.BlockDev("dev", 1),
)

////////////////////////////////////////////////////////////////////////////////

type inMemoryTestShardBackup struct {
	t                 *testing.T
	client            nfs.Client
	shardFileSystemID string
	nodesByParent     map[uint64][]nfs.Node
	nodesInShard      []nfs.Node
}

func newInMemoryTestShardBackup(
	t *testing.T,
	client nfs.Client,
	shardFileSystemID string,
	nodes []nfs.Node,
) inMemoryTestShardBackup {

	backup := inMemoryTestShardBackup{
		t:                 t,
		client:            client,
		shardFileSystemID: shardFileSystemID,
		nodesByParent:     make(map[uint64][]nfs.Node),
	}

	for _, node := range nodes {
		backup.nodesByParent[node.ParentID] = append(
			backup.nodesByParent[node.ParentID],
			node,
		)

		if node.ShardFileSystemID == shardFileSystemID {
			backup.nodesInShard = append(backup.nodesInShard, node)
		}
	}

	require.NotEmpty(t, backup.nodesInShard)
	return backup
}

func (i inMemoryTestShardBackup) restore(ctx context.Context) {
	for _, node := range i.nodesInShard {
		i.restoreNode(ctx, node)
	}

	for _, node := range i.nodesInShard {
		if !node.Type.IsDirectory() {
			continue
		}

		for _, child := range i.nodesByParent[node.NodeID] {
			i.createChildRef(ctx, node.NodeID, child)
		}
	}
}

func (i inMemoryTestShardBackup) restoreNode(ctx context.Context, node nfs.Node) {
	err := i.client.UnsafeCreateNode(ctx, i.shardFileSystemID, node)
	require.NoError(i.t, err)

	err = i.client.UnsafeCreateNodeRef(
		ctx,
		i.shardFileSystemID,
		nfs.RootNodeID,
		node.ShardNodeName,
		node.NodeID,
		"",
		"",
	)
	require.NoError(i.t, err)
}

func (i inMemoryTestShardBackup) createChildRef(
	ctx context.Context,
	parentID uint64,
	child nfs.Node,
) {

	shardID := child.ShardFileSystemID
	shardNodeName := child.ShardNodeName

	// The child node is created in the shard, so the childID is 0
	err := i.client.UnsafeCreateNodeRef(
		ctx,
		i.shardFileSystemID,
		parentID,
		child.Name,
		uint64(0), // childID
		shardID,
		shardNodeName,
	)
	require.NoError(i.t, err)
}

////////////////////////////////////////////////////////////////////////////////

func TestUnsafeCreateNodeRestoreShardAfterDeletion(t *testing.T) {
	ctx := nfs_testing.NewContext()
	logger := logging.GetLogger(ctx)
	client := nfs_testing.NewClient(t, ctx)

	filesystemID := t.Name()
	shardCount := uint32(2)

	err := client.Create(ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
		ShardCount:  shardCount,
	})
	require.NoError(t, err)
	defer client.Delete(ctx, filesystemID, true /* force */)

	err = client.EnableDirectoryCreationInShards(ctx, filesystemID, shardCount)
	require.NoError(t, err)

	session, err := client.CreateSession(ctx, filesystemID, "", false)
	require.NoError(t, err)

	model := nfs_testing.NewFileSystemModel(
		t,
		ctx,
		session,
		unsafeCreateNodeFilesystem,
	)
	defer model.Close()
	model.CreateAllNodesRecursively()

	initialNodes := model.ListAllNodesRecursively(true)
	logger.Infof("initial listing has %d nodes", len(initialNodes))

	// Backup shard 2 nodes and directory children before deleting the shard.
	shard2FsID := fmt.Sprintf("%s_s2", filesystemID)
	shard2Backup := newInMemoryTestShardBackup(
		t,
		client,
		shard2FsID,
		initialNodes,
	)

	shardIDs := make([]string, 0, shardCount)
	for i := uint32(1); i <= shardCount; i++ {
		shardIDs = append(shardIDs, fmt.Sprintf("%s_s%d", filesystemID, i))
	}

	// Filesystem deletion invokes deletion of all of its shards.
	// To avoid that, configure empty shards list.
	err = client.ConfigureShards(ctx, shard2FsID, nfs.ConfigureShardsParams{
		ShardFileSystemIDs: []string{},
		Force:              true,
	})
	require.NoError(t, err)

	err = client.Delete(ctx, shard2FsID, true /* force */)
	require.NoError(t, err)

	err = client.Create(ctx, shard2FsID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)

	err = client.ConfigureAsShard(ctx, shard2FsID, nfs.ConfigureAsShardParams{
		ShardNo:                          2,
		ShardFileSystemIDs:               shardIDs,
		MainFileSystemID:                 filesystemID,
		DirectoryCreationInShardsEnabled: true,
	})
	require.NoError(t, err)

	err = session.Close(ctx)
	require.NoError(t, err)

	session, err = client.CreateSession(ctx, filesystemID, "", false)
	require.NoError(t, err)
	model.SetSession(session)
	// Verify that the listing got shorter but is not empty.
	afterDeleteNodes := model.ListAllNodesRecursively(true)
	require.Less(t, len(afterDeleteNodes), len(initialNodes))
	require.NotEmpty(t, afterDeleteNodes)

	shard2Backup.restore(ctx)

	// Verify the restored listing matches the original.
	restoredNodes := model.ListAllNodesRecursively(true)
	model.RequireNodesEqual(restoredNodes, true)
}
