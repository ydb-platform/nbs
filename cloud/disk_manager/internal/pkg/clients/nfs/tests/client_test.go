package tests

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/testing"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

var listNodesRootDir = nfs_testing.Root(
	nfs_testing.Dir("etc",
		nfs_testing.File("passwd"),
		nfs_testing.File("hosts"),
	),
	nfs_testing.Dir(
		"var",
		nfs_testing.Dir(
			"log",
		),
	),
	nfs_testing.Symlink("bin", "/usr/bin"),
	nfs_testing.Dir("usr",
		nfs_testing.Dir("bin", nfs_testing.File("bash"), nfs_testing.File("ls")),
		nfs_testing.Dir("lib", nfs_testing.File("libc.so")),
	),
)

var listNodesExpectedNames = []string{
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

	model := nfs_testing.NewFileSystemModel(t, ctx, session, listNodesRootDir)
	model.CreateAllNodesRecursively()

	nodes := model.ListAllNodesRecursively(false)
	model.RequireNodesEqual(nodes)
	require.Equal(t, listNodesExpectedNames, nfs_testing.NodeNames(nodes))
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

	model := nfs_testing.NewFileSystemModel(t, ctx, session, listNodesRootDir)
	model.CreateAllNodesRecursively()

	nodes := model.ListAllNodesRecursively(true)
	model.RequireNodesEqual(nodes)
	require.Equal(t, listNodesExpectedNames, nfs_testing.NodeNames(nodes))

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
	session1, err := client.CreateSessionWithClientID(ctx, filesystemID, clientID, "", false)
	require.NoError(t, err)
	defer session1.Close(ctx)

	session2, err := client.CreateSessionWithClientID(ctx, filesystemID, clientID, "", false)
	require.NoError(t, err)
	defer session2.Close(ctx)
}
