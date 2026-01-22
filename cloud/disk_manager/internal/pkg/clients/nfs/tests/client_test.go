package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/testing"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
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

	err = client.Delete(ctx, filesystemID)
	require.NoError(t, err)

	// Deleting the same filesystem twice is not an error
	err = client.Delete(ctx, filesystemID)
	require.NoError(t, err)

	// Deleting non-existent filesystem is also not an error
	err = client.Delete(ctx, filesystemID+"_does_not_exist")
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
	defer client.Delete(ctx, filesystemID)

	checkpointID := "checkpoint_1"
	nodeID := uint64(0)
	session, err := client.CreateSession(ctx, filesystemID, "", false)
	require.NoError(t, err)
	defer client.DestroySession(ctx, session)

	err = client.CreateCheckpoint(
		ctx,
		session,
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
	defer client.Delete(ctx, filesystemID)

	session, err := client.CreateSession(ctx, filesystemID, "", false)
	require.NoError(t, err)
	defer client.DestroySession(ctx, session)

	rootDir := nfs_testing.Root(
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
	model := nfs_testing.NewFileSystemModel(t, ctx, client, session, rootDir)
	model.Create()

	nodes := model.ListAllNodesRecursively()
	require.Equal(t, len(model.ExpectedNodes), len(nodes))
	for index, node := range nodes {
		expectedNode := model.ExpectedNodes[index]
		require.Equal(t, expectedNode.ParentID, node.ParentID)
		require.Equal(t, expectedNode.Name, node.Name)
		require.Equal(t, expectedNode.Type, node.Type)
		// TODO: enable when ydb based nfs server is supported in the recipe.
		// Local filestore service also does not support proper uid/gid.
		// See: https://github.com/ydb-platform/nbs/issues/4302
		// require.Equal(t, expectedNode.Mode, node.Mode)
		// require.Equal(t, expectedNode.UID, node.UID)
		// require.Equal(t, expectedNode.GID, node.GID)
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
	require.Equal(t, expectedNames, nfs_testing.NodeNames(nodes))
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
	defer client.Delete(ctx, filesystemID)

	session, err := client.CreateSession(ctx, filesystemID, "", false)
	require.NoError(t, err)
	defer client.DestroySession(ctx, session)

	rootDir := nfs_testing.Root(nfs_testing.Dir("first"), nfs_testing.File("second"))
	model := nfs_testing.NewFileSystemModel(t, ctx, client, session, rootDir)
	model.Create()

	checkpointID := "checkpoint_1"
	nodeID := uint64(0)
	err = client.CreateCheckpoint(
		ctx,
		session,
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

	model.CreateNodes(nfs_testing.RootNodeID, nfs_testing.Dir("after_checkpoint"))
	nodes := model.ListAllNodesRecursively()
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
	defer client.DestroySession(ctx, checkpointSession)
	model.SetSession(checkpointSession)

	// Verify that only files created before the checkpoint are visible
	nodes = model.ListAllNodesRecursively()
	require.Equal(t, []string{
		"first",
		"second",
	}, nfs_testing.NodeNames(nodes))
}
