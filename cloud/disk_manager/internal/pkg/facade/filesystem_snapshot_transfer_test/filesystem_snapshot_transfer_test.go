package tests

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/testing"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
)

////////////////////////////////////////////////////////////////////////////////

const filesystemSize = 1024 * 1024 * 1024

func createFilesystem(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	filesystemID string,
) {

	operation, err := client.CreateFilesystem(
		testcommon.GetRequestContext(t, ctx),
		&disk_manager.CreateFilesystemRequest{
			FilesystemId: &disk_manager.FilesystemId{
				ZoneId:       "zone-a",
				FilesystemId: filesystemID,
			},
			BlockSize: 4096,
			Size:      filesystemSize,
			Kind:      disk_manager.FilesystemKind_FILESYSTEM_KIND_SSD,
			CloudId:   "cloud",
			FolderId:  "folder",
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)
}

func copyFilesystemThroughSnapshot(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	srcFilesystemID string,
	dstFilesystemID string,
	snapshotID string,
) {

	taskID := testcommon.ScheduleTransferFromFilesystemToSnapshot(
		t, ctx, "zone-a", srcFilesystemID, "", snapshotID,
	)
	testcommon.WaitOperationEnded(t, ctx, taskID, time.Second*200)
	testcommon.RequireTaskHasNoError(t, ctx, taskID)

	createFilesystem(t, ctx, client, dstFilesystemID)

	taskID = testcommon.ScheduleTransferFromSnapshotToFilesystem(
		t, ctx, "zone-a", dstFilesystemID, snapshotID,
	)
	testcommon.WaitOperationEnded(t, ctx, taskID, time.Second*200)
	testcommon.RequireTaskHasNoError(t, ctx, taskID)
}

func compareFindResults(
	t *testing.T,
	filestoreClient *testcommon.FilestoreClient,
	srcFilesystemID string,
	dstFilesystemID string,
) {

	srcPaths := filestoreClient.FindAllPaths(srcFilesystemID)
	dstPaths := filestoreClient.FindAllPaths(dstFilesystemID)
	require.Equal(t, srcPaths, dstPaths)
}

////////////////////////////////////////////////////////////////////////////////

func TestFilesystemTraversalLargeDirectoryTree(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	nfsClient := testcommon.NewNfsTestingClient(t, ctx, "zone-a")
	defer nfsClient.Close()

	filestoreClient := testcommon.NewFilestoreClient(t)

	srcFilesystemID := t.Name() + "_src"
	dstFilesystemID := t.Name() + "_dst"

	createFilesystem(t, ctx, client, srcFilesystemID)
	defer func() {
		err := nfsClient.Delete(ctx, srcFilesystemID, true)
		require.NoError(t, err)
	}()

	nfsClient.FillFilesystemWithDefaultTree(
		ctx,
		srcFilesystemID,
		5,
		1000,
		1,
	)

	copyFilesystemThroughSnapshot(
		t, ctx, client,
		srcFilesystemID, dstFilesystemID,
		t.Name()+"_snapshot",
	)
	defer func() {
		err := nfsClient.Delete(ctx, dstFilesystemID, true)
		require.NoError(t, err)
	}()

	compareFindResults(t, filestoreClient, srcFilesystemID, dstFilesystemID)
}

////////////////////////////////////////////////////////////////////////////////

func TestFilesystemTraversalSmallTree(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	nfsClient := testcommon.NewNfsTestingClient(t, ctx, "zone-a")
	defer nfsClient.Close()

	filestoreClient := testcommon.NewFilestoreClient(t)

	srcFilesystemID := t.Name() + "_src"
	dstFilesystemID := t.Name() + "_dst"

	createFilesystem(t, ctx, client, srcFilesystemID)
	defer func() {
		err := nfsClient.Delete(ctx, srcFilesystemID, true)
		require.NoError(t, err)
	}()

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
			nfs_testing.Dir("bin",
				nfs_testing.File("bash"),
				nfs_testing.File("ls"),
			),
			nfs_testing.Dir("lib",
				nfs_testing.File("libc.so"),
			),
		),
	)

	session, err := nfsClient.CreateSession(ctx, srcFilesystemID, "", false)
	require.NoError(t, err)
	model := nfs_testing.NewFileSystemModel(t, ctx, session, rootDir)
	model.CreateAllNodesRecursively()

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
	nodes := model.ListAllNodesRecursively()
	require.Equal(t, expectedNames, nfs_testing.NodeNames(nodes))
	model.Close()

	srcPaths := filestoreClient.FindAllPaths(srcFilesystemID)
	require.NotEmpty(t, srcPaths)

	copyFilesystemThroughSnapshot(
		t, ctx, client,
		srcFilesystemID, dstFilesystemID,
		t.Name()+"_snapshot",
	)
	defer func() {
		err := nfsClient.Delete(ctx, dstFilesystemID, true)
		require.NoError(t, err)
	}()

	dstSession, err := nfsClient.CreateSession(ctx, dstFilesystemID, "", true)
	require.NoError(t, err)
	dstModel := nfs_testing.NewFileSystemModel(t, ctx, dstSession, rootDir)
	dstNodes := dstModel.ListAllNodesRecursively()
	require.Equal(t, expectedNames, nfs_testing.NodeNames(dstNodes))
	dstModel.Close()

	verifySession, err := nfsClient.CreateSession(
		ctx, dstFilesystemID, "", true,
	)
	require.NoError(t, err)
	verifyModel := nfs_testing.NewFileSystemModel(t, ctx, verifySession, rootDir)
	verifyNodes := verifyModel.ListAllNodesRecursively()
	require.Equal(t, expectedNames, nfs_testing.NodeNames(verifyNodes))
	verifyModel.Close()

	compareFindResults(t, filestoreClient, srcFilesystemID, dstFilesystemID)
}

////////////////////////////////////////////////////////////////////////////////

func TestFilesystemTraversalHardlinks(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	nfsClient := testcommon.NewNfsTestingClient(t, ctx, "zone-a")
	defer nfsClient.Close()

	filestoreClient := testcommon.NewFilestoreClient(t)

	srcFilesystemID := t.Name() + "_src"
	dstFilesystemID := t.Name() + "_dst"

	createFilesystem(t, ctx, client, srcFilesystemID)
	defer func() {
		err := nfsClient.Delete(ctx, srcFilesystemID, true)
		require.NoError(t, err)
	}()

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
			nfs_testing.Dir("bin",
				nfs_testing.File("bash"),
				nfs_testing.File("ls"),
			),
			nfs_testing.Dir("lib",
				nfs_testing.File("libc.so"),
			),
		),
	)

	session, err := nfsClient.CreateSession(ctx, srcFilesystemID, "", false)
	require.NoError(t, err)
	model := nfs_testing.NewFileSystemModel(t, ctx, session, rootDir)
	model.CreateAllNodesRecursively()

	var fileNodes []nfs.Node
	var dirNodes []nfs.Node
	for _, node := range model.ExpectedNodes {
		if node.Type.IsDirectory() {
			dirNodes = append(dirNodes, node)
		} else if !node.Type.IsSymlink() {
			fileNodes = append(fileNodes, node)
		}
	}

	for _, dir := range dirNodes {
		for _, file := range fileNodes {
			linkName := fmt.Sprintf(
				"hardlink_%s_in_%s",
				file.Name,
				dir.Name,
			)
			_, err := session.CreateNode(ctx, nfs.Node{
				ParentID: dir.NodeID,
				NodeID:   file.NodeID,
				Name:     linkName,
				Type:     nfs.NODE_KIND_LINK,
			})
			require.NoError(t, err)
		}
	}
	session.Close(ctx)

	copyFilesystemThroughSnapshot(
		t, ctx, client,
		srcFilesystemID, dstFilesystemID,
		t.Name()+"_snapshot",
	)
	defer func() {
		err := nfsClient.Delete(ctx, dstFilesystemID, true)
		require.NoError(t, err)
	}()

	srcPaths := filestoreClient.FindAllPaths(srcFilesystemID)
	dstPaths := filestoreClient.FindAllPaths(dstFilesystemID)

	slices.Sort(srcPaths)
	slices.Sort(dstPaths)
	require.Equal(t, srcPaths, dstPaths)
}
