package filesystem_snapshot_service_test

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/testing"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/test/filestore_client"
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

type comparableNode struct {
	Name  string
	Mode  uint32
	Type  uint32
	Links uint32
	UID   uint32
	GID   uint32
}

func listComparableNodes(
	t *testing.T,
	ctx context.Context,
	nfsClient nfs_testing.TestingClient,
	filesystemID string,
) []comparableNode {

	session, err := nfsClient.CreateSession(ctx, filesystemID, "", true)
	require.NoError(t, err)
	model := nfs_testing.NewFileSystemModel(
		t,
		ctx,
		session,
		nfs_testing.Root(),
	)
	defer model.Close()

	nodes := model.ListAllNodesRecursively(false)
	result := make([]comparableNode, 0, len(nodes))
	for _, node := range nodes {
		require.NotEmpty(t, node.Name)
		result = append(result, comparableNode{
			Name:  node.Name,
			Mode:  node.Mode,
			Type:  uint32(node.Type),
			Links: node.Links,
			UID:   node.UID,
			GID:   node.GID,
		})
	}

	slices.SortFunc(result, func(a, b comparableNode) int {
		if c := strings.Compare(a.Name, b.Name); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Type, b.Type); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Mode, b.Mode); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Links, b.Links); c != 0 {
			return c
		}
		if c := cmp.Compare(a.UID, b.UID); c != 0 {
			return c
		}
		return cmp.Compare(a.GID, b.GID)
	})
	return result
}

func checkCopyFilesystemThroughSnapshot(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	nfsClient nfs_testing.TestingClient,
	filestoreClient *filestore_client.FilestoreClient,
	srcFilesystemID string,
	dstFilesystemID string,
	snapshotID string,
) {

	copyFilesystemThroughSnapshot(
		t,
		ctx,
		client,
		srcFilesystemID,
		dstFilesystemID,
		snapshotID,
	)

	srcNodes := listComparableNodes(t, ctx, nfsClient, srcFilesystemID)
	dstNodes := listComparableNodes(t, ctx, nfsClient, dstFilesystemID)
	require.NotEmpty(t, srcNodes)
	require.NotEmpty(t, dstNodes)
	require.Equal(t, srcNodes, dstNodes)

	srcPaths := filestoreClient.FindAllPaths(ctx, srcFilesystemID)
	dstPaths := filestoreClient.FindAllPaths(ctx, dstFilesystemID)
	require.NotEmpty(t, srcPaths)
	require.NotEmpty(t, dstPaths)
	require.Equal(t, srcPaths, dstPaths)
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateFilesystemSnapshot(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer func() {
		_ = client.Close()
	}()

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateFilesystemSnapshot(
		reqCtx,
		&disk_manager.CreateFilesystemSnapshotRequest{
			Src: &disk_manager.FilesystemId{
				FilesystemId: "test-filesystem-id",
				ZoneId:       "test-zone-id",
			},
			FilesystemSnapshotId: "test-filesystem-snapshot-id",
		},
	)
	require.NoError(t, err)
	require.NotNil(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.DeleteFilesystemSnapshot(
		reqCtx,
		&disk_manager.DeleteFilesystemSnapshotRequest{
			FilesystemSnapshotId: "test-filesystem-snapshot-id",
		},
	)
	require.NoError(t, err)
	require.NotNil(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)
}

func TestFilesystemSnapshotLargeDirectoryTree(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	nfsClient := testcommon.NewNfsTestingClient(t, ctx, "zone-a")
	defer nfsClient.Close()

	filestoreClient := filestore_client.NewFilestoreClient(t)

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

	checkCopyFilesystemThroughSnapshot(
		t,
		ctx,
		client,
		nfsClient,
		filestoreClient,
		srcFilesystemID,
		dstFilesystemID,
		t.Name()+"_snapshot",
	)
	defer func() {
		err := nfsClient.Delete(ctx, dstFilesystemID, true)
		require.NoError(t, err)
	}()
}

////////////////////////////////////////////////////////////////////////////////

func TestFilesystemSnapshotEmptyFilesystem(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	nfsClient := testcommon.NewNfsTestingClient(t, ctx, "zone-a")
	defer nfsClient.Close()

	filestoreClient := filestore_client.NewFilestoreClient(t)

	srcFilesystemID := t.Name() + "_src"
	dstFilesystemID := t.Name() + "_dst"

	createFilesystem(t, ctx, client, srcFilesystemID)
	defer func() {
		err := nfsClient.Delete(ctx, srcFilesystemID, true)
		require.NoError(t, err)
	}()

	copyFilesystemThroughSnapshot(
		t,
		ctx,
		client,
		srcFilesystemID,
		dstFilesystemID,
		t.Name()+"_snapshot",
	)
	defer func() {
		err := nfsClient.Delete(ctx, dstFilesystemID, true)
		require.NoError(t, err)
	}()

	require.Empty(t, listComparableNodes(t, ctx, nfsClient, srcFilesystemID))
	require.Empty(t, listComparableNodes(t, ctx, nfsClient, dstFilesystemID))
	require.Empty(t, filestoreClient.FindAllPaths(ctx, srcFilesystemID))
	require.Empty(t, filestoreClient.FindAllPaths(ctx, dstFilesystemID))
}

////////////////////////////////////////////////////////////////////////////////

func TestFilesystemSnapshotSmallTree(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	nfsClient := testcommon.NewNfsTestingClient(t, ctx, "zone-a")
	defer nfsClient.Close()

	filestoreClient := filestore_client.NewFilestoreClient(t)

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
	nodes := model.ListAllNodesRecursively(false)
	require.Equal(t, expectedNames, nfs_testing.NodeNames(nodes))
	model.Close()

	checkCopyFilesystemThroughSnapshot(
		t,
		ctx,
		client,
		nfsClient,
		filestoreClient,
		srcFilesystemID,
		dstFilesystemID,
		t.Name()+"_snapshot",
	)
	defer func() {
		err := nfsClient.Delete(ctx, dstFilesystemID, true)
		require.NoError(t, err)
	}()
}

////////////////////////////////////////////////////////////////////////////////

func TestFilesystemSnapshotHardlinks(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	nfsClient := testcommon.NewNfsTestingClient(t, ctx, "zone-a")
	defer nfsClient.Close()

	filestoreClient := filestore_client.NewFilestoreClient(t)

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
				ParentNodeID: dir.NodeID,
				NodeID:       file.NodeID,
				Name:         linkName,
				Type:         nfs.NODE_KIND_LINK,
			})
			require.NoError(t, err)
		}
	}
	_ = session.Close(ctx)

	checkCopyFilesystemThroughSnapshot(
		t,
		ctx,
		client,
		nfsClient,
		filestoreClient,
		srcFilesystemID,
		dstFilesystemID,
		t.Name()+"_snapshot",
	)
	defer func() {
		err := nfsClient.Delete(ctx, dstFilesystemID, true)
		require.NoError(t, err)
	}()
}
