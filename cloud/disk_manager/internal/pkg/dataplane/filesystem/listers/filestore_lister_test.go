package listers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/mocks"
	nfs_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/testing"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
)

////////////////////////////////////////////////////////////////////////////////

type fixture struct {
	ctx    context.Context
	client nfs.Client
}

func newFixture(t *testing.T) *fixture {
	ctx := nfs_testing.NewContext()
	client := nfs_testing.NewClient(t, ctx)

	return &fixture{
		ctx:    ctx,
		client: client,
	}
}

func (f *fixture) close(t *testing.T) {
	require.NoError(t, f.client.Close())
}

func (f *fixture) prepareFilesystem(t *testing.T, filesystemID string) {
	err := f.client.Create(f.ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)
}

func (f *fixture) cleanupFilesystem(t *testing.T, filesystemID string) {
	err := f.client.Delete(f.ctx, filesystemID, false)
	require.NoError(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func TestMaxBytesIsUsed(t *testing.T) {
	fixture := newFixture(t)
	defer fixture.close(t)

	filesystemID := t.Name()
	fixture.prepareFilesystem(t, filesystemID)
	defer fixture.cleanupFilesystem(t, filesystemID)

	session, err := fixture.client.CreateSession(
		fixture.ctx,
		filesystemID,
		"",
		false,
	)
	require.NoError(t, err)

	fsModel := nfs_testing.NewFileSystemModel(
		t,
		fixture.ctx,
		session,
		nfs_testing.Root(
			nfs_testing.File("file1"),
			nfs_testing.File("file2"),
		),
	)
	fsModel.CreateAllNodesRecursively()
	fsModel.Close()

	factory := NewFilestoreListerFactory(
		fixture.client,
		1,     // listNodesMaxBytes
		true,  // readOnly
		false, // unsafe
	)

	lister, err := factory.CreateLister(fixture.ctx, filesystemID, "")
	require.NoError(t, err)
	defer func() {
		err := lister.Close(fixture.ctx)
		require.NoError(t, err)
	}()

	nodes, cookie, err := lister.ListNodes(
		fixture.ctx,
		nfs.RootNodeID,
		"",
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(nodes))
	require.NotEmpty(t, cookie)

	nodes2, _, err := lister.ListNodes(
		fixture.ctx,
		nfs.RootNodeID,
		cookie,
	)
	require.NoError(t, err)

	allNodes := append(nodes, nodes2...)
	names := nfs_testing.NodeNames(allNodes)
	require.ElementsMatch(t, []string{"file1", "file2"}, names)
}

func TestReadOnlyIsUsed(t *testing.T) {
	fixture := newFixture(t)
	defer fixture.close(t)

	filesystemID := t.Name()
	fixture.prepareFilesystem(t, filesystemID)
	defer fixture.cleanupFilesystem(t, filesystemID)

	factory := NewFilestoreListerFactory(
		fixture.client,
		0,     // listNodesMaxBytes
		true,  // readOnly
		false, // unsafe
	)

	lister, err := factory.CreateLister(fixture.ctx, filesystemID, "")
	require.NoError(t, err)
	defer func() {
		err := lister.Close(fixture.ctx)
		require.NoError(t, err)
	}()

	concreteLister, ok := lister.(*filestoreLister)
	require.True(t, ok)

	_, err = concreteLister.session.CreateNode(
		fixture.ctx,
		nfs.Node{
			ParentID: nfs.RootNodeID,
			Name:     "should_fail",
			Type:     nfs_client.NODE_KIND_FILE,
			Mode:     0o644,
			UID:      1,
			GID:      1,
		},
	)
	require.Error(t, err)

	writeFactory := NewFilestoreListerFactory(
		fixture.client,
		0,     // listNodesMaxBytes
		false, // readOnly
		false, // unsafe
	)

	writeLister, err := writeFactory.CreateLister(fixture.ctx, filesystemID, "")
	require.NoError(t, err)
	defer func() {
		err := writeLister.Close(fixture.ctx)
		require.NoError(t, err)
	}()

	concreteWriteLister, ok := writeLister.(*filestoreLister)
	require.True(t, ok)

	_, err = concreteWriteLister.session.CreateNode(
		fixture.ctx,
		nfs.Node{
			ParentID: nfs.RootNodeID,
			Name:     "should_succeed",
			Type:     nfs_client.NODE_KIND_FILE,
			Mode:     0o644,
			UID:      1,
			GID:      1,
		},
	)
	require.NoError(t, err)
}

func TestUnsafeIsPropagated(t *testing.T) {
	ctx := nfs_testing.NewContext()

	clientMock := nfs_mocks.NewClientMock()
	sessionMock := nfs_mocks.NewSessionMock()

	filesystemID := "test-filesystem"
	checkpointID := "test-checkpoint"

	factory := NewFilestoreListerFactory(
		clientMock,
		42,    // listNodesMaxBytes
		true,  // readOnly
		true,  // unsafe
	)

	clientMock.On(
		"CreateSession",
		mock.Anything,
		filesystemID,
		checkpointID,
		true, // readOnly
	).Return(sessionMock, nil)

	sessionMock.On(
		"ListNodes",
		mock.Anything,
		nfs.RootNodeID,
		"",         // cookie
		uint32(42), // maxBytes
		true,       // unsafe
	).Return([]nfs.Node{}, "", nil)

	sessionMock.On("Close", mock.Anything).Return(nil)

	lister, err := factory.CreateLister(ctx, filesystemID, checkpointID)
	require.NoError(t, err)

	nodes, nextCookie, err := lister.ListNodes(ctx, nfs.RootNodeID, "")
	require.NoError(t, err)
	require.Empty(t, nodes)
	require.Empty(t, nextCookie)

	err = lister.Close(ctx)
	require.NoError(t, err)

	clientMock.AssertExpectations(t)
	sessionMock.AssertExpectations(t)
}
