package snapshot

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/mocks"
	nodes_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/nodes"
	nodes_storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/nodes/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type hardlinkRestorerTestFixture struct {
	ctx      context.Context
	cancel   context.CancelFunc
	session  *nfs_mocks.SessionMock
	storage  *nodes_storage_mocks.StorageMock
	restorer *hardlinkBatchRestorer
	cookie   nodes_storage.HardLinksCookie
}

func newHardlinkRestorerTestFixture(t *testing.T) *hardlinkRestorerTestFixture {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	f := &hardlinkRestorerTestFixture{
		ctx:     logging.SetLogger(ctx, logging.NewStderrLogger(logging.ErrorLevel)),
		cancel:  cancel,
		session: nfs_mocks.NewSessionMock(),
		storage: nodes_storage_mocks.NewStorageMock(),
		cookie: nodes_storage.HardLinksCookie{
			NodeID: 10, ParentNodeID: 2, Name: "file",
		},
	}

	f.restorer = newHardlinkBatchRestorer(
		f.session,
		f.storage,
		"snapshot",
		"filesystem",
		2,
		1,
	)
	return f
}

func (f *hardlinkRestorerTestFixture) close(t *testing.T) {
	f.cancel()
	f.session.AssertExpectations(t)
	f.storage.AssertExpectations(t)
}

////////////////////////////////////////////////////////////////////////////////

func TestHardlinkBatchRestorerEmptyBatch(t *testing.T) {
	f := newHardlinkRestorerTestFixture(t)
	defer f.close(t)

	f.storage.On("ListHardLinks", mock.Anything, "snapshot", 2, f.cookie).
		Return([]nfs.Node(nil), nodes_storage.HardLinksCookie{}, nil).Once()

	cookie, err := f.restorer.Restore(f.ctx, f.cookie)

	require.NoError(t, err)
	require.Equal(t, nodes_storage.HardLinksCookie{}, cookie)
	f.session.AssertNumberOfCalls(t, "CreateNodeIdempotent", 0)
	f.storage.AssertNumberOfCalls(t, "GetDestinationNodeIDs", 0)
	f.storage.AssertNumberOfCalls(t, "UpdateRestorationNodeIDMapping", 0)
}

func TestHardlinkBatchRestorerCreatesInodeAndLink(t *testing.T) {
	f := newHardlinkRestorerTestFixture(t)
	defer f.close(t)

	batch := []nfs.Node{
		{NodeID: 10, ParentNodeID: 2, Name: "file", Type: nfs.NODE_KIND_FILE},
		{NodeID: 10, ParentNodeID: 2, Name: "link", Type: nfs.NODE_KIND_FILE},
	}

	nextCookie := nodes_storage.HardLinksCookie{
		NodeID: 11, ParentNodeID: 2, Name: "next",
	}

	f.storage.On("ListHardLinks", mock.Anything, "snapshot", 2, f.cookie).
		Return(batch, nextCookie, nil).Once()
	f.storage.On(
		"GetDestinationNodeIDs",
		mock.Anything,
		"snapshot",
		"filesystem",
		[]uint64{2},
	).Return(map[uint64]uint64{2: 20}, nil).Once()
	f.storage.On(
		"GetDestinationNodeIDs",
		mock.Anything,
		"snapshot",
		"filesystem",
		[]uint64{10},
	).Return(map[uint64]uint64(nil), nil).Once()
	inode := nfs.Node{
		NodeID: 10, ParentNodeID: 20, Name: "file", Type: nfs.NODE_KIND_FILE,
	}

	link := nfs.Node{
		NodeID: 100, ParentNodeID: 20, Name: "link", Type: nfs.NODE_KIND_LINK,
	}

	inodeCall := f.session.On("CreateNodeIdempotent", mock.Anything, inode).
		Return(uint64(100), nil).Once()
	linkCall := f.session.On("CreateNodeIdempotent", mock.Anything, link).
		Return(uint64(100), nil).Once().NotBefore(inodeCall)
	f.storage.On(
		"UpdateRestorationNodeIDMapping",
		mock.Anything,
		"snapshot",
		"filesystem",
		map[uint64]uint64{10: 100},
	).Return(nil).Once().NotBefore(linkCall)

	cookie, err := f.restorer.Restore(f.ctx, f.cookie)

	require.NoError(t, err)
	require.Equal(t, nextCookie, cookie)
	require.Equal(
		t,
		[]nfs.Node{
			{NodeID: 10, ParentNodeID: 2, Name: "file", Type: nfs.NODE_KIND_FILE},
			{NodeID: 10, ParentNodeID: 2, Name: "link", Type: nfs.NODE_KIND_FILE},
		},
		batch,
	)
}

func TestHardlinkBatchRestorerUsesExistingInode(t *testing.T) {
	f := newHardlinkRestorerTestFixture(t)
	defer f.close(t)

	batch := []nfs.Node{
		{NodeID: 10, ParentNodeID: 2, Name: "file", Type: nfs.NODE_KIND_FILE},
		{NodeID: 10, ParentNodeID: 2, Name: "link", Type: nfs.NODE_KIND_FILE},
	}

	f.storage.On("ListHardLinks", mock.Anything, "snapshot", 2, f.cookie).
		Return(batch, nodes_storage.HardLinksCookie{}, nil).Once()
	f.storage.On(
		"GetDestinationNodeIDs",
		mock.Anything,
		"snapshot",
		"filesystem",
		[]uint64{2},
	).Return(map[uint64]uint64{2: 20}, nil).Once()
	f.storage.On(
		"GetDestinationNodeIDs",
		mock.Anything,
		"snapshot",
		"filesystem",
		[]uint64{10},
	).Return(map[uint64]uint64{10: 100}, nil).Once()
	firstLink := nfs.Node{
		NodeID: 100, ParentNodeID: 20, Name: "file", Type: nfs.NODE_KIND_LINK,
	}

	secondLink := nfs.Node{
		NodeID: 100, ParentNodeID: 20, Name: "link", Type: nfs.NODE_KIND_LINK,
	}

	f.session.On("CreateNodeIdempotent", mock.Anything, firstLink).
		Return(uint64(100), nil).Once()
	f.session.On("CreateNodeIdempotent", mock.Anything, secondLink).
		Return(uint64(100), nil).Once()

	cookie, err := f.restorer.Restore(f.ctx, f.cookie)

	require.NoError(t, err)
	require.Equal(t, nodes_storage.HardLinksCookie{}, cookie)
	f.storage.AssertNumberOfCalls(t, "UpdateRestorationNodeIDMapping", 0)
}

func TestHardlinkBatchRestorerInodeFailure(t *testing.T) {
	f := newHardlinkRestorerTestFixture(t)
	defer f.close(t)

	expectedErr := errors.New("inode creation failed")
	batch := []nfs.Node{
		{NodeID: 10, ParentNodeID: 2, Name: "file", Type: nfs.NODE_KIND_FILE},
		{NodeID: 10, ParentNodeID: 2, Name: "link", Type: nfs.NODE_KIND_FILE},
	}

	f.storage.On("ListHardLinks", mock.Anything, "snapshot", 2, f.cookie).
		Return(batch, nodes_storage.HardLinksCookie{}, nil).Once()
	f.storage.On(
		"GetDestinationNodeIDs",
		mock.Anything,
		"snapshot",
		"filesystem",
		[]uint64{2},
	).Return(map[uint64]uint64{2: 20}, nil).Once()
	f.storage.On(
		"GetDestinationNodeIDs",
		mock.Anything,
		"snapshot",
		"filesystem",
		[]uint64{10},
	).Return(map[uint64]uint64(nil), nil).Once()
	inode := nfs.Node{
		NodeID: 10, ParentNodeID: 20, Name: "file", Type: nfs.NODE_KIND_FILE,
	}

	f.session.On("CreateNodeIdempotent", mock.Anything, inode).
		Return(uint64(0), expectedErr).Once()

	cookie, err := f.restorer.Restore(f.ctx, f.cookie)

	require.ErrorIs(t, err, expectedErr)
	require.Equal(t, f.cookie, cookie)
	f.session.AssertNumberOfCalls(t, "CreateNodeIdempotent", 1)
	f.storage.AssertNumberOfCalls(t, "UpdateRestorationNodeIDMapping", 0)
}

func TestHardlinkBatchRestorerLinkFailure(t *testing.T) {
	f := newHardlinkRestorerTestFixture(t)
	defer f.close(t)

	expectedErr := errors.New("link creation failed")
	batch := []nfs.Node{
		{NodeID: 10, ParentNodeID: 2, Name: "file", Type: nfs.NODE_KIND_FILE},
		{NodeID: 10, ParentNodeID: 2, Name: "link", Type: nfs.NODE_KIND_FILE},
	}

	f.storage.On("ListHardLinks", mock.Anything, "snapshot", 2, f.cookie).
		Return(batch, nodes_storage.HardLinksCookie{}, nil).Once()
	f.storage.On(
		"GetDestinationNodeIDs",
		mock.Anything,
		"snapshot",
		"filesystem",
		[]uint64{2},
	).Return(map[uint64]uint64{2: 20}, nil).Once()
	f.storage.On(
		"GetDestinationNodeIDs",
		mock.Anything,
		"snapshot",
		"filesystem",
		[]uint64{10},
	).Return(map[uint64]uint64(nil), nil).Once()
	inode := nfs.Node{
		NodeID: 10, ParentNodeID: 20, Name: "file", Type: nfs.NODE_KIND_FILE,
	}

	link := nfs.Node{
		NodeID: 100, ParentNodeID: 20, Name: "link", Type: nfs.NODE_KIND_LINK,
	}

	inodeCall := f.session.On("CreateNodeIdempotent", mock.Anything, inode).
		Return(uint64(100), nil).Once()
	f.session.On("CreateNodeIdempotent", mock.Anything, link).
		Return(uint64(0), expectedErr).Once().NotBefore(inodeCall)

	cookie, err := f.restorer.Restore(f.ctx, f.cookie)

	require.ErrorIs(t, err, expectedErr)
	require.Equal(t, f.cookie, cookie)
	f.storage.AssertNumberOfCalls(t, "UpdateRestorationNodeIDMapping", 0)
}
