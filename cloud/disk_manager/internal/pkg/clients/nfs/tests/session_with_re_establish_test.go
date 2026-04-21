package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/mocks"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	nfs_client_mocks "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

var invalidSessionError = &nfs_client.ClientError{
	Code:    nfs_client.E_FS_INVALID_SESSION,
	Message: "invalid session",
}

var errOther = fmt.Errorf("some other error")

var reEstablishedSession = nfs_client.Session{
	SessionID:    "re-established-session",
	SessionSeqNo: 2,
	FileSystemID: "fs-1",
	CheckpointId: "cp-1",
	ClientID:     "client-1",
}

func newTestCtx() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestSessionWithReEstablishCreateCheckpoint(t *testing.T) {
	ctx := newTestCtx()

	sessionMock := nfs_mocks.NewSessionMock()
	nfsMock := nfs_client_mocks.NewClientInterfaceMock()

	sessionMock.On("CreateCheckpoint", mock.Anything, "fs-1", "cp-1", uint64(42)).
		Return(invalidSessionError).Once()
	nfsMock.On(
		"CreateSession",
		mock.Anything,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	).Return(reEstablishedSession, nil).Once()
	sessionMock.On("SetSession", reEstablishedSession).Once()
	sessionMock.On("CreateCheckpoint", mock.Anything, "fs-1", "cp-1", uint64(42)).
		Return(nil).Once()

	s := nfs.NewSessionWithReEstablish(
		sessionMock,
		nfsMock,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	)

	err := s.CreateCheckpoint(ctx, "fs-1", "cp-1", 42)
	require.NoError(t, err)

	sessionMock.On("CreateCheckpoint", mock.Anything, "fs-1", "cp-1", uint64(42)).
		Return(errOther).Once()

	err = s.CreateCheckpoint(ctx, "fs-1", "cp-1", 42)
	require.ErrorIs(t, err, errOther)

	sessionMock.AssertExpectations(t)
	nfsMock.AssertExpectations(t)
}

func TestSessionWithReEstablishListNodes(t *testing.T) {
	ctx := newTestCtx()

	sessionMock := nfs_mocks.NewSessionMock()
	nfsMock := nfs_client_mocks.NewClientInterfaceMock()

	expectedNodes := []nfs.Node{{NodeID: 1, Name: "a"}}

	sessionMock.On("ListNodes", mock.Anything, uint64(0), "", uint32(1024), false).
		Return([]nfs.Node(nil), "", invalidSessionError).Once()
	nfsMock.On(
		"CreateSession",
		mock.Anything,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	).Return(reEstablishedSession, nil).Once()
	sessionMock.On("SetSession", reEstablishedSession).Once()
	sessionMock.On("ListNodes", mock.Anything, uint64(0), "", uint32(1024), false).
		Return(expectedNodes, "next", nil).Once()

	s := nfs.NewSessionWithReEstablish(sessionMock, nfsMock, "fs-1", "client-1", "cp-1", true)

	nodes, cookie, err := s.ListNodes(ctx, 0, "", 1024, false)
	require.NoError(t, err)
	require.Equal(t, expectedNodes, nodes)
	require.Equal(t, "next", cookie)

	sessionMock.On("ListNodes", mock.Anything, uint64(0), "", uint32(1024), false).
		Return([]nfs.Node(nil), "", errOther).Once()

	_, _, err = s.ListNodes(ctx, 0, "", 1024, false)
	require.ErrorIs(t, err, errOther)

	sessionMock.AssertExpectations(t)
	nfsMock.AssertExpectations(t)
}

func TestSessionWithReEstablishCreateNode(t *testing.T) {
	ctx := newTestCtx()

	sessionMock := nfs_mocks.NewSessionMock()
	nfsMock := nfs_client_mocks.NewClientInterfaceMock()

	testNode := nfs.Node{Name: "test"}

	sessionMock.On("CreateNode", mock.Anything, testNode).
		Return(uint64(0), invalidSessionError).Once()
	nfsMock.On(
		"CreateSession",
		mock.Anything,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	).Return(reEstablishedSession, nil).Once()
	sessionMock.On("SetSession", reEstablishedSession).Once()
	sessionMock.On("CreateNode", mock.Anything, testNode).
		Return(uint64(42), nil).Once()

	s := nfs.NewSessionWithReEstablish(
		sessionMock,
		nfsMock,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	)

	nodeID, err := s.CreateNode(ctx, testNode)
	require.NoError(t, err)
	require.Equal(t, uint64(42), nodeID)

	sessionMock.On("CreateNode", mock.Anything, testNode).
		Return(uint64(0), errOther).Once()

	_, err = s.CreateNode(ctx, testNode)
	require.ErrorIs(t, err, errOther)

	sessionMock.AssertExpectations(t)
	nfsMock.AssertExpectations(t)
}

func TestSessionWithReEstablishCreateNodeIdempotent(t *testing.T) {
	ctx := newTestCtx()

	sessionMock := nfs_mocks.NewSessionMock()
	nfsMock := nfs_client_mocks.NewClientInterfaceMock()

	testNode := nfs.Node{Name: "test"}

	sessionMock.On("CreateNodeIdempotent", mock.Anything, testNode).
		Return(uint64(0), invalidSessionError).Once()
	nfsMock.On(
		"CreateSession",
		mock.Anything,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	).Return(reEstablishedSession, nil).Once()
	sessionMock.On("SetSession", reEstablishedSession).Once()
	sessionMock.On("CreateNodeIdempotent", mock.Anything, testNode).
		Return(uint64(42), nil).Once()

	s := nfs.NewSessionWithReEstablish(
		sessionMock,
		nfsMock,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	)

	nodeID, err := s.CreateNodeIdempotent(ctx, testNode)
	require.NoError(t, err)
	require.Equal(t, uint64(42), nodeID)

	sessionMock.On("CreateNodeIdempotent", mock.Anything, testNode).
		Return(uint64(0), errOther).Once()

	_, err = s.CreateNodeIdempotent(ctx, testNode)
	require.ErrorIs(t, err, errOther)

	sessionMock.AssertExpectations(t)
	nfsMock.AssertExpectations(t)
}

func TestSessionWithReEstablishReadLink(t *testing.T) {
	ctx := newTestCtx()

	sessionMock := nfs_mocks.NewSessionMock()
	nfsMock := nfs_client_mocks.NewClientInterfaceMock()

	sessionMock.On("ReadLink", mock.Anything, uint64(42)).
		Return([]byte(nil), invalidSessionError).Once()
	nfsMock.On(
		"CreateSession",
		mock.Anything,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	).Return(reEstablishedSession, nil).Once()
	sessionMock.On("SetSession", reEstablishedSession).Once()
	sessionMock.On("ReadLink", mock.Anything, uint64(42)).
		Return([]byte("/target"), nil).Once()

	s := nfs.NewSessionWithReEstablish(
		sessionMock,
		nfsMock,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	)

	data, err := s.ReadLink(ctx, 42)
	require.NoError(t, err)
	require.Equal(t, []byte("/target"), data)

	sessionMock.On("ReadLink", mock.Anything, uint64(42)).
		Return([]byte(nil), errOther).Once()

	_, err = s.ReadLink(ctx, 42)
	require.ErrorIs(t, err, errOther)

	sessionMock.AssertExpectations(t)
	nfsMock.AssertExpectations(t)
}

func TestSessionWithReEstablishGetNodeAttr(t *testing.T) {
	ctx := newTestCtx()

	sessionMock := nfs_mocks.NewSessionMock()
	nfsMock := nfs_client_mocks.NewClientInterfaceMock()

	expectedNode := nfs.Node{NodeID: 42, Name: "testfile"}

	sessionMock.On("GetNodeAttr", mock.Anything, uint64(1), "testfile").
		Return(nfs.Node{}, invalidSessionError).Once()
	nfsMock.On(
		"CreateSession",
		mock.Anything,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	).Return(reEstablishedSession, nil).Once()
	sessionMock.On("SetSession", reEstablishedSession).Once()
	sessionMock.On("GetNodeAttr", mock.Anything, uint64(1), "testfile").
		Return(expectedNode, nil).Once()

	s := nfs.NewSessionWithReEstablish(
		sessionMock,
		nfsMock,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	)

	node, err := s.GetNodeAttr(ctx, 1, "testfile")
	require.NoError(t, err)
	require.Equal(t, expectedNode, node)

	sessionMock.On("GetNodeAttr", mock.Anything, uint64(1), "testfile").
		Return(nfs.Node{}, errOther).Once()

	_, err = s.GetNodeAttr(ctx, 1, "testfile")
	require.ErrorIs(t, err, errOther)

	sessionMock.AssertExpectations(t)
	nfsMock.AssertExpectations(t)
}

func TestSessionWithReEstablishMaxRetriesExceeded(t *testing.T) {
	ctx := newTestCtx()

	sessionMock := nfs_mocks.NewSessionMock()
	nfsMock := nfs_client_mocks.NewClientInterfaceMock()

	testNode := nfs.Node{Name: "test"}

	// All calls return invalid session error, exhausting retries.
	sessionMock.On("CreateNode", mock.Anything, testNode).
		Return(uint64(0), invalidSessionError).
		Times(nfs.SessionReEstablishMaxRetries + 1)
	nfsMock.On(
		"CreateSession",
		mock.Anything,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	).Return(reEstablishedSession, nil).
		Times(nfs.SessionReEstablishMaxRetries)
	sessionMock.On("SetSession", reEstablishedSession).
		Times(nfs.SessionReEstablishMaxRetries)

	s := nfs.NewSessionWithReEstablish(
		sessionMock,
		nfsMock,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	)

	_, err := s.CreateNode(ctx, testNode)
	require.Error(t, err)

	var retriableErr *errors.RetriableError
	require.ErrorAs(t, err, &retriableErr)
	require.ErrorIs(t, retriableErr.Err, invalidSessionError)

	sessionMock.AssertExpectations(t)
	nfsMock.AssertExpectations(t)
}

func TestSessionWithReEstablishSucceedsAfterTwoRetries(t *testing.T) {
	ctx := newTestCtx()

	sessionMock := nfs_mocks.NewSessionMock()
	nfsMock := nfs_client_mocks.NewClientInterfaceMock()

	testNode := nfs.Node{Name: "test"}

	// First two calls return invalid session, third succeeds.
	sessionMock.On("CreateNode", mock.Anything, testNode).
		Return(uint64(0), invalidSessionError).Twice()
	nfsMock.On(
		"CreateSession",
		mock.Anything,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	).Return(reEstablishedSession, nil).Twice()
	sessionMock.On("SetSession", reEstablishedSession).Twice()
	sessionMock.On("CreateNode", mock.Anything, testNode).
		Return(uint64(42), nil).Once()

	s := nfs.NewSessionWithReEstablish(
		sessionMock,
		nfsMock,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	)

	nodeID, err := s.CreateNode(ctx, testNode)
	require.NoError(t, err)
	require.Equal(t, uint64(42), nodeID)

	sessionMock.AssertExpectations(t)
	nfsMock.AssertExpectations(t)
}

func TestSessionWithReEstablishUnlinkNode(t *testing.T) {
	ctx := newTestCtx()

	sessionMock := nfs_mocks.NewSessionMock()
	nfsMock := nfs_client_mocks.NewClientInterfaceMock()

	sessionMock.On("UnlinkNode", mock.Anything, uint64(1), "testfile", false).
		Return(invalidSessionError).Once()
	nfsMock.On(
		"CreateSession",
		mock.Anything,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	).Return(reEstablishedSession, nil).Once()
	sessionMock.On("SetSession", reEstablishedSession).Once()
	sessionMock.On("UnlinkNode", mock.Anything, uint64(1), "testfile", false).
		Return(nil).Once()

	s := nfs.NewSessionWithReEstablish(
		sessionMock,
		nfsMock,
		"fs-1",
		"client-1",
		"cp-1",
		true,
	)

	err := s.UnlinkNode(ctx, 1, "testfile", false)
	require.NoError(t, err)

	sessionMock.On("UnlinkNode", mock.Anything, uint64(1), "testfile", false).
		Return(errOther).Once()

	err = s.UnlinkNode(ctx, 1, "testfile", false)
	require.ErrorIs(t, err, errOther)

	sessionMock.AssertExpectations(t)
	nfsMock.AssertExpectations(t)
}
