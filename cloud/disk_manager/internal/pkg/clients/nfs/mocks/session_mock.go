package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
)

////////////////////////////////////////////////////////////////////////////////

type SessionMock struct {
	mock.Mock
}

func (s *SessionMock) ListNodes(
	ctx context.Context,
	parentNodeID uint64,
	cookie string,
	maxBytes uint32,
	unsafe bool,
) ([]nfs.Node, string, error) {

	args := s.Called(ctx, parentNodeID, cookie, maxBytes, unsafe)
	res, _ := args.Get(0).([]nfs.Node)
	return res, args.String(1), args.Error(2)
}

func (s *SessionMock) CreateCheckpoint(
	ctx context.Context,
	filesystemID string,
	checkpointID string,
	nodeID uint64,
) error {

	args := s.Called(ctx, filesystemID, checkpointID, nodeID)
	return args.Error(0)
}

func (s *SessionMock) CreateNode(
	ctx context.Context,
	node nfs.Node,
) (uint64, error) {

	args := s.Called(ctx, node)
	return args.Get(0).(uint64), args.Error(1)
}

func (s *SessionMock) SafeCreateNode(
	ctx context.Context,
	node nfs.Node,
) (uint64, error) {
	args := s.Called(ctx, node)
	return args.Get(0).(uint64), args.Error(1)
}

func (s *SessionMock) ReadLink(
	ctx context.Context,
	nodeID uint64,
) ([]byte, error) {

	args := s.Called(ctx, nodeID)
	res, _ := args.Get(0).([]byte)
	return res, args.Error(1)
}

func (s *SessionMock) GetNodeAttr(
	ctx context.Context,
	parentNodeID uint64,
	name string,
) (nfs.Node, error) {

	args := s.Called(ctx, parentNodeID, name)
	res, _ := args.Get(0).(nfs.Node)
	return res, args.Error(1)
}

func (s *SessionMock) Close(ctx context.Context) error {
	args := s.Called(ctx)
	return args.Error(0)
}

////////////////////////////////////////////////////////////////////////////////

func NewSessionMock() *SessionMock {
	return &SessionMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that SessionMock implements nfs.Session.
func assertSessionMockIsSession(arg *SessionMock) nfs.Session {
	return arg
}
