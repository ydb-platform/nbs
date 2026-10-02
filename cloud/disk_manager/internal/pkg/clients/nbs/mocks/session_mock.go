package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

type SessionMock struct {
	mock.Mock
}

func (s *SessionMock) BlockSize() uint32 {
	args := s.Called()
	return args.Get(0).(uint32)
}

func (s *SessionMock) BlockCount() uint64 {
	args := s.Called()
	return args.Get(0).(uint64)
}

func (s *SessionMock) IsOverlayDisk() bool {
	args := s.Called()
	return args.Bool(0)
}

func (s *SessionMock) EncryptionDesc() (*types.EncryptionDesc, error) {
	args := s.Called()
	res, _ := args.Get(0).(*types.EncryptionDesc)
	return res, args.Error(1)
}

func (s *SessionMock) IsDiskRegistryBasedDisk() bool {
	args := s.Called()
	return args.Bool(0)
}

func (s *SessionMock) Read(
	ctx context.Context,
	startIndex uint64,
	blockCount uint32,
	checkpointID string,
	data []byte,
	zero *bool,
) error {

	args := s.Called(ctx, startIndex, blockCount, checkpointID, data, zero)
	return args.Error(0)
}

func (s *SessionMock) Write(
	ctx context.Context,
	startIndex uint64,
	data []byte,
) error {

	args := s.Called(ctx, startIndex, data)
	return args.Error(0)
}

func (s *SessionMock) Zero(
	ctx context.Context,
	startIndex uint64,
	blockCount uint32,
) error {

	args := s.Called(ctx, startIndex, blockCount)
	return args.Error(0)
}

func (s *SessionMock) Close(ctx context.Context) {
	s.Called(ctx)
}

////////////////////////////////////////////////////////////////////////////////

func NewSessionMock() *SessionMock {
	return &SessionMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that SessionMock implements nbs.Session.
func assertSessionMockIsSession(arg *SessionMock) nbs.Session {
	return arg
}
