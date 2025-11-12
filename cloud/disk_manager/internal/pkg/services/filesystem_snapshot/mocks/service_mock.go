package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	filesystem_snapshot "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem_snapshot"
)

////////////////////////////////////////////////////////////////////////////////

type ServiceMock struct {
	mock.Mock
}

func (s *ServiceMock) CreateFilesystemSnapshot(
	ctx context.Context,
	req *disk_manager.CreateFilesystemSnapshotRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) DeleteFilesystemSnapshot(
	ctx context.Context,
	req *disk_manager.DeleteFilesystemSnapshotRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewServiceMock() *ServiceMock {
	return &ServiceMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that ServiceMock implements Service.
func assertServiceMockIsService(arg *ServiceMock) filesystem_snapshot.Service {
	return arg
}
