package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	filesystembackups "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem_backups"
)

////////////////////////////////////////////////////////////////////////////////

type ServiceMock struct {
	mock.Mock
}

func (s *ServiceMock) CreateFilesystemBackup(
	ctx context.Context,
	req *disk_manager.CreateFilesystemBackupRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) DeleteFilesystemBackup(
	ctx context.Context,
	req *disk_manager.DeleteFilesystemBackupRequest,
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
func assertServiceMockIsService(arg *ServiceMock) filesystembackups.Service {
	return arg
}
