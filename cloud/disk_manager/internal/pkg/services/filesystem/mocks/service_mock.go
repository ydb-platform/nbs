package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem"
)

////////////////////////////////////////////////////////////////////////////////

type ServiceMock struct {
	mock.Mock
}

func (s *ServiceMock) CreateFilesystem(
	ctx context.Context,
	req *disk_manager.CreateFilesystemRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) DeleteFilesystem(
	ctx context.Context,
	req *disk_manager.DeleteFilesystemRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) ResizeFilesystem(
	ctx context.Context,
	req *disk_manager.ResizeFilesystemRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) DescribeFilesystemModel(
	ctx context.Context,
	req *disk_manager.DescribeFilesystemModelRequest,
) (*disk_manager.FilesystemModel, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*disk_manager.FilesystemModel)
	return res, args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewServiceMock() *ServiceMock {
	return &ServiceMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that ServiceMock implements Service.
func assertServiceMockIsService(arg *ServiceMock) filesystem.Service {
	return arg
}
