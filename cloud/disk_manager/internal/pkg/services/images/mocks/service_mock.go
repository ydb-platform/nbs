package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images"
)

////////////////////////////////////////////////////////////////////////////////

type ServiceMock struct {
	mock.Mock
}

func (s *ServiceMock) CreateImage(
	ctx context.Context,
	req *disk_manager.CreateImageRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) DeleteImage(
	ctx context.Context,
	req *disk_manager.DeleteImageRequest,
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
func assertServiceMockIsService(arg *ServiceMock) images.Service {
	return arg
}
