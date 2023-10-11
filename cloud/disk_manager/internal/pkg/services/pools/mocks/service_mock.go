package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
)

////////////////////////////////////////////////////////////////////////////////

type ServiceMock struct {
	mock.Mock
}

func (s *ServiceMock) AcquireBaseDisk(
	ctx context.Context,
	req *protos.AcquireBaseDiskRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) ReleaseBaseDisk(
	ctx context.Context,
	req *protos.ReleaseBaseDiskRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) RebaseOverlayDisk(
	ctx context.Context,
	req *protos.RebaseOverlayDiskRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) ConfigurePool(
	ctx context.Context,
	req *protos.ConfigurePoolRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) DeletePool(
	ctx context.Context,
	req *protos.DeletePoolRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) ImageDeleting(
	ctx context.Context,
	req *protos.ImageDeletingRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) IsPoolConfigured(
	ctx context.Context,
	imageID string,
	zoneID string,
) (bool, error) {

	args := s.Called(ctx, imageID, zoneID)
	return args.Bool(0), args.Error(1)
}

func (s *ServiceMock) RetireBaseDisk(
	ctx context.Context,
	req *protos.RetireBaseDiskRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) RetireBaseDisks(
	ctx context.Context,
	req *protos.RetireBaseDisksRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) OptimizeBaseDisks(
	ctx context.Context,
) (string, error) {

	args := s.Called(ctx)
	return args.String(0), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewServiceMock() *ServiceMock {
	return &ServiceMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that ServiceMock implements Service.
func assertServiceMockIsService(arg *ServiceMock) pools.Service {
	return arg
}
