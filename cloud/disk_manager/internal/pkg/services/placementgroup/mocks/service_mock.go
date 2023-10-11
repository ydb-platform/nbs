package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/placementgroup"
)

////////////////////////////////////////////////////////////////////////////////

type ServiceMock struct {
	mock.Mock
}

func (s *ServiceMock) CreatePlacementGroup(
	ctx context.Context,
	req *disk_manager.CreatePlacementGroupRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) DeletePlacementGroup(
	ctx context.Context,
	req *disk_manager.DeletePlacementGroupRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) AlterPlacementGroupMembership(
	ctx context.Context,
	req *disk_manager.AlterPlacementGroupMembershipRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *ServiceMock) ListPlacementGroups(
	ctx context.Context,
	req *disk_manager.ListPlacementGroupsRequest,
) (*disk_manager.ListPlacementGroupsResponse, error) {

	args := s.Called(ctx, req)
	return args.Get(0).(*disk_manager.ListPlacementGroupsResponse), args.Error(1)
}

func (s *ServiceMock) DescribePlacementGroup(
	ctx context.Context,
	req *disk_manager.DescribePlacementGroupRequest,
) (*disk_manager.PlacementGroup, error) {

	args := s.Called(ctx, req)
	return args.Get(0).(*disk_manager.PlacementGroup), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewServiceMock() *ServiceMock {
	return &ServiceMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that ServiceMock implements Service.
func assertServiceMockIsService(arg *ServiceMock) placementgroup.Service {
	return arg
}
