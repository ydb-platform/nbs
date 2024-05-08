package facade

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/placementgroup"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

type placementGroupService struct {
	taskScheduler tasks.Scheduler
	service       placementgroup.Service
}

func (s *placementGroupService) Create(
	ctx context.Context,
	req *disk_manager.CreatePlacementGroupRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.CreatePlacementGroup(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *placementGroupService) Delete(
	ctx context.Context,
	req *disk_manager.DeletePlacementGroupRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.DeletePlacementGroup(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *placementGroupService) Alter(
	ctx context.Context,
	req *disk_manager.AlterPlacementGroupMembershipRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.AlterPlacementGroupMembership(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *placementGroupService) List(
	ctx context.Context,
	req *disk_manager.ListPlacementGroupsRequest,
) (*disk_manager.ListPlacementGroupsResponse, error) {

	return s.service.ListPlacementGroups(ctx, req)
}

func (s *placementGroupService) Describe(
	ctx context.Context,
	req *disk_manager.DescribePlacementGroupRequest,
) (*disk_manager.PlacementGroup, error) {

	return s.service.DescribePlacementGroup(ctx, req)
}

////////////////////////////////////////////////////////////////////////////////

func RegisterPlacementGroupService(
	server *grpc.Server,
	taskScheduler tasks.Scheduler,
	service placementgroup.Service,
) {

	disk_manager.RegisterPlacementGroupServiceServer(server, &placementGroupService{
		taskScheduler: taskScheduler,
		service:       service,
	})
}
