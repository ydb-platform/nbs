package placementgroup

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/placementgroup/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func preparePlacementStrategy(
	strategy disk_manager.PlacementStrategy,
) (types.PlacementStrategy, error) {

	switch strategy {
	case disk_manager.PlacementStrategy_PLACEMENT_STRATEGY_UNSPECIFIED:
		return 0, errors.NewNonRetriableErrorf("placement strategy is required")
	case disk_manager.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD:
		return types.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD, nil
	case disk_manager.PlacementStrategy_PLACEMENT_STRATEGY_PARTITION:
		return types.PlacementStrategy_PLACEMENT_STRATEGY_PARTITION, nil
	default:
		return 0, errors.NewNonRetriableErrorf(
			"unknown placement strategy %v",
			strategy,
		)
	}
}

func getPlacementStrategy(
	strategy types.PlacementStrategy,
) disk_manager.PlacementStrategy {

	switch strategy {
	case types.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD:
		return disk_manager.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD
	case types.PlacementStrategy_PLACEMENT_STRATEGY_PARTITION:
		return disk_manager.PlacementStrategy_PLACEMENT_STRATEGY_PARTITION
	default:
		return disk_manager.PlacementStrategy_PLACEMENT_STRATEGY_UNSPECIFIED
	}
}

////////////////////////////////////////////////////////////////////////////////

type service struct {
	taskScheduler tasks.Scheduler
	nbsFactory    nbs.Factory
}

func (s *service) CreatePlacementGroup(
	ctx context.Context,
	req *disk_manager.CreatePlacementGroupRequest,
) (string, error) {

	if len(req.GroupId.ZoneId) == 0 ||
		len(req.GroupId.GroupId) == 0 {

		return "", errors.NewNonRetriableErrorf(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	placementStrategy, err := preparePlacementStrategy(req.PlacementStrategy)
	if err != nil {
		return "", err
	}

	return s.taskScheduler.ScheduleTask(
		ctx,
		"placement_group.CreatePlacementGroup",
		"",
		&protos.CreatePlacementGroupRequest{
			ZoneId:                  req.GroupId.ZoneId,
			GroupId:                 req.GroupId.GroupId,
			PlacementStrategy:       placementStrategy,
			PlacementPartitionCount: req.PlacementPartitionCount,
		},
		"",
		"",
	)
}

func (s *service) DeletePlacementGroup(
	ctx context.Context,
	req *disk_manager.DeletePlacementGroupRequest,
) (string, error) {

	if len(req.GroupId.ZoneId) == 0 ||
		len(req.GroupId.GroupId) == 0 {

		return "", errors.NewNonRetriableErrorf(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(
		ctx,
		"placement_group.DeletePlacementGroup",
		"",
		&protos.DeletePlacementGroupRequest{
			ZoneId:  req.GroupId.ZoneId,
			GroupId: req.GroupId.GroupId,
		},
		"",
		"",
	)
}

func (s *service) AlterPlacementGroupMembership(
	ctx context.Context,
	req *disk_manager.AlterPlacementGroupMembershipRequest,
) (string, error) {

	if len(req.GroupId.ZoneId) == 0 ||
		len(req.GroupId.GroupId) == 0 {

		return "", errors.NewNonRetriableErrorf(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(
		ctx,
		"placement_group.AlterPlacementGroupMembership",
		"",
		&protos.AlterPlacementGroupMembershipRequest{
			ZoneId:                  req.GroupId.ZoneId,
			GroupId:                 req.GroupId.GroupId,
			PlacementPartitionIndex: req.PlacementPartitionIndex,
			DisksToAdd:              req.DisksToAdd,
			DisksToRemove:           req.DisksToRemove,
		},
		"",
		"",
	)
}

func (s *service) ListPlacementGroups(
	ctx context.Context,
	req *disk_manager.ListPlacementGroupsRequest,
) (*disk_manager.ListPlacementGroupsResponse, error) {

	if len(req.ZoneId) == 0 {
		return nil, errors.NewNonRetriableErrorf(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	client, err := s.nbsFactory.GetClient(ctx, req.ZoneId)
	if err != nil {
		return nil, err
	}

	ids, err := client.ListPlacementGroups(ctx)
	if err != nil {
		return nil, err
	}

	return &disk_manager.ListPlacementGroupsResponse{
		GroupIds: ids,
	}, nil
}

func (s *service) DescribePlacementGroup(
	ctx context.Context,
	req *disk_manager.DescribePlacementGroupRequest,
) (*disk_manager.PlacementGroup, error) {

	if len(req.GroupId.ZoneId) == 0 ||
		len(req.GroupId.GroupId) == 0 {

		return nil, errors.NewNonRetriableErrorf(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	client, err := s.nbsFactory.GetClient(ctx, req.GroupId.ZoneId)
	if err != nil {
		return nil, err
	}

	group, err := client.DescribePlacementGroup(ctx, req.GroupId.GroupId)
	if err != nil {
		return nil, err
	}

	return &disk_manager.PlacementGroup{
		GroupId: &disk_manager.GroupId{
			ZoneId:  req.GroupId.ZoneId,
			GroupId: group.GroupID,
		},
		PlacementStrategy:       getPlacementStrategy(group.PlacementStrategy),
		PlacementPartitionCount: group.PlacementPartitionCount,
		DiskIds:                 group.DiskIDs,
		Racks:                   group.Racks,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func NewService(
	taskScheduler tasks.Scheduler,
	nbsFactory nbs.Factory,
) Service {

	return &service{
		taskScheduler: taskScheduler,
		nbsFactory:    nbsFactory,
	}
}
