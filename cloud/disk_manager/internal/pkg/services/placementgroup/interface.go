package placementgroup

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/api/yandex/cloud/priv/disk_manager/v1"
)

////////////////////////////////////////////////////////////////////////////////

type Service interface {
	CreatePlacementGroup(
		ctx context.Context,
		req *disk_manager.CreatePlacementGroupRequest,
	) (string, error)

	DeletePlacementGroup(
		ctx context.Context,
		req *disk_manager.DeletePlacementGroupRequest,
	) (string, error)

	AlterPlacementGroupMembership(
		ctx context.Context,
		req *disk_manager.AlterPlacementGroupMembershipRequest,
	) (string, error)

	ListPlacementGroups(
		ctx context.Context,
		req *disk_manager.ListPlacementGroupsRequest,
	) (*disk_manager.ListPlacementGroupsResponse, error)

	DescribePlacementGroup(
		ctx context.Context,
		req *disk_manager.DescribePlacementGroupRequest,
	) (*disk_manager.PlacementGroup, error)
}
