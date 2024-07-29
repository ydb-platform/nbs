package facade

import (
	"context"
	"math"

	"github.com/golang/protobuf/ptypes/empty"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

////////////////////////////////////////////////////////////////////////////////

type privateService struct {
	taskScheduler   tasks.Scheduler
	nbsFactory      nbs.Factory
	poolService     pools.Service
	resourceStorage resources.Storage
	taskStorage     tasks_storage.Storage
}

func (s *privateService) ScheduleBlankOperation(
	ctx context.Context,
	req *empty.Empty,
) (*disk_manager.Operation, error) {

	taskID, err := s.taskScheduler.ScheduleBlankTask(ctx)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *privateService) RebaseOverlayDisk(
	ctx context.Context,
	req *api.RebaseOverlayDiskRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.poolService.RebaseOverlayDisk(
		ctx,
		&pools_protos.RebaseOverlayDiskRequest{
			OverlayDisk: &types.Disk{
				ZoneId: req.DiskId.ZoneId,
				DiskId: req.DiskId.DiskId,
			},
			BaseDiskId:       req.BaseDiskId,
			TargetBaseDiskId: req.TargetBaseDiskId,
			SlotGeneration:   req.SlotGeneration,
		},
	)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *privateService) RetireBaseDisk(
	ctx context.Context,
	req *api.RetireBaseDiskRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.poolService.RetireBaseDisk(
		ctx,
		&pools_protos.RetireBaseDiskRequest{
			BaseDiskId: req.BaseDiskId,
			SrcDisk: &types.Disk{
				ZoneId: req.SrcDiskId.ZoneId,
				DiskId: req.SrcDiskId.DiskId,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *privateService) RetireBaseDisks(
	ctx context.Context,
	req *api.RetireBaseDisksRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.poolService.RetireBaseDisks(
		ctx,
		&pools_protos.RetireBaseDisksRequest{
			ImageId:          req.ImageId,
			ZoneId:           req.ZoneId,
			UseBaseDiskAsSrc: req.UseBaseDiskAsSrc,
			UseImageSize:     req.UseImageSize,
		},
	)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *privateService) OptimizeBaseDisks(
	ctx context.Context,
	req *empty.Empty,
) (*disk_manager.Operation, error) {

	taskID, err := s.poolService.OptimizeBaseDisks(ctx)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *privateService) ConfigurePool(
	ctx context.Context,
	req *api.ConfigurePoolRequest,
) (*disk_manager.Operation, error) {

	// NBS-1375.
	if !s.nbsFactory.HasClient(req.ZoneId) {
		return nil, errors.NewInvalidArgumentError(
			"unknown zone id: %v",
			req.ZoneId,
		)
	}

	if req.Capacity < 0 || req.Capacity > math.MaxUint32 {
		return nil, errors.NewInvalidArgumentError(
			"invalid capacity: %v",
			req.Capacity,
		)
	}

	taskID, err := s.poolService.ConfigurePool(
		ctx,
		&pools_protos.ConfigurePoolRequest{
			ImageId:      req.ImageId,
			ZoneId:       req.ZoneId,
			Capacity:     uint32(req.Capacity),
			UseImageSize: req.UseImageSize,
		},
	)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *privateService) DeletePool(
	ctx context.Context,
	req *api.DeletePoolRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.poolService.DeletePool(
		ctx,
		&pools_protos.DeletePoolRequest{
			ImageId: req.ImageId,
			ZoneId:  req.ZoneId,
		},
	)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *privateService) ListDisks(
	ctx context.Context,
	req *api.ListDisksRequest,
) (*api.ListDisksResponse, error) {

	ids, err := s.resourceStorage.ListDisks(
		ctx,
		req.FolderId,
		req.CreatingBefore.AsTime(),
	)
	if err != nil {
		return nil, err
	}

	return &api.ListDisksResponse{DiskIds: ids}, nil
}

func (s *privateService) ListImages(
	ctx context.Context,
	req *api.ListImagesRequest,
) (*api.ListImagesResponse, error) {

	ids, err := s.resourceStorage.ListImages(
		ctx,
		req.FolderId,
		req.CreatingBefore.AsTime(),
	)
	if err != nil {
		return nil, err
	}

	return &api.ListImagesResponse{ImageIds: ids}, nil
}

func (s *privateService) ListSnapshots(
	ctx context.Context,
	req *api.ListSnapshotsRequest,
) (*api.ListSnapshotsResponse, error) {

	ids, err := s.resourceStorage.ListSnapshots(
		ctx,
		req.FolderId,
		req.CreatingBefore.AsTime(),
	)
	if err != nil {
		return nil, err
	}

	return &api.ListSnapshotsResponse{SnapshotIds: ids}, nil
}

func (s *privateService) ListFilesystems(
	ctx context.Context,
	req *api.ListFilesystemsRequest,
) (*api.ListFilesystemsResponse, error) {

	ids, err := s.resourceStorage.ListFilesystems(
		ctx,
		req.FolderId,
		req.CreatingBefore.AsTime(),
	)
	if err != nil {
		return nil, err
	}

	return &api.ListFilesystemsResponse{FilesystemIds: ids}, nil
}

func (s *privateService) ListPlacementGroups(
	ctx context.Context,
	req *api.ListPlacementGroupsRequest,
) (*api.ListPlacementGroupsResponse, error) {

	ids, err := s.resourceStorage.ListPlacementGroups(
		ctx,
		req.FolderId,
		req.CreatingBefore.AsTime(),
	)
	if err != nil {
		return nil, err
	}

	return &api.ListPlacementGroupsResponse{PlacementGroupIds: ids}, nil
}

func (s *privateService) GetAliveNodes(
	ctx context.Context,
	req *empty.Empty,
) (*api.GetAliveNodesResponse, error) {

	aliveNodes, err := s.taskStorage.GetAliveNodes(ctx)
	if err != nil {
		return nil, err
	}

	nodes := make([]*api.GetAliveNodesResponse_Node, 0, len(aliveNodes))

	for _, node := range aliveNodes {
		nodes = append(nodes, &api.GetAliveNodesResponse_Node{
			Host:              node.Host,
			LastHeartbeat:     timestamppb.New(node.LastHeartbeat),
			InflightTaskCount: node.InflightTaskCount,
		})
	}
	return &api.GetAliveNodesResponse{Nodes: nodes}, nil
}

////////////////////////////////////////////////////////////////////////////////

func RegisterPrivateService(
	server *grpc.Server,
	taskScheduler tasks.Scheduler,
	nbsFactory nbs.Factory,
	poolService pools.Service,
	resourceStorage resources.Storage,
	taskStorage tasks_storage.Storage,
) {

	api.RegisterPrivateServiceServer(server, &privateService{
		taskScheduler:   taskScheduler,
		nbsFactory:      nbsFactory,
		poolService:     poolService,
		resourceStorage: resourceStorage,
		taskStorage:     taskStorage,
	})
}
