package facade

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

////////////////////////////////////////////////////////////////////////////////

type diskService struct {
	taskScheduler tasks.Scheduler
	service       disks.Service
}

func (s *diskService) Create(
	ctx context.Context,
	req *disk_manager.CreateDiskRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.CreateDisk(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *diskService) Delete(
	ctx context.Context,
	req *disk_manager.DeleteDiskRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.DeleteDisk(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *diskService) Resize(
	ctx context.Context,
	req *disk_manager.ResizeDiskRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.ResizeDisk(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *diskService) Alter(
	ctx context.Context,
	req *disk_manager.AlterDiskRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.AlterDisk(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *diskService) Assign(
	ctx context.Context,
	req *disk_manager.AssignDiskRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.AssignDisk(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *diskService) Unassign(
	ctx context.Context,
	req *disk_manager.UnassignDiskRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.UnassignDisk(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *diskService) DescribeModel(
	ctx context.Context,
	req *disk_manager.DescribeDiskModelRequest,
) (*disk_manager.DiskModel, error) {

	return s.service.DescribeDiskModel(ctx, req)
}

func (s *diskService) Stat(
	ctx context.Context,
	req *disk_manager.StatDiskRequest,
) (*disk_manager.DiskStats, error) {

	return s.service.StatDisk(ctx, req)
}

func (s *diskService) Migrate(
	ctx context.Context,
	req *disk_manager.MigrateDiskRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.MigrateDisk(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *diskService) SendMigrationSignal(
	ctx context.Context,
	req *disk_manager.SendMigrationSignalRequest,
) (*emptypb.Empty, error) {

	err := s.service.SendMigrationSignal(ctx, req)
	if err != nil {
		return nil, err
	}

	return new(emptypb.Empty), nil
}

func (s *diskService) Describe(
	ctx context.Context,
	req *disk_manager.DescribeDiskRequest,
) (*disk_manager.DiskParams, error) {

	return s.service.DescribeDisk(ctx, req)
}

func (s *diskService) ListStates(
	ctx context.Context,
	req *disk_manager.ListDiskStatesRequest,
) (*disk_manager.ListDiskStatesResponse, error) {

	return s.service.ListDiskStates(ctx, req)
}

////////////////////////////////////////////////////////////////////////////////

func RegisterDiskService(
	server *grpc.Server,
	taskScheduler tasks.Scheduler,
	service disks.Service,
) {

	disk_manager.RegisterDiskServiceServer(server, &diskService{
		taskScheduler: taskScheduler,
		service:       service,
	})
}
