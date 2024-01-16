package facade

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/api/operation"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api/yandex/cloud/priv/disk_manager/v1"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

////////////////////////////////////////////////////////////////////////////////

type diskService struct {
	scheduler tasks.Scheduler
	service   disks.Service
}

func (s *diskService) Create(
	ctx context.Context,
	req *disk_manager.CreateDiskRequest,
) (*operation.Operation, error) {

	taskID, err := s.service.CreateDisk(ctx, req)
	if err != nil {
		return nil, err
	}

	return s.scheduler.GetOperation(ctx, taskID)
}

func (s *diskService) Delete(
	ctx context.Context,
	req *disk_manager.DeleteDiskRequest,
) (*operation.Operation, error) {

	taskID, err := s.service.DeleteDisk(ctx, req)
	if err != nil {
		return nil, err
	}

	return s.scheduler.GetOperation(ctx, taskID)
}

func (s *diskService) Resize(
	ctx context.Context,
	req *disk_manager.ResizeDiskRequest,
) (*operation.Operation, error) {

	taskID, err := s.service.ResizeDisk(ctx, req)
	if err != nil {
		return nil, err
	}

	return s.scheduler.GetOperation(ctx, taskID)
}

func (s *diskService) Alter(
	ctx context.Context,
	req *disk_manager.AlterDiskRequest,
) (*operation.Operation, error) {

	taskID, err := s.service.AlterDisk(ctx, req)
	if err != nil {
		return nil, err
	}

	return s.scheduler.GetOperation(ctx, taskID)
}

func (s *diskService) Assign(
	ctx context.Context,
	req *disk_manager.AssignDiskRequest,
) (*operation.Operation, error) {

	taskID, err := s.service.AssignDisk(ctx, req)
	if err != nil {
		return nil, err
	}

	return s.scheduler.GetOperation(ctx, taskID)
}

func (s *diskService) Unassign(
	ctx context.Context,
	req *disk_manager.UnassignDiskRequest,
) (*operation.Operation, error) {

	taskID, err := s.service.UnassignDisk(ctx, req)
	if err != nil {
		return nil, err
	}

	return s.scheduler.GetOperation(ctx, taskID)
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
) (*operation.Operation, error) {

	taskID, err := s.service.MigrateDisk(ctx, req)
	if err != nil {
		return nil, err
	}

	return s.scheduler.GetOperation(ctx, taskID)
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

////////////////////////////////////////////////////////////////////////////////

func RegisterDiskService(
	server *grpc.Server,
	scheduler tasks.Scheduler,
	service disks.Service,
) {

	disk_manager.RegisterDiskServiceServer(server, &diskService{
		scheduler: scheduler,
		service:   service,
	})
}
