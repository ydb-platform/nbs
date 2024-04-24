package facade

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

type filesystemService struct {
	taskScheduler tasks.Scheduler
	service       filesystem.Service
}

func (s *filesystemService) Create(
	ctx context.Context,
	req *disk_manager.CreateFilesystemRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.CreateFilesystem(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *filesystemService) Delete(
	ctx context.Context,
	req *disk_manager.DeleteFilesystemRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.DeleteFilesystem(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *filesystemService) Resize(
	ctx context.Context,
	req *disk_manager.ResizeFilesystemRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.ResizeFilesystem(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *filesystemService) DescribeModel(
	ctx context.Context,
	req *disk_manager.DescribeFilesystemModelRequest,
) (*disk_manager.FilesystemModel, error) {

	return s.service.DescribeFilesystemModel(ctx, req)
}

////////////////////////////////////////////////////////////////////////////////

func RegisterFilesystemService(
	server *grpc.Server,
	taskScheduler tasks.Scheduler,
	service filesystem.Service,
) {

	disk_manager.RegisterFilesystemServiceServer(server, &filesystemService{
		taskScheduler: taskScheduler,
		service:       service,
	})
}
