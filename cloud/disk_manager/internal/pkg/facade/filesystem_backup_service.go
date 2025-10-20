package facade

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

type filesystemBackupService struct {
	taskScheduler tasks.Scheduler
	service       filesystemBackups.Service
}

func (s *filesystemBackupService) Create(
	ctx context.Context,
	req *disk_manager.CreateFilesystemBackupRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.CreateFilesystemBackup(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *filesystemBackupService) Delete(
	ctx context.Context,
	req *disk_manager.DeleteFilesystemBackupRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.DeleteFilesystemBackup(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

////////////////////////////////////////////////////////////////////////////////

func RegisterFilesystemBackupService(
	server *grpc.Server,
	taskScheduler tasks.Scheduler,
	service filesystemBackups.Service,
) {

	disk_manager.RegisterFilesystemBackupServiceServer(server, &filesystemBackupService{
		taskScheduler: taskScheduler,
		service:       service,
	})
}
