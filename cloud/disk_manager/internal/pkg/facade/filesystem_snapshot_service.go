package facade

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	filesystem_snapshot "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem_snapshot"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

type filesystemSnapshotService struct {
	taskScheduler tasks.Scheduler
	service       filesystem_snapshot.Service
}

func (s *filesystemSnapshotService) Create(
	ctx context.Context,
	req *disk_manager.CreateFilesystemSnapshotRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.CreateFilesystemSnapshot(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *filesystemSnapshotService) Delete(
	ctx context.Context,
	req *disk_manager.DeleteFilesystemSnapshotRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.DeleteFilesystemSnapshot(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

////////////////////////////////////////////////////////////////////////////////

func RegisterFilesystemSnapshotService(
	server *grpc.Server,
	taskScheduler tasks.Scheduler,
	service filesystem_snapshot.Service,
) {

	disk_manager.RegisterFilesystemSnapshotServiceServer(server, &filesystemSnapshotService{
		taskScheduler: taskScheduler,
		service:       service,
	})
}
