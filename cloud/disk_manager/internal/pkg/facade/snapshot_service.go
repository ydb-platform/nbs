package facade

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

type snapshotService struct {
	taskScheduler tasks.Scheduler
	service       snapshots.Service
}

func (s *snapshotService) Create(
	ctx context.Context,
	req *disk_manager.CreateSnapshotRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.CreateSnapshot(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *snapshotService) Delete(
	ctx context.Context,
	req *disk_manager.DeleteSnapshotRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.DeleteSnapshot(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

////////////////////////////////////////////////////////////////////////////////

func RegisterSnapshotService(
	server *grpc.Server,
	taskScheduler tasks.Scheduler,
	service snapshots.Service,
) {

	disk_manager.RegisterSnapshotServiceServer(server, &snapshotService{
		taskScheduler: taskScheduler,
		service:       service,
	})
}
