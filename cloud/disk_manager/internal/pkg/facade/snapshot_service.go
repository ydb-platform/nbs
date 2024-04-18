package facade

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/api/operation"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

type snapshotService struct {
	scheduler tasks.Scheduler
	service   snapshots.Service
}

func (s *snapshotService) Create(
	ctx context.Context,
	req *disk_manager.CreateSnapshotRequest,
) (*operation.Operation, error) {

	taskID, err := s.service.CreateSnapshot(ctx, req)
	if err != nil {
		return nil, err
	}

	return s.scheduler.GetOperation(ctx, taskID)
}

func (s *snapshotService) Delete(
	ctx context.Context,
	req *disk_manager.DeleteSnapshotRequest,
) (*operation.Operation, error) {

	taskID, err := s.service.DeleteSnapshot(ctx, req)
	if err != nil {
		return nil, err
	}

	return s.scheduler.GetOperation(ctx, taskID)
}

////////////////////////////////////////////////////////////////////////////////

func RegisterSnapshotService(
	server *grpc.Server,
	scheduler tasks.Scheduler,
	service snapshots.Service,
) {

	disk_manager.RegisterSnapshotServiceServer(server, &snapshotService{
		scheduler: scheduler,
		service:   service,
	})
}
