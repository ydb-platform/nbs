package facade

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api/operation"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

type operationService struct {
	taskScheduler tasks.Scheduler
}

func (s *operationService) Get(
	ctx context.Context,
	req *disk_manager.GetOperationRequest,
) (*operation.Operation, error) {

	return s.taskScheduler.GetOperationProto(ctx, req.OperationId)
}

func (s *operationService) Cancel(
	ctx context.Context,
	req *disk_manager.CancelOperationRequest,
) (*operation.Operation, error) {

	_, err := s.taskScheduler.CancelTask(ctx, req.OperationId)
	if err != nil {
		return nil, err
	}

	return s.taskScheduler.GetOperationProto(ctx, req.OperationId)
}

////////////////////////////////////////////////////////////////////////////////

func RegisterOperationService(
	server *grpc.Server,
	taskScheduler tasks.Scheduler,
) {

	disk_manager.RegisterOperationServiceServer(server, &operationService{
		taskScheduler: taskScheduler,
	})
}
