package facade

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/api/operation"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/tasks"
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

	return s.taskScheduler.GetOperation(ctx, req.OperationId)
}

func (s *operationService) Cancel(
	ctx context.Context,
	req *disk_manager.CancelOperationRequest,
) (*operation.Operation, error) {

	_, err := s.taskScheduler.CancelTask(ctx, req.OperationId)
	if err != nil {
		return nil, err
	}

	return s.taskScheduler.GetOperation(ctx, req.OperationId)
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
