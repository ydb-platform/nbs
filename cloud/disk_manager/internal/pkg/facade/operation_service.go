package facade

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/operation"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

func getOperation(
	ctx context.Context,
	taskScheduler tasks.Scheduler,
	operationID string,
) (*disk_manager.Operation, error) {

	o, err := taskScheduler.GetOperation(ctx, operationID)
	if err != nil {
		return nil, err
	}

	dmOp := &disk_manager.Operation{
		Id:          o.Id,
		Description: o.Description,
		CreatedAt:   o.CreatedAt,
		CreatedBy:   o.CreatedBy,
		ModifiedAt:  o.ModifiedAt,
		Done:        o.Done,
		Metadata:    o.Metadata,
	}

	switch result := o.Result.(type) {
	case *operation.Operation_Error:
		dmOp.Result = &disk_manager.Operation_Error{
			Error: result.Error,
		}
	case *operation.Operation_Response:
		dmOp.Result = &disk_manager.Operation_Response{
			Response: result.Response,
		}
	}

	return dmOp, nil
}

////////////////////////////////////////////////////////////////////////////////

type operationService struct {
	taskScheduler tasks.Scheduler
}

func (s *operationService) Get(
	ctx context.Context,
	req *disk_manager.GetOperationRequest,
) (*disk_manager.Operation, error) {

	return getOperation(ctx, s.taskScheduler, req.OperationId)
}

func (s *operationService) Cancel(
	ctx context.Context,
	req *disk_manager.CancelOperationRequest,
) (*disk_manager.Operation, error) {

	_, err := s.taskScheduler.CancelTask(ctx, req.OperationId)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, req.OperationId)
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
