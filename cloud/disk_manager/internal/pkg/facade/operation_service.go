package facade

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/operation"
	"google.golang.org/grpc"
	grpc_status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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
		status := grpc_status.FromProto(result.Error)
		dmStatus := grpc_status.New(status.Code(), status.Message())

		for _, errorDetail := range status.Details() {
			protoMessage, err := proto.Marshal(errorDetail.(proto.Message))
			if err != nil {
				logging.Warn(ctx, "failed to marshal error detail: %v", err)
				continue
			}

			var dmErrorDetail disk_manager.ErrorDetails
			err = proto.Unmarshal(protoMessage, &dmErrorDetail)
			if err != nil {
				logging.Warn(ctx, "failed to unmarshal error detail: %v", err)
				continue
			}

			dmStatusWithDetails, err := dmStatus.WithDetails(&dmErrorDetail)
			if err == nil {
				dmStatus = dmStatusWithDetails
			} else {
				logging.Warn(ctx, "failed to attach error details: %v", err)
				continue
			}
		}

		dmOp.Result = &disk_manager.Operation_Error{
			Error: dmStatus.Proto(),
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
