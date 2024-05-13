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

func toDiskManagerOperationError(
	ctx context.Context,
	result *operation.Operation_Error,
) *disk_manager.Operation_Error {

	status := grpc_status.FromProto(result.Error)
	dmStatus := grpc_status.New(status.Code(), status.Message())

	defaultDmOperationError := &disk_manager.Operation_Error{
		Error: dmStatus.Proto(),
	}

	if len(status.Details()) == 0 {
		return defaultDmOperationError
	}

	// We expect the only one error detail.
	errorDetail := status.Details()[0]

	// Convert to disk_manager.ErrorDetails via marshaling/unmarshaling in
	// order to be compliant with disk_manager/api.
	protoMessage, err := proto.Marshal(errorDetail.(proto.Message))
	if err != nil {
		logging.Warn(ctx, "failed to marshal error details: %v", err)
		return defaultDmOperationError
	}

	var dmErrorDetails disk_manager.ErrorDetails
	err = proto.Unmarshal(protoMessage, &dmErrorDetails)
	if err != nil {
		logging.Warn(ctx, "failed to unmarshal error details: %v", err)
		return defaultDmOperationError
	}

	dmStatusWithDetails, err := dmStatus.WithDetails(&dmErrorDetails)
	if err != nil {
		logging.Warn(ctx, "failed to attach error details: %v", err)
		return defaultDmOperationError
	}

	return &disk_manager.Operation_Error{
		Error: dmStatusWithDetails.Proto(),
	}
}

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
		dmOp.Result = toDiskManagerOperationError(ctx, result)
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
