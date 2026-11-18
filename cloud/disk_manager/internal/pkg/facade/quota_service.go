package facade

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

type quotaService struct {
}

func (s *quotaService) Create(
	ctx context.Context,
	req *disk_manager.CreateQuotaRequest,
) (*disk_manager.Operation, error) {

	return nil, status.Error(
		codes.Unimplemented,
		"QuotaService.Create is not implemented yet",
	)
}

func (s *quotaService) Delete(
	ctx context.Context,
	req *disk_manager.DeleteQuotaRequest,
) (*disk_manager.Operation, error) {

	return nil, status.Error(
		codes.Unimplemented,
		"QuotaService.Delete is not implemented yet",
	)
}

func (s *quotaService) Update(
	ctx context.Context,
	req *disk_manager.UpdateQuotaRequest,
) (*disk_manager.Operation, error) {

	return nil, status.Error(
		codes.Unimplemented,
		"QuotaService.Update is not implemented yet",
	)
}

func (s *quotaService) Attach(
	ctx context.Context,
	req *disk_manager.AttachQuotaRequest,
) (*disk_manager.Operation, error) {

	return nil, status.Error(
		codes.Unimplemented,
		"QuotaService.Attach is not implemented yet",
	)
}

func (s *quotaService) List(
	ctx context.Context,
	req *disk_manager.ListQuotasRequest,
) (*disk_manager.ListQuotasResponse, error) {

	return nil, status.Error(
		codes.Unimplemented,
		"QuotaService.List is not implemented yet",
	)
}

////////////////////////////////////////////////////////////////////////////////

func RegisterQuotaService(server *grpc.Server) {
	disk_manager.RegisterQuotaServiceServer(server, &quotaService{})
}
