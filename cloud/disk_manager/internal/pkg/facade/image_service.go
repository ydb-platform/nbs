package facade

import (
	"context"
	"errors"

	"github.com/ydb-platform/nbs/cloud/api/operation"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

type imageService struct {
	scheduler tasks.Scheduler
	service   images.Service
}

func (s *imageService) Create(
	ctx context.Context,
	req *disk_manager.CreateImageRequest,
) (*operation.Operation, error) {

	taskID, err := s.service.CreateImage(ctx, req)
	if err != nil {
		return nil, err
	}

	return s.scheduler.GetOperation(ctx, taskID)
}

func (s *imageService) Update(
	ctx context.Context,
	req *disk_manager.UpdateImageRequest,
) (*operation.Operation, error) {

	return nil, errors.New("not implemented")
}

func (s *imageService) Delete(
	ctx context.Context,
	req *disk_manager.DeleteImageRequest,
) (*operation.Operation, error) {

	taskID, err := s.service.DeleteImage(ctx, req)
	if err != nil {
		return nil, err
	}

	return s.scheduler.GetOperation(ctx, taskID)
}

////////////////////////////////////////////////////////////////////////////////

func RegisterImageService(
	server *grpc.Server,
	scheduler tasks.Scheduler,
	service images.Service,
) {

	disk_manager.RegisterImageServiceServer(server, &imageService{
		scheduler: scheduler,
		service:   service,
	})
}
