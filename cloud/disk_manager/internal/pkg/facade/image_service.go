package facade

import (
	"context"
	"errors"

	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

type imageService struct {
	taskScheduler tasks.Scheduler
	service       images.Service
}

func (s *imageService) Create(
	ctx context.Context,
	req *disk_manager.CreateImageRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.CreateImage(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

func (s *imageService) Update(
	ctx context.Context,
	req *disk_manager.UpdateImageRequest,
) (*disk_manager.Operation, error) {

	return nil, errors.New("not implemented")
}

func (s *imageService) Delete(
	ctx context.Context,
	req *disk_manager.DeleteImageRequest,
) (*disk_manager.Operation, error) {

	taskID, err := s.service.DeleteImage(ctx, req)
	if err != nil {
		return nil, err
	}

	return getOperation(ctx, s.taskScheduler, taskID)
}

////////////////////////////////////////////////////////////////////////////////

func RegisterImageService(
	server *grpc.Server,
	taskScheduler tasks.Scheduler,
	service images.Service,
) {

	disk_manager.RegisterImageServiceServer(server, &imageService{
		taskScheduler: taskScheduler,
		service:       service,
	})
}
