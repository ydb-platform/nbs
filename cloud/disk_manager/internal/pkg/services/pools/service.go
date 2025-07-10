package pools

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type service struct {
	taskScheduler tasks.Scheduler
	storage       storage.Storage
}

func (s *service) AcquireBaseDisk(
	ctx context.Context,
	req *protos.AcquireBaseDiskRequest,
) (string, error) {

	if len(req.SrcImageId) == 0 ||
		len(req.OverlayDisk.ZoneId) == 0 ||
		len(req.OverlayDisk.DiskId) == 0 {

		return "", common.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(ctx, "pools.AcquireBaseDisk", "", req)
}

func (s *service) ReleaseBaseDisk(
	ctx context.Context,
	req *protos.ReleaseBaseDiskRequest,
) (string, error) {

	if len(req.OverlayDisk.ZoneId) == 0 ||
		len(req.OverlayDisk.DiskId) == 0 {

		return "", common.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(ctx, "pools.ReleaseBaseDisk", "", req)
}

func (s *service) RebaseOverlayDisk(
	ctx context.Context,
	req *protos.RebaseOverlayDiskRequest,
) (string, error) {

	if len(req.OverlayDisk.ZoneId) == 0 ||
		len(req.OverlayDisk.DiskId) == 0 ||
		len(req.TargetBaseDiskId) == 0 {

		return "", common.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(ctx, "pools.RebaseOverlayDisk", "", req)
}

func (s *service) ConfigurePool(
	ctx context.Context,
	req *protos.ConfigurePoolRequest,
) (string, error) {

	if len(req.ZoneId) == 0 ||
		len(req.ImageId) == 0 {

		return "", common.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(ctx, "pools.ConfigurePool", "", req)
}

func (s *service) DeletePool(
	ctx context.Context,
	req *protos.DeletePoolRequest,
) (string, error) {

	if len(req.ImageId) == 0 || len(req.ZoneId) == 0 {
		return "", common.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(ctx, "pools.DeletePool", "", req)
}

func (s *service) ImageDeleting(
	ctx context.Context,
	req *protos.ImageDeletingRequest,
) (string, error) {

	if len(req.ImageId) == 0 {
		return "", common.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(ctx, "pools.ImageDeleting", "", req)
}

func (s *service) IsPoolConfigured(
	ctx context.Context,
	imageID string,
	zoneID string,
) (bool, error) {

	return s.storage.IsPoolConfigured(ctx, imageID, zoneID)
}

func (s *service) RetireBaseDisk(
	ctx context.Context,
	req *protos.RetireBaseDiskRequest,
) (string, error) {

	if len(req.BaseDiskId) == 0 {
		return "", common.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(ctx, "pools.RetireBaseDisk", "", req)
}

func (s *service) RetireBaseDisks(
	ctx context.Context,
	req *protos.RetireBaseDisksRequest,
) (string, error) {

	if len(req.ImageId) == 0 || len(req.ZoneId) == 0 {
		return "", common.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(ctx, "pools.RetireBaseDisks", "", req)
}

func (s *service) OptimizeBaseDisks(ctx context.Context) (string, error) {
	return s.taskScheduler.ScheduleTask(
		ctx,
		"pools.OptimizeBaseDisks",
		"",
		&empty.Empty{},
	)
}

////////////////////////////////////////////////////////////////////////////////

func NewService(
	taskScheduler tasks.Scheduler,
	storage storage.Storage,
) Service {

	return &service{
		taskScheduler: taskScheduler,
		storage:       storage,
	}
}
