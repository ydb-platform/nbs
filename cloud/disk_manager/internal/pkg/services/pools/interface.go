package pools

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
)

////////////////////////////////////////////////////////////////////////////////

type Service interface {
	AcquireBaseDisk(
		ctx context.Context,
		req *protos.AcquireBaseDiskRequest,
	) (string, error)

	ReleaseBaseDisk(
		ctx context.Context,
		req *protos.ReleaseBaseDiskRequest,
	) (string, error)

	RebaseOverlayDisk(
		ctx context.Context,
		req *protos.RebaseOverlayDiskRequest,
	) (string, error)

	ConfigurePool(
		ctx context.Context,
		req *protos.ConfigurePoolRequest,
	) (string, error)

	DeletePool(
		ctx context.Context,
		req *protos.DeletePoolRequest,
	) (string, error)

	ImageDeleting(
		ctx context.Context,
		req *protos.ImageDeletingRequest,
	) (string, error)

	IsPoolConfigured(
		ctx context.Context,
		imageID string,
		zoneID string,
	) (bool, error)

	RetireBaseDisk(
		ctx context.Context,
		req *protos.RetireBaseDiskRequest,
	) (string, error)

	RetireBaseDisks(
		ctx context.Context,
		req *protos.RetireBaseDisksRequest,
	) (string, error)

	OptimizeBaseDisks(ctx context.Context) (string, error)
}
