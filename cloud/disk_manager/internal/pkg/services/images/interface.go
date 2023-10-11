package images

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type Service interface {
	CreateImage(
		ctx context.Context,
		req *disk_manager.CreateImageRequest,
	) (string, error)

	DeleteImage(
		ctx context.Context,
		req *disk_manager.DeleteImageRequest,
	) (string, error)
}
