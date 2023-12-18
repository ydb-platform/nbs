package images

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/api/yandex/cloud/priv/disk_manager/v1"
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
