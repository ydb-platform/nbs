package filesystem

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type Service interface {
	CreateFilesystem(
		ctx context.Context,
		req *disk_manager.CreateFilesystemRequest,
	) (string, error)

	DeleteFilesystem(
		ctx context.Context,
		req *disk_manager.DeleteFilesystemRequest,
	) (string, error)

	ResizeFilesystem(
		ctx context.Context,
		req *disk_manager.ResizeFilesystemRequest,
	) (string, error)

	DescribeFilesystemModel(
		ctx context.Context,
		req *disk_manager.DescribeFilesystemModelRequest,
	) (*disk_manager.FilesystemModel, error)
}
