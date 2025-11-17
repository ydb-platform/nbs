package filesystembackups

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type Service interface {
	CreateFilesystemBackup(
		ctx context.Context,
		req *disk_manager.CreateFilesystemBackupRequest,
	) (string, error)

	DeleteFilesystemBackup(
		ctx context.Context,
		req *disk_manager.DeleteFilesystemBackupRequest,
	) (string, error)
}
