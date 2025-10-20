package filesystembackups

import (
	"context"
	"math/rand"
	"time"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type service struct {
	taskScheduler tasks.Scheduler
	config        *config.FilesystemBackupsConfig
}

func (s *service) CreateFilesystemBackup(
	ctx context.Context,
	req *disk_manager.CreateFilesystemBackupRequest,
) (string, error) {

	if len(req.Src.ZoneId) == 0 ||
		len(req.Src.FilesystemId) == 0 ||
		len(req.FilesystemBackupId) == 0 {

		return "", common.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	rand.Seed(time.Now().UnixNano())
	useS3 := common.Find(s.config.GetUseS3ForFolder(), req.FolderId) ||
		rand.Uint32()%100 < s.config.GetUseS3Percentage()

	return s.taskScheduler.ScheduleTask(
		ctx,
		"snapshots.CreateFilesystemBackupFromDisk",
		"",
		&protos.CreateFilesystemBackupFromDiskRequest{
			SrcDisk: &types.Disk{
				ZoneId: req.Src.ZoneId,
				DiskId: req.Src.FilesystemId,
			},
			DstFilesystemBackupId:            req.FilesystemBackupId,
			FolderId:                         req.FolderId,
			UseS3:                            useS3,
			UseProxyOverlayDisk:              s.config.GetUseProxyOverlayDisk(),
			RetryBrokenDRBasedDiskCheckpoint: s.config.GetRetryBrokenDRBasedDiskCheckpoint(),
		},
	)
}

func (s *service) DeleteFilesystemBackup(
	ctx context.Context,
	req *disk_manager.DeleteFilesystemBackupRequest,
) (string, error) {

	if len(req.FilesystemBackupId) == 0 {
		return "", common.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(
		ctx,
		"filesystembackups.DeleteFilesystemBackup",
		"",
		&protos.DeleteFilesystemBackupRequest{
			FilesystemBackupId: req.FilesystemBackupId,
		},
	)
}

////////////////////////////////////////////////////////////////////////////////

func NewService(
	taskScheduler tasks.Scheduler,
	config *config.FilesystemBackupsConfig,
) Service {

	return &service{
		taskScheduler: taskScheduler,
		config:        config,
	}
}
