package snapshots

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
	config        *config.SnapshotsConfig
}

func (s *service) CreateSnapshot(
	ctx context.Context,
	req *disk_manager.CreateSnapshotRequest,
) (string, error) {

	if len(req.Src.ZoneId) == 0 ||
		len(req.Src.DiskId) == 0 ||
		len(req.SnapshotId) == 0 {

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
		"snapshots.CreateSnapshotFromDisk",
		"",
		&protos.CreateSnapshotFromDiskRequest{
			SrcDisk: &types.Disk{
				ZoneId: req.Src.ZoneId,
				DiskId: req.Src.DiskId,
			},
			DstSnapshotId:                    req.SnapshotId,
			FolderId:                         req.FolderId,
			UseS3:                            useS3,
			UseProxyOverlayDisk:              s.config.GetUseProxyOverlayDisk(),
			RetryBrokenDRBasedDiskCheckpoint: s.config.GetRetryBrokenDRBasedDiskCheckpoint(),
		},
	)
}

func (s *service) DeleteSnapshot(
	ctx context.Context,
	req *disk_manager.DeleteSnapshotRequest,
) (string, error) {

	if len(req.SnapshotId) == 0 {
		return "", common.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(
		ctx,
		"snapshots.DeleteSnapshot",
		"",
		&protos.DeleteSnapshotRequest{
			SnapshotId: req.SnapshotId,
		},
	)
}

////////////////////////////////////////////////////////////////////////////////

func NewService(
	taskScheduler tasks.Scheduler,
	config *config.SnapshotsConfig,
) Service {

	return &service{
		taskScheduler: taskScheduler,
		config:        config,
	}
}
