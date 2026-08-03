package filesystem_snapshot

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem_snapshot/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type service struct {
	taskScheduler tasks.Scheduler
}

func (s *service) CreateFilesystemSnapshot(
	ctx context.Context,
	req *disk_manager.CreateFilesystemSnapshotRequest,
) (string, error) {

	if req.Src == nil ||
		len(req.Src.ZoneId) == 0 ||
		len(req.Src.FilesystemId) == 0 ||
		len(req.FilesystemSnapshotId) == 0 {

		return "", common.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(
		ctx,
		"filesystem_snapshot.CreateFilesystemSnapshot",
		"",
		&protos.CreateFilesystemSnapshotRequest{
			SrcFilesystem: &types.Filesystem{
				ZoneId:       req.Src.ZoneId,
				FilesystemId: req.Src.FilesystemId,
			},
			DstSnapshotId: req.FilesystemSnapshotId,
			FolderId:      req.FolderId,
			NodeId:        req.NodeId,
		},
	)
}

func (s *service) DeleteFilesystemSnapshot(
	ctx context.Context,
	req *disk_manager.DeleteFilesystemSnapshotRequest,
) (string, error) {

	if len(req.FilesystemSnapshotId) == 0 {
		return "", common.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleNonCancellableTask(
		ctx,
		"filesystem_snapshot.DeleteFilesystemSnapshot",
		"", // description
		"", // zoneID
		&protos.DeleteFilesystemSnapshotRequest{
			SnapshotId: req.FilesystemSnapshotId,
		},
	)
}

////////////////////////////////////////////////////////////////////////////////

func NewService(
	taskScheduler tasks.Scheduler,
) Service {
	return &service{
		taskScheduler: taskScheduler,
	}
}
