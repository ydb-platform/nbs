package snapshots

import (
	"context"
	"math/rand"
	"time"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
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

	chunkSize := req.ChunkSize
	if chunkSize == 0 {
		if useS3 {
			chunkSize = s.config.GetChunkSize()
		}
	} else if !common.Find(s.config.GetChunkSizeOverrideAllowedForFolder(), req.FolderId) {
		return "", grpc_status.Errorf(
			grpc_codes.InvalidArgument,
			"chunk size override is not allowed for folder %q",
			req.FolderId,
		)
	}

	if chunkSize == 0 {
		chunkSize = dataplane_common.DefaultChunkSize
	}
	if err := dataplane_common.ValidateSnapshotChunkSize(chunkSize, useS3); err != nil {
		return "", grpc_status.Errorf(grpc_codes.InvalidArgument, "%v", err)
	}

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
			ChunkSize:                        chunkSize,
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

	return s.taskScheduler.ScheduleNonCancellableTask(
		ctx,
		"snapshots.DeleteSnapshot",
		"", // description
		"", // zoneID
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
