package snapshots

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/golang/protobuf/proto"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type service struct {
	taskScheduler                  tasks.Scheduler
	taskStorage                    tasks_storage.Storage
	config                         *config.SnapshotsConfig
	createSnapshotStaggeringWindow time.Duration
}

func validateCreateSnapshotTask(
	state tasks_storage.TaskState,
	req *disk_manager.CreateSnapshotRequest,
) error {

	if state.TaskType != "snapshots.CreateSnapshotFromDisk" {
		return common.NewInvalidArgumentError(
			"idempotency key is already used by another operation",
		)
	}

	existing := &protos.CreateSnapshotFromDiskRequest{}
	if err := proto.Unmarshal(state.Request, existing); err != nil {
		return fmt.Errorf("decode snapshot task %q: %w", state.ID, err)
	}

	if existing.GetSrcDisk().GetZoneId() != req.GetSrc().GetZoneId() ||
		existing.GetSrcDisk().GetDiskId() != req.GetSrc().GetDiskId() ||
		existing.GetDstSnapshotId() != req.GetSnapshotId() ||
		existing.GetFolderId() != req.GetFolderId() {

		return common.NewInvalidArgumentError(
			"idempotency key is already used by a different snapshot request",
		)
	}

	return nil
}

func (s *service) findExistingCreateSnapshot(
	ctx context.Context,
	req *disk_manager.CreateSnapshotRequest,
) (string, bool, error) {

	state, err := s.taskStorage.GetTaskByIdempotencyKey(
		ctx,
		headers.GetIdempotencyKey(ctx),
		headers.GetAccountID(ctx),
	)
	if err != nil {
		if task_errors.Is(err, task_errors.NewEmptyNotFoundError()) {
			return "", false, nil
		}

		return "", false, err
	}

	if err := validateCreateSnapshotTask(state, req); err != nil {
		return "", false, err
	}

	return state.ID, true, nil
}

func (s *service) CreateSnapshot(
	ctx context.Context,
	req *disk_manager.CreateSnapshotRequest,
) (string, error) {

	// Capture once, before processing the request.
	receivedAt := time.Now()

	// Validate the request before accessing nested fields.
	if req == nil ||
		req.Src == nil ||
		len(req.Src.ZoneId) == 0 ||
		len(req.Src.DiskId) == 0 ||
		len(req.SnapshotId) == 0 {

		return "", common.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	idempotencyKey := headers.GetIdempotencyKey(ctx)
	if len(idempotencyKey) == 0 {
		return "", common.NewInvalidArgumentError(
			"idempotency-key header is required",
		)
	}

	if id, found, err := s.findExistingCreateSnapshot(ctx, req); err != nil {
		return "", err
	} else if found {
		return id, nil
	}

	rand.Seed(time.Now().UnixNano())
	useS3 := common.Find(s.config.GetUseS3ForFolder(), req.FolderId) ||
		rand.Uint32()%100 < s.config.GetUseS3Percentage()

	request := &protos.CreateSnapshotFromDiskRequest{
		SrcDisk: &types.Disk{
			ZoneId: req.Src.ZoneId,
			DiskId: req.Src.DiskId,
		},
		DstSnapshotId:                    req.SnapshotId,
		FolderId:                         req.FolderId,
		UseS3:                            useS3,
		UseProxyOverlayDisk:              s.config.GetUseProxyOverlayDisk(),
		RetryBrokenDRBasedDiskCheckpoint: s.config.GetRetryBrokenDRBasedDiskCheckpoint(),
	}

	window := s.createSnapshotStaggeringWindow
	var taskID string
	var scheduleErr error

	if window == 0 {
		taskID, scheduleErr = s.taskScheduler.ScheduleTask(
			ctx,
			"snapshots.CreateSnapshotFromDisk",
			"",
			request,
		)
	} else {
		offset := snapshotStartOffset(
			idempotencyKey,
			req.SnapshotId,
			req.Src.ZoneId,
			req.Src.DiskId,
			window,
		)

		taskID, scheduleErr = s.taskScheduler.ScheduleTaskAt(
			ctx,
			"snapshots.CreateSnapshotFromDisk",
			"",
			tasks.TaskScheduleTiming{
				ReceivedAt: receivedAt,
				NotBefore:  receivedAt.Add(offset),
			},
			request,
		)
	}

	if scheduleErr == nil {
		// The scheduler may return an existing task from legacy storage.
		// Validate the task identified by the returned operation ID.
		state, err := s.taskStorage.GetTask(ctx, taskID)
		if err != nil {
			return "", fmt.Errorf("read scheduled snapshot task %q: %w", taskID, err)
		}

		if err := validateCreateSnapshotTask(state, req); err != nil {
			return "", err
		}

		return taskID, nil
	}

	// Another request may have persisted the task after our first lookup.
	if id, found, err := s.findExistingCreateSnapshot(ctx, req); err != nil {
		return "", err
	} else if found {
		return id, nil
	}

	return "", scheduleErr
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

func parseCreateSnapshotStaggeringWindow(
	value string,
) (time.Duration, error) {

	window, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf(
			"invalid CreateSnapshotStaggeringWindow %q: %w",
			value,
			err,
		)
	}

	if window < 0 {
		return 0, fmt.Errorf(
			"invalid CreateSnapshotStaggeringWindow must not be negative: %q",
			value,
		)
	}

	return window, nil
}

func NewService(
	taskScheduler tasks.Scheduler,
	taskStorage tasks_storage.Storage,
	config *config.SnapshotsConfig,
) (Service, error) {

	window, err := parseCreateSnapshotStaggeringWindow(
		config.GetCreateSnapshotStaggeringWindow(),
	)
	if err != nil {
		return nil, err
	}

	return &service{
		taskScheduler:                  taskScheduler,
		taskStorage:                    taskStorage,
		config:                         config,
		createSnapshotStaggeringWindow: window,
	}, nil
}
