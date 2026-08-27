package pools

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type optimizeBaseDisksTask struct {
	scheduler                               tasks.Scheduler
	storage                                 storage.Storage
	convertToImageSizedBaseDisksThreshold   uint64
	convertToDefaultSizedBaseDisksThreshold uint64
	minPoolAge                              time.Duration
	baseDiskIdleTTL                         time.Duration
	cleanupIdleBaseDisksLimit               uint64
	state                                   *protos.OptimizeBaseDisksTaskState
}

func (t *optimizeBaseDisksTask) ensureResourcesToOptimizeCollected(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	sizeOptimizationEnabled bool,
	idleCleanupEnabled bool,
) error {

	if t.state.CollectedResourcesToOptimize {
		return nil
	}

	poolInfos, err := t.storage.GetReadyPoolInfos(ctx)
	if err != nil {
		return err
	}

	now := time.Now()

	type poolKey struct {
		imageID string
		zoneID  string
	}

	optimizedPools := make(map[poolKey]bool)

	if sizeOptimizationEnabled {
		t1 := t.convertToDefaultSizedBaseDisksThreshold
		t2 := t.convertToImageSizedBaseDisksThreshold

		for _, poolInfo := range poolInfos {
			dateThreshold := poolInfo.CreatedAt.Add(t.minPoolAge)
			if now.Before(dateThreshold) {
				continue
			}

			useImageSize := poolInfo.ImageSize > 0
			newUseImageSize := useImageSize

			if useImageSize && poolInfo.AcquiredUnits > t1 {
				newUseImageSize = false
			} else if !useImageSize && poolInfo.AcquiredUnits < t2 {
				newUseImageSize = true
			}

			if useImageSize == newUseImageSize {
				continue
			}

			t.state.ConfigurePoolForImageSizeRequests = append(
				t.state.ConfigurePoolForImageSizeRequests,
				&protos.ConfigurePoolRequest{
					ZoneId:       poolInfo.ZoneID,
					ImageId:      poolInfo.ImageID,
					Capacity:     poolInfo.Capacity,
					UseImageSize: newUseImageSize,
				},
			)

			// Pools selected for size optimization will have all their base disks retired,
			// including idle ones, so there is no need to run idle cleanup for them.
			optimizedPools[poolKey{poolInfo.ImageID, poolInfo.ZoneID}] = true
		}
	}

	if idleCleanupEnabled {
		for _, poolInfo := range poolInfos {
			key := poolKey{poolInfo.ImageID, poolInfo.ZoneID}
			if optimizedPools[key] {
				continue
			}

			idleDisks, err := t.storage.GetIdleBaseDisks(
				ctx,
				poolInfo.ImageID,
				poolInfo.ZoneID,
				t.baseDiskIdleTTL,
				t.cleanupIdleBaseDisksLimit,
			)
			if err != nil {
				return err
			}

			if len(idleDisks) == 0 {
				continue
			}

			var reduction uint32
			for _, disk := range idleDisks {
				reduction += uint32(disk.FreeSlots)
				t.state.IdleBaseDiskIds = append(
					t.state.IdleBaseDiskIds,
					disk.ID,
				)
			}

			// TODO(https://github.com/ydb-platform/nbs/issues/6684):
			// setting capacity to 0 may cause a race. If a slot is acquired
			// on an idle disk between ConfigurePool and RetireBaseDisk, the
			// retire triggers a rebase that creates a new base disk with srcDisk=nil.
			// The scheduler (takeBaseDisksToSchedule) never picks it up: no SrcDisk
			// for the global path, and capacity=0 excludes the pool from the per-pool
			// path. The rebase task retries indefinitely, hanging the optimization.
			newCapacity := uint32(0)
			if poolInfo.Capacity > reduction {
				newCapacity = poolInfo.Capacity - reduction
			}

			t.state.PoolReductionRequests = append(
				t.state.PoolReductionRequests,
				&protos.ConfigurePoolRequest{
					ZoneId:       key.zoneID,
					ImageId:      key.imageID,
					Capacity:     newCapacity,
					UseImageSize: poolInfo.ImageSize > 0,
				},
			)
		}
	}

	t.state.CollectedResourcesToOptimize = true
	return execCtx.SaveState(ctx)
}

func (t *optimizeBaseDisksTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *optimizeBaseDisksTask) Load(_, state []byte) error {
	t.state = &protos.OptimizeBaseDisksTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *optimizeBaseDisksTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	t1 := t.convertToDefaultSizedBaseDisksThreshold
	t2 := t.convertToImageSizedBaseDisksThreshold

	sizeOptimizationEnabled := t1 > 0 || t2 > 0
	if sizeOptimizationEnabled {
		common.Assert(t1 > t2, `ConvertToDefaultSizedBaseDisksThreshold should be greater
			than convertToImageSizedBaseDisksThreshold`)
	}

	idleCleanupEnabled := t.baseDiskIdleTTL > 0

	if !sizeOptimizationEnabled && !idleCleanupEnabled {
		return nil
	}

	err := t.ensureResourcesToOptimizeCollected(
		ctx,
		execCtx,
		sizeOptimizationEnabled,
		idleCleanupEnabled,
	)
	if err != nil {
		return err
	}

	// Execute optimization: ConfigurePool + RetireBaseDisks for each pool.
	var configurePoolTaskIDs []string

	for _, request := range t.state.ConfigurePoolForImageSizeRequests {
		taskID, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(
				ctx,
				"configure_pool_"+execCtx.GetTaskID()+
					":"+request.GetZoneId()+":"+request.GetImageId(),
			),
			"pools.ConfigurePool",
			"",
			request,
		)
		if err != nil {
			return err
		}

		configurePoolTaskIDs = append(configurePoolTaskIDs, taskID)
	}

	for i, taskID := range configurePoolTaskIDs {
		_, err := t.scheduler.WaitTask(ctx, execCtx, taskID)
		if err != nil {
			if !errors.CanRetry(err) {
				// Ignore non-retriable (fatal) error, because image might be already
				// deleted.
				continue
			}

			return err
		}

		request := t.state.ConfigurePoolForImageSizeRequests[i]

		taskID, err = t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(
				ctx,
				"retire_base_disks_"+execCtx.GetTaskID()+
					":"+request.GetZoneId()+":"+request.GetImageId(),
			),
			"pools.RetireBaseDisks",
			"",
			&protos.RetireBaseDisksRequest{
				ZoneId:           request.GetZoneId(),
				ImageId:          request.GetImageId(),
				UseBaseDiskAsSrc: true,
			},
		)
		if err != nil {
			return err
		}

		_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
		if err != nil {
			return err
		}
	}

	// Execute idle cleanup: reduce pool capacities, then retire idle base disks.
	for _, request := range t.state.PoolReductionRequests {
		taskID, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(
				ctx,
				"idle_configure_pool_"+execCtx.GetTaskID()+
					":"+request.GetZoneId()+":"+request.GetImageId(),
			),
			"pools.ConfigurePool",
			"",
			request,
		)
		if err != nil {
			return err
		}

		_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
		if err != nil {
			if !errors.CanRetry(err) {
				// Ignore non-retriable (fatal) error, because image might be already
				// deleted.
				continue
			}

			return err
		}
	}

	for _, baseDiskID := range t.state.IdleBaseDiskIds {
		taskID, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(
				ctx,
				"retire_idle_base_disk_"+execCtx.GetTaskID()+
					":"+baseDiskID,
			),
			"pools.RetireBaseDisk",
			"",
			&protos.RetireBaseDiskRequest{
				BaseDiskId: baseDiskID,
			},
		)
		if err != nil {
			return err
		}

		_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *optimizeBaseDisksTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *optimizeBaseDisksTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *optimizeBaseDisksTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
