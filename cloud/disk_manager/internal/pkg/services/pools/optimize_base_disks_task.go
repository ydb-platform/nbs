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
	state                                   *protos.OptimizeBaseDisksTaskState
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
	common.Assert(t1 > t2, "There should be gap between tresholds")

	if t1 == 0 && t2 == 0 {
		return nil
	}

	if len(t.state.ConfigurePoolRequests) == 0 {
		poolInfos, err := t.storage.GetReadyPoolInfos(ctx)
		if err != nil {
			return err
		}

		now := time.Now()

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

			t.state.ConfigurePoolRequests = append(
				t.state.ConfigurePoolRequests,
				&protos.ConfigurePoolRequest{
					ZoneId:       poolInfo.ZoneID,
					ImageId:      poolInfo.ImageID,
					Capacity:     poolInfo.Capacity,
					UseImageSize: newUseImageSize,
				},
			)
		}

		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	var configurePoolTaskIDs []string

	for _, request := range t.state.ConfigurePoolRequests {
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

		request := t.state.ConfigurePoolRequests[i]

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
