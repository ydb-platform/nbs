package images

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	pools_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/mocks"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestDeleteImageTaskDisabledZones(t *testing.T) {
	poolZones := []string{"zone-d", "zone-a", "zone-d-shard1"}
	disabledZone := common.NewDisabledZones([]string{"zone-d"})
	allDisabled := common.NewDisabledZones(poolZones)
	testCases := []struct {
		name          string
		disabledZones *common.DisabledZones
		retireZones   []string
		cancel        bool
		markErr       error
	}{
		{name: "mixed zones keep enabled shard", disabledZones: disabledZone, retireZones: []string{"zone-a", "zone-d-shard1"}},
		{name: "disabled shard", disabledZones: common.NewDisabledZones([]string{"zone-d-shard1"}), retireZones: []string{"zone-d", "zone-a"}},
		{name: "all zones disabled", disabledZones: allDisabled},
		{name: "enabled zones", disabledZones: common.NewDisabledZones([]string{"other"}), retireZones: poolZones},
		{name: "missing policy", retireZones: poolZones},
		{name: "empty policy", disabledZones: common.NewDisabledZones([]string{}), retireZones: poolZones},
		{name: "cancel skips disabled zone", disabledZones: disabledZone, retireZones: []string{"zone-a", "zone-d-shard1"}, cancel: true},
		{name: "mark deleted error after skip", disabledZones: allDisabled, markErr: assert.AnError},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.WarnLevel))
			storage := storage_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			poolService := pools_mocks.NewServiceMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			execCtx.On("GetTaskID").Return("delete-image")
			imageConfig := &config.ImagesConfig{}
			for _, zoneID := range poolZones {
				imageConfig.DefaultDiskPoolConfigs = append(imageConfig.DefaultDiskPoolConfigs,
					&config.DiskPoolConfig{ZoneId: proto.String(zoneID)})
			}
			poolService.On("ImageDeleting",
				headers.SetIncomingIdempotencyKey(ctx, "delete-image_image_deleting"),
				&pools_protos.ImageDeletingRequest{ImageId: "image"},
			).Return("image-deleting", nil).Once()
			scheduler.On("WaitTask", ctx, execCtx, "image-deleting").Return(nil, nil).Once()
			storage.On("DeleteImage", ctx, "image", "delete-image", mock.Anything).
				Return(&resources.ImageMeta{ID: "image", Size: 100, DeleteTaskID: "delete-image"}, nil).Once()
			for _, zoneID := range tc.retireZones {
				scheduler.On("ScheduleTask",
					headers.SetIncomingIdempotencyKey(ctx, "retire_base_disks_delete-image:"+zoneID+":image"),
					"pools.RetireBaseDisks", "",
					&pools_protos.RetireBaseDisksRequest{
						ImageId: "image", ZoneId: zoneID, UseBaseDiskAsSrc: true, UseImageSize: 100,
					},
				).Return("retire-"+zoneID, nil).Once()
			}
			scheduler.On("ScheduleTask",
				headers.SetIncomingIdempotencyKey(ctx, "delete-image_delete"),
				"dataplane.DeleteSnapshot", "", &dataplane_protos.DeleteSnapshotRequest{SnapshotId: "image"},
			).Return("delete-snapshot", nil).Once()
			scheduler.On("WaitTask", ctx, execCtx, "delete-snapshot").Return(nil, nil).Once()
			storage.On("ImageDeleted", ctx, "image", mock.Anything).Return(tc.markErr).Once()
			task := &deleteImageTask{
				config: imageConfig, disabledZones: tc.disabledZones,
				storage: storage, scheduler: scheduler, poolService: poolService,
				request: &protos.DeleteImageRequest{ImageId: "image"},
				state:   &protos.DeleteImageTaskState{},
			}
			var err error
			if tc.cancel {
				err = task.Cancel(ctx, execCtx)
			} else {
				err = task.Run(ctx, execCtx)
			}
			if tc.markErr != nil {
				require.True(t, errors.Is(err, tc.markErr))
				retryError, ok := err.(*errors.RetriableError)
				require.True(t, ok)
				require.True(t, retryError.IgnoreRetryLimit)
			} else {
				require.NoError(t, err)
			}
			scheduler.AssertNotCalled(t, "WaitTaskEnded", mock.Anything, mock.Anything)
			for _, zoneID := range tc.retireZones {
				scheduler.AssertNotCalled(t, "WaitTask", mock.Anything, mock.Anything, "retire-"+zoneID)
			}
			mock.AssertExpectationsForObjects(t, storage, scheduler, poolService, execCtx)
		})
	}
}
