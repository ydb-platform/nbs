package disks

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	cells_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	pools_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/mocks"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestDeleteDiskTaskDisabledZones(t *testing.T) {
	disabledZone := common.NewDisabledZones([]string{"zone-d"})
	disabledShard := common.NewDisabledZones([]string{"zone-d-shard1"})
	disabledDisk := &resources.DiskMeta{ZoneID: "zone-d"}
	allowedDisk := &resources.DiskMeta{ZoneID: "zone-a"}
	shardDisk := &resources.DiskMeta{ZoneID: "zone-d-shard1"}
	overlayDisk := &resources.DiskMeta{ZoneID: "zone-d", SrcImageID: "image"}

	testCases := []struct {
		name             string
		diskMeta         *resources.DiskMeta
		deletedZone      string
		zoneContainsCell bool
		nonCancellable   bool
		requestZone      string
		disabledZones    *common.DisabledZones
		sync             bool
		cancel           bool
		expectedZone     string
		expectNBS        bool
		errorAt          string
	}{
		{
			name: "disabled zone", diskMeta: disabledDisk, requestZone: "zone-d",
			disabledZones: disabledZone, expectedZone: "zone-d",
		},
		{
			name: "disabled zone with sync estimate", diskMeta: disabledDisk, requestZone: "zone-d",
			disabledZones: disabledZone, sync: true, expectedZone: "zone-d",
		},
		{
			name: "disabled shard", diskMeta: shardDisk, requestZone: "zone-d-shard1",
			disabledZones: disabledShard, expectedZone: "zone-d-shard1",
		},
		{
			name: "region does not disable shard", diskMeta: shardDisk, requestZone: "zone-d-shard1",
			disabledZones: disabledZone, expectedZone: "zone-d-shard1", expectNBS: true,
		},
		{
			name: "shard does not disable region", diskMeta: disabledDisk, requestZone: "zone-d",
			disabledZones: disabledShard, expectedZone: "zone-d", expectNBS: true,
		},
		{
			name: "disabled metadata zone overrides request", diskMeta: disabledDisk,
			requestZone: "zone-a", disabledZones: disabledZone, expectedZone: "zone-d",
		},
		{
			name: "disabled request zone conflicts with enabled metadata zone", diskMeta: allowedDisk,
			requestZone: "zone-d", disabledZones: disabledZone, nonCancellable: true,
		},
		{
			name: "enabled metadata cell belongs to request zone", diskMeta: allowedDisk,
			requestZone: "region", zoneContainsCell: true,
			disabledZones: disabledZone, expectedZone: "zone-a", expectNBS: true,
		},
		{
			name: "enabled metadata zone conflicts with request zone before sync estimate", diskMeta: allowedDisk,
			requestZone: "zone-b", disabledZones: disabledZone, sync: true, nonCancellable: true,
		},
		{
			name: "missing policy", diskMeta: disabledDisk, requestZone: "zone-d",
			expectedZone: "zone-d", expectNBS: true,
		},
		{
			name: "empty policy", diskMeta: disabledDisk, requestZone: "zone-d",
			disabledZones: common.NewDisabledZones(nil), expectedZone: "zone-d", expectNBS: true,
		},
		{
			name: "allowed sync estimate", diskMeta: allowedDisk, requestZone: "zone-a",
			disabledZones: disabledZone, sync: true, expectedZone: "zone-a", expectNBS: true,
		},
		{
			name: "cancel in disabled zone", diskMeta: disabledDisk, requestZone: "zone-d",
			disabledZones: disabledZone, cancel: true, expectedZone: "zone-d",
		},
		{
			name: "release overlay in disabled zone", diskMeta: overlayDisk, requestZone: "zone-d",
			disabledZones: disabledZone, expectedZone: "zone-d",
		},
		{
			name: "missing disk", requestZone: "zone-a", disabledZones: disabledZone,
		},
		{
			name:        "missing disk with sync and disabled request zone",
			requestZone: "zone-d", disabledZones: disabledZone, sync: true,
		},
		{
			name: "deleted disk zone matches request", deletedZone: "zone-a",
			requestZone: "zone-a", disabledZones: disabledZone,
		},
		{
			name: "deleted disk cell belongs to request zone", deletedZone: "zone-a",
			requestZone: "region", zoneContainsCell: true, disabledZones: disabledZone,
		},
		{
			name: "deleted disk zone conflicts with request", deletedZone: "zone-a",
			requestZone: "zone-b", disabledZones: disabledZone, nonCancellable: true,
		},
		{
			name:        "disabled deleted disk zone is handled before checking the request",
			deletedZone: "zone-d", requestZone: "zone-a", disabledZones: disabledZone,
		},
		{
			name: "legacy scheduled task without a request zone", diskMeta: allowedDisk,
			disabledZones: disabledZone, expectedZone: "zone-a", expectNBS: true,
		},
		{
			name: "metadata deletion error in disabled zone", diskMeta: disabledDisk, requestZone: "zone-d",
			disabledZones: disabledZone, errorAt: "DeleteDisk",
		},
		{
			name: "incremental scheduling error in disabled zone", diskMeta: disabledDisk, requestZone: "zone-d",
			disabledZones: disabledZone, expectedZone: "zone-d", errorAt: "ScheduleTask",
		},
		{
			name: "incremental cleanup error in disabled zone", diskMeta: disabledDisk, requestZone: "zone-d",
			disabledZones: disabledZone, expectedZone: "zone-d", errorAt: "WaitIncremental",
		},
		{
			name: "overlay release scheduling error in disabled zone", diskMeta: overlayDisk, requestZone: "zone-d",
			disabledZones: disabledZone, expectedZone: "zone-d", errorAt: "ReleaseBaseDisk",
		},
		{
			name: "overlay release error in disabled zone", diskMeta: overlayDisk, requestZone: "zone-d",
			disabledZones: disabledZone, expectedZone: "zone-d", errorAt: "WaitRelease",
		},
		{
			name: "mark deleted error in disabled zone", diskMeta: disabledDisk, requestZone: "zone-d",
			disabledZones: disabledZone, expectedZone: "zone-d", errorAt: "DiskDeleted",
		},
		{
			name: "nbs client error in allowed zone", diskMeta: allowedDisk, requestZone: "zone-a",
			disabledZones: disabledZone, expectedZone: "zone-a", expectNBS: true, errorAt: "GetClient",
		},
		{
			name: "nbs deletion error in allowed zone", diskMeta: allowedDisk, requestZone: "zone-a",
			disabledZones: disabledZone, expectedZone: "zone-a", expectNBS: true, errorAt: "Delete",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := logging.SetLogger(
				context.Background(),
				logging.NewStderrLogger(logging.WarnLevel),
			)
			storage := storage_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			poolService := pools_mocks.NewServiceMock()
			nbsFactory := nbs_mocks.NewFactoryMock()
			nbsClient := nbs_mocks.NewClientMock()
			cellSelector := cells_mocks.NewCellSelectorMock()
			execCtx := newExecutionContextMock()
			task := &deleteDiskTask{
				performanceConfig: &performance_config.PerformanceConfig{
					SSDLocalDiskDeletingBandwidthMiBs: proto.Uint64(100),
				},
				storage: storage, scheduler: scheduler, poolService: poolService,
				nbsFactory: nbsFactory, cellSelector: cellSelector,
				disabledZones: tc.disabledZones,
				request: &protos.DeleteDiskRequest{
					Disk: &types.Disk{DiskId: "disk", ZoneId: tc.requestZone},
					Sync: tc.sync,
				},
				state: &protos.DeleteDiskTaskState{},
			}

			// Configure the expected steps until the injected failure.
			errorFor := func(step string) error {
				if tc.errorAt == step {
					return assert.AnError
				}
				return nil
			}
			setupExpectations := func() {
				recordedZone := tc.deletedZone
				if tc.diskMeta != nil {
					recordedZone = tc.diskMeta.ZoneID
				}
				if recordedZone != "" && !tc.disabledZones.IsDisabled(recordedZone) &&
					tc.requestZone != "" && tc.requestZone != recordedZone && tc.errorAt != "DeleteDisk" {
					cellSelector.On("ZoneContainsCell", tc.requestZone, recordedZone).
						Return(tc.zoneContainsCell).Once()
				}

				deleteErr := errorFor("DeleteDisk")
				resultMeta := tc.diskMeta
				if tc.nonCancellable {
					resultMeta = nil
					deleteErr = errors.NewNonCancellableErrorf("zone mismatch")
				}
				storage.On("DeleteDiskWithZoneCheck", ctx, "disk", "toplevel_task_id", mock.Anything, mock.Anything).
					Run(func(args mock.Arguments) {
						if tc.errorAt == "DeleteDisk" || recordedZone == "" {
							return
						}
						checkZone := args.Get(4).(func(string) error)
						err := checkZone(recordedZone)
						if tc.nonCancellable {
							require.ErrorIs(t, err, errors.NewEmptyNonCancellableError())
						} else {
							require.NoError(t, err)
						}
					}).Return(resultMeta, deleteErr).Once()
				if deleteErr != nil || resultMeta == nil {
					return
				}
				if tc.sync && tc.expectNBS {
					nbsFactory.On("GetClient", ctx, tc.expectedZone).Return(nbsClient, nil).Once()
					nbsClient.On("Describe", ctx, "disk").Return(nbs.DiskParams{
						Kind: types.DiskKind_DISK_KIND_SSD_LOCAL, BlocksCount: 100 * 256, BlockSize: 4096,
					}, nil).Once()
					execCtx.On("SetEstimatedInflightDuration", time.Second).Once()
				}
				scheduler.On(
					"ScheduleTask",
					headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_delete_disk_from_incremental"),
					"dataplane.DeleteDiskFromIncremental", "",
					&dataplane_protos.DeleteDiskFromIncrementalRequest{
						Disk: &types.Disk{ZoneId: tc.expectedZone, DiskId: "disk"},
					},
				).Return("incremental", errorFor("ScheduleTask")).Once()
				if tc.errorAt == "ScheduleTask" {
					return
				}
				scheduler.On("WaitTask", ctx, execCtx, "incremental").
					Return(nil, errorFor("WaitIncremental")).Once()
				if tc.errorAt == "WaitIncremental" {
					return
				}
				if tc.expectNBS {
					nbsFactory.On("GetClient", ctx, tc.expectedZone).
						Return(nbsClient, errorFor("GetClient")).Once()
					if tc.errorAt == "GetClient" {
						return
					}
					method := "Delete"
					if tc.sync {
						method = "DeleteSync"
					}
					nbsClient.On(method, ctx, "disk").Return(errorFor("Delete")).Once()
					if tc.errorAt == "Delete" {
						return
					}
				}
				if tc.diskMeta != nil && len(tc.diskMeta.SrcImageID) != 0 {
					poolService.On(
						"ReleaseBaseDisk",
						headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_release_base_disk"),
						&pools_protos.ReleaseBaseDiskRequest{
							OverlayDisk: &types.Disk{ZoneId: tc.expectedZone, DiskId: "disk"},
						},
					).Return("release", errorFor("ReleaseBaseDisk")).Once()
					if tc.errorAt == "ReleaseBaseDisk" {
						return
					}
					scheduler.On("WaitTask", ctx, execCtx, "release").
						Return(nil, errorFor("WaitRelease")).Once()
					if tc.errorAt == "WaitRelease" {
						return
					}
				}
				storage.On("DiskDeleted", ctx, "disk", mock.Anything).
					Return(errorFor("DiskDeleted")).Once()
			}
			setupExpectations()

			var err error
			if tc.cancel {
				err = task.Cancel(ctx, execCtx)
			} else {
				err = task.Run(ctx, execCtx)
			}
			if tc.nonCancellable {
				require.Error(t, err)
				require.True(t, errors.Is(err, errors.NewEmptyNonCancellableError()))
				require.False(t, errors.CanRetry(err))
			} else if tc.errorAt != "" {
				require.True(t, errors.Is(err, assert.AnError))
				if !tc.cancel {
					retryError, ok := err.(*errors.RetriableError)
					require.True(t, ok)
					require.True(t, retryError.IgnoreRetryLimit)
				}
			} else {
				require.NoError(t, err)
			}
			if !tc.expectNBS {
				nbsFactory.AssertNotCalled(t, "GetClient", mock.Anything, mock.Anything)
				nbsClient.AssertNotCalled(t, "Describe", mock.Anything, mock.Anything)
				nbsClient.AssertNotCalled(t, "Delete", mock.Anything, mock.Anything)
				nbsClient.AssertNotCalled(t, "DeleteSync", mock.Anything, mock.Anything)
				execCtx.AssertNotCalled(t, "SetEstimatedInflightDuration", mock.Anything)
			}
			mock.AssertExpectationsForObjects(t, storage, scheduler, poolService, nbsFactory, nbsClient, cellSelector, execCtx)
		})
	}
}
