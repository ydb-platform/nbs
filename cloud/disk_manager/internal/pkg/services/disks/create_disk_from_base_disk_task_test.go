package disks

import (
	"context"
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	cells_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	pools_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/mocks"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

// The task logs, so the context should carry a logger.
func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

// The base disk slot of the proxy overlay disk is charged by the transfer
// bandwidth.
const transferBandwidthMiBs = uint64(300)

func newPerformanceConfig() *performance_config.PerformanceConfig {
	transferFromDiskToDiskBandwidthMiBs := transferBandwidthMiBs
	return &performance_config.PerformanceConfig{
		TransferFromDiskToDiskBandwidthMiBs: &transferFromDiskToDiskBandwidthMiBs,
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateDiskFromBaseDiskTask(t *testing.T) {
	ctx := newContext()
	storage := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	poolService := pools_mocks.NewServiceMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

	request := &protos.CreateDiskFromBaseDiskRequest{
		SrcImageId: "image",
		Params: &protos.CreateDiskParams{
			BlocksCount: 1 << 20,
			Disk: &types.Disk{
				ZoneId: "zone",
				DiskId: "disk",
			},
			BlockSize: 4096,
			Kind:      types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
			CloudId:   "cloud",
			FolderId:  "folder",
		},
	}

	task := &createDiskFromBaseDiskTask{
		storage:                  storage,
		scheduler:                scheduler,
		poolService:              poolService,
		nbsFactory:               nbsFactory,
		performanceConfig:        newPerformanceConfig(),
		proxyOverlayDiskIDPrefix: "proxy-",
		request:                  request,
		state:                    &protos.CreateDiskFromBaseDiskTaskState{},
		cellSelector:             cellSelector,
	}

	proxyOverlayDisk := &types.Disk{
		ZoneId: "zone",
		DiskId: "proxy-disk_toplevel_task_id",
	}

	// TODO: Improve this expectations.
	storage.On("CreateDisk", ctx, mock.Anything).Return(&resources.DiskMeta{
		ID: "disk",
	}, nil)
	storage.On("DiskCreated", ctx, mock.Anything).Return(nil)

	cellSelector.On(
		"SelectCellForDisk",
		ctx,
		"zone",
		"folder",
		types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		false, // requireExactCellIDMatch
	).Return(nbsClient, nil)
	nbsClient.On("ZoneID").Return("zone")

	// Pool is configured in the selected cell, so a base disk slot is acquired
	// for the proxy overlay disk.
	poolService.On("IsPoolConfigured", ctx, "image", "zone").Return(true, nil)
	poolService.On(
		"AcquireBaseDisk",
		headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_acquire"),
		&pools_protos.AcquireBaseDiskRequest{
			SrcImageId:                "image",
			OverlayDisk:               proxyOverlayDisk,
			OverlayDiskKind:           types.DiskKind_DISK_KIND_SSD,
			ExpectedReadBandwidthMiBs: transferBandwidthMiBs,
		}).Return("acquire", nil)
	scheduler.On("WaitTask", ctx, execCtx, "acquire").Return(
		&pools_protos.AcquireBaseDiskResponse{
			BaseDiskId:           "base",
			BaseDiskCheckpointId: "checkpoint",
		},
		nil,
	)

	nbsClient.On(
		"CreateProxyOverlayDisk",
		ctx,
		proxyOverlayDisk.DiskId,
		"base",
		"checkpoint",
		uint64(1<<20)*4096, // maxSizeBytes: size of the disk being created
	).Return(true, nil)

	// Disk itself is not layered.
	nbsClient.On("Create", ctx, nbs.CreateDiskParams{
		ID:          "disk",
		BlocksCount: 1 << 20,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		CloudID:     "cloud",
		FolderID:    "folder",
	}).Return(nil)

	scheduler.On(
		"ScheduleZonalTask",
		headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id"),
		"dataplane.TransferFromDiskToDisk",
		"Fill ssd-nonreplicated disk disk from image image using base disk base",
		"zone",
		&dataplane_protos.TransferFromDiskToDiskRequest{
			SrcDisk:             proxyOverlayDisk,
			SrcDiskCheckpointId: "checkpoint",
			MountSrcDiskLocally: true,
			DstDisk: &types.Disk{
				ZoneId: "zone",
				DiskId: "disk",
			},
		},
	).Return("transfer", nil)
	scheduler.On("WaitTask", ctx, execCtx, "transfer").Return(
		&empty.Empty{},
		nil,
	)

	// Proxy overlay disk is deleted and the slot is released after the
	// transfer.
	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
	nbsClient.On("Delete", ctx, proxyOverlayDisk.DiskId).Return(nil)
	poolService.On(
		"ReleaseBaseDisk",
		headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_release"),
		&pools_protos.ReleaseBaseDiskRequest{
			OverlayDisk: proxyOverlayDisk,
		}).Return("release", nil)
	scheduler.On("WaitTask", ctx, execCtx, "release").Return(
		&empty.Empty{},
		nil,
	)

	execCtx.On("SaveState", ctx).Return(nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(
		t,
		storage,
		scheduler,
		poolService,
		nbsFactory,
		nbsClient,
		execCtx,
		cellSelector,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		protos.FillSource_FILL_SOURCE_BASE_DISK,
		task.state.FillSource,
	)
	require.True(t, task.state.ProxyOverlayDiskCreated)
	require.True(t, task.state.DataTransferred)
}

// No pool is configured for the image in the cell selected for the disk
// (pools are configured per cell, the disk service only checks the requested
// zone): the disk is filled from the snapshot storage, nothing is acquired
// from pools and no proxy overlay disk is created.
func TestCreateDiskFromBaseDiskTaskFallbackToSnapshotStorage(t *testing.T) {
	ctx := newContext()
	storage := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	poolService := pools_mocks.NewServiceMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

	request := &protos.CreateDiskFromBaseDiskRequest{
		SrcImageId: "image",
		Params: &protos.CreateDiskParams{
			BlocksCount: 1 << 20,
			Disk: &types.Disk{
				ZoneId: "zone",
				DiskId: "disk",
			},
			BlockSize: 4096,
			Kind:      types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
			CloudId:   "cloud",
			FolderId:  "folder",
		},
	}

	task := &createDiskFromBaseDiskTask{
		storage:                  storage,
		scheduler:                scheduler,
		poolService:              poolService,
		nbsFactory:               nbsFactory,
		performanceConfig:        newPerformanceConfig(),
		proxyOverlayDiskIDPrefix: "proxy-",
		request:                  request,
		state:                    &protos.CreateDiskFromBaseDiskTaskState{},
		cellSelector:             cellSelector,
	}

	// TODO: Improve this expectations.
	storage.On("CreateDisk", ctx, mock.Anything).Return(&resources.DiskMeta{
		ID: "disk",
	}, nil)
	storage.On("DiskCreated", ctx, mock.Anything).Return(nil)

	cellSelector.On(
		"SelectCellForDisk",
		ctx,
		"zone",
		"folder",
		types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		false, // requireExactCellIDMatch
	).Return(nbsClient, nil)
	nbsClient.On("ZoneID").Return("zone")

	poolService.On("IsPoolConfigured", ctx, "image", "zone").Return(false, nil)

	nbsClient.On("Create", ctx, nbs.CreateDiskParams{
		ID:          "disk",
		BlocksCount: 1 << 20,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		CloudID:     "cloud",
		FolderID:    "folder",
	}).Return(nil)

	scheduler.On(
		"ScheduleZonalTask",
		headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id"),
		"dataplane.TransferFromSnapshotToDisk",
		"",
		"zone",
		&dataplane_protos.TransferFromSnapshotToDiskRequest{
			SrcSnapshotId: "image",
			DstDisk: &types.Disk{
				ZoneId: "zone",
				DiskId: "disk",
			},
		},
	).Return("transfer", nil)
	scheduler.On("WaitTask", ctx, execCtx, "transfer").Return(
		&empty.Empty{},
		nil,
	)

	execCtx.On("SaveState", ctx).Return(nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(
		t,
		storage,
		scheduler,
		poolService,
		nbsFactory,
		nbsClient,
		execCtx,
		cellSelector,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		protos.FillSource_FILL_SOURCE_SNAPSHOT_STORAGE,
		task.state.FillSource,
	)
	require.False(t, task.state.ProxyOverlayDiskCreated)
	require.True(t, task.state.DataTransferred)

	// Nothing to clean up in pools on cancel either, only the disk itself.
	storage.On(
		"DeleteDisk",
		ctx,
		"disk",
		"toplevel_task_id",
		mock.Anything,
	).Return(&resources.DiskMeta{ID: "disk", ZoneID: "zone"}, nil)
	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
	nbsClient.On("Delete", ctx, "disk").Return(nil)
	storage.On("DiskDeleted", ctx, "disk", mock.Anything).Return(nil)

	err = task.Cancel(ctx, execCtx)
	mock.AssertExpectationsForObjects(
		t,
		storage,
		scheduler,
		poolService,
		nbsFactory,
		nbsClient,
		execCtx,
		cellSelector,
	)
	require.NoError(t, err)
}

func TestCreateDiskFromBaseDiskTaskFailureWhenProxyOverlayDiskIsNotCreated(
	t *testing.T,
) {

	ctx := newContext()
	storage := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	poolService := pools_mocks.NewServiceMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

	request := &protos.CreateDiskFromBaseDiskRequest{
		SrcImageId: "image",
		Params: &protos.CreateDiskParams{
			BlocksCount: 1 << 20,
			Disk: &types.Disk{
				ZoneId: "zone",
				DiskId: "disk",
			},
			BlockSize: 4096,
			Kind:      types.DiskKind_DISK_KIND_SSD_MIRROR3,
			CloudId:   "cloud",
			FolderId:  "folder",
		},
	}

	task := &createDiskFromBaseDiskTask{
		storage:                  storage,
		scheduler:                scheduler,
		poolService:              poolService,
		nbsFactory:               nbsFactory,
		performanceConfig:        newPerformanceConfig(),
		proxyOverlayDiskIDPrefix: "proxy-",
		request:                  request,
		state:                    &protos.CreateDiskFromBaseDiskTaskState{},
		cellSelector:             cellSelector,
	}

	proxyOverlayDisk := &types.Disk{
		ZoneId: "zone",
		DiskId: "proxy-disk_toplevel_task_id",
	}

	// TODO: Improve this expectations.
	storage.On("CreateDisk", ctx, mock.Anything).Return(&resources.DiskMeta{
		ID: "disk",
	}, nil)

	cellSelector.On(
		"SelectCellForDisk",
		ctx,
		"zone",
		"folder",
		types.DiskKind_DISK_KIND_SSD_MIRROR3,
		false, // requireExactCellIDMatch
	).Return(nbsClient, nil)
	nbsClient.On("ZoneID").Return("zone")

	poolService.On("IsPoolConfigured", ctx, "image", "zone").Return(true, nil)
	poolService.On(
		"AcquireBaseDisk",
		headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_acquire"),
		&pools_protos.AcquireBaseDiskRequest{
			SrcImageId:                "image",
			OverlayDisk:               proxyOverlayDisk,
			OverlayDiskKind:           types.DiskKind_DISK_KIND_SSD,
			ExpectedReadBandwidthMiBs: transferBandwidthMiBs,
		}).Return("acquire", nil)
	scheduler.On("WaitTask", ctx, execCtx, "acquire").Return(
		&pools_protos.AcquireBaseDiskResponse{
			BaseDiskId:           "base",
			BaseDiskCheckpointId: "checkpoint",
		},
		nil,
	)

	nbsClient.On("Create", ctx, nbs.CreateDiskParams{
		ID:          "disk",
		BlocksCount: 1 << 20,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD_MIRROR3,
		CloudID:     "cloud",
		FolderID:    "folder",
	}).Return(nil)

	// Base disk can't be used as a base for the proxy overlay disk: no
	// fallback to direct reads from the base disk.
	nbsClient.On(
		"CreateProxyOverlayDisk",
		ctx,
		proxyOverlayDisk.DiskId,
		"base",
		"checkpoint",
		uint64(1<<20)*4096, // maxSizeBytes: size of the disk being created
	).Return(false, nil)

	execCtx.On("SaveState", ctx).Return(nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(
		t,
		storage,
		scheduler,
		poolService,
		nbsFactory,
		nbsClient,
		execCtx,
		cellSelector,
	)
	require.Error(t, err)
	require.Equal(
		t,
		protos.FillSource_FILL_SOURCE_BASE_DISK,
		task.state.FillSource,
	)
	require.False(t, task.state.ProxyOverlayDiskCreated)
	require.False(t, task.state.DataTransferred)
}

// Disk has already been deleted by a concurrent disks.DeleteDisk task (which
// knows nothing about the proxy overlay disk), so storage.DeleteDisk returns
// nil. Proxy overlay disk and its base disk slot must be cleaned up anyway.
func TestCreateDiskFromBaseDiskTaskCancelAfterDiskIsDeletedConcurrently(
	t *testing.T,
) {

	ctx := newContext()
	storage := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	poolService := pools_mocks.NewServiceMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

	request := &protos.CreateDiskFromBaseDiskRequest{
		SrcImageId: "image",
		Params: &protos.CreateDiskParams{
			BlocksCount: 1 << 20,
			Disk: &types.Disk{
				ZoneId: "zone",
				DiskId: "disk",
			},
			BlockSize: 4096,
			Kind:      types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
			CloudId:   "cloud",
			FolderId:  "folder",
		},
	}

	task := &createDiskFromBaseDiskTask{
		storage:                  storage,
		scheduler:                scheduler,
		poolService:              poolService,
		nbsFactory:               nbsFactory,
		performanceConfig:        newPerformanceConfig(),
		proxyOverlayDiskIDPrefix: "proxy-",
		request:                  request,
		// Run has selected the cell and created the proxy overlay disk, then
		// the task was cancelled.
		state: &protos.CreateDiskFromBaseDiskTaskState{
			SelectedCellId:          "zone",
			FillSource:              protos.FillSource_FILL_SOURCE_BASE_DISK,
			ProxyOverlayDiskCreated: true,
		},
		cellSelector: cellSelector,
	}

	proxyOverlayDisk := &types.Disk{
		ZoneId: "zone",
		DiskId: "proxy-disk_toplevel_task_id",
	}

	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
	nbsClient.On("Delete", ctx, proxyOverlayDisk.DiskId).Return(nil)
	poolService.On(
		"ReleaseBaseDisk",
		headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_release"),
		&pools_protos.ReleaseBaseDiskRequest{
			OverlayDisk: proxyOverlayDisk,
		}).Return("release", nil)
	scheduler.On("WaitTask", ctx, execCtx, "release").Return(
		&empty.Empty{},
		nil,
	)

	storage.On(
		"DeleteDisk",
		ctx,
		"disk",
		"toplevel_task_id",
		mock.Anything,
	).Return((*resources.DiskMeta)(nil), nil)

	err := task.Cancel(ctx, execCtx)
	mock.AssertExpectationsForObjects(
		t,
		storage,
		scheduler,
		poolService,
		nbsFactory,
		nbsClient,
		execCtx,
		cellSelector,
	)
	require.NoError(t, err)
}

// Nothing has been created yet (cell is not even selected): Cancel must not
// touch NBS or pools.
func TestCreateDiskFromBaseDiskTaskCancelBeforeCellIsSelected(t *testing.T) {
	ctx := newContext()
	storage := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	poolService := pools_mocks.NewServiceMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

	request := &protos.CreateDiskFromBaseDiskRequest{
		SrcImageId: "image",
		Params: &protos.CreateDiskParams{
			BlocksCount: 1 << 20,
			Disk: &types.Disk{
				ZoneId: "zone",
				DiskId: "disk",
			},
			BlockSize: 4096,
			Kind:      types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
			CloudId:   "cloud",
			FolderId:  "folder",
		},
	}

	task := &createDiskFromBaseDiskTask{
		storage:                  storage,
		scheduler:                scheduler,
		poolService:              poolService,
		nbsFactory:               nbsFactory,
		performanceConfig:        newPerformanceConfig(),
		proxyOverlayDiskIDPrefix: "proxy-",
		request:                  request,
		state:                    &protos.CreateDiskFromBaseDiskTaskState{},
		cellSelector:             cellSelector,
	}

	storage.On(
		"DeleteDisk",
		ctx,
		"disk",
		"toplevel_task_id",
		mock.Anything,
	).Return((*resources.DiskMeta)(nil), nil)

	err := task.Cancel(ctx, execCtx)
	mock.AssertExpectationsForObjects(
		t,
		storage,
		scheduler,
		poolService,
		nbsFactory,
		nbsClient,
		execCtx,
		cellSelector,
	)
	require.NoError(t, err)
}

// The pool may be reconfigured while the task is running: the fill source
// is selected once and retries stick to it.
func TestCreateDiskFromBaseDiskTaskSelectFillSourceIsIdempotent(t *testing.T) {
	for _, tc := range []struct {
		name            string
		fillSource      protos.FillSource
		poolConfigured  bool
		expectPoolCheck bool
		expected        protos.FillSource
	}{
		{
			name:            "pool is configured",
			fillSource:      protos.FillSource_FILL_SOURCE_UNSPECIFIED,
			poolConfigured:  true,
			expectPoolCheck: true,
			expected:        protos.FillSource_FILL_SOURCE_BASE_DISK,
		},
		{
			name:            "pool is not configured",
			fillSource:      protos.FillSource_FILL_SOURCE_UNSPECIFIED,
			poolConfigured:  false,
			expectPoolCheck: true,
			expected:        protos.FillSource_FILL_SOURCE_SNAPSHOT_STORAGE,
		},
		{
			// Persisted by a previous attempt, the pool has been deleted since
			// then.
			name:            "base disk has already been selected",
			fillSource:      protos.FillSource_FILL_SOURCE_BASE_DISK,
			poolConfigured:  false,
			expectPoolCheck: false,
			expected:        protos.FillSource_FILL_SOURCE_BASE_DISK,
		},
		{
			// Persisted by a previous attempt, the pool has been configured
			// since then.
			name:            "snapshot storage has already been selected",
			fillSource:      protos.FillSource_FILL_SOURCE_SNAPSHOT_STORAGE,
			poolConfigured:  true,
			expectPoolCheck: false,
			expected:        protos.FillSource_FILL_SOURCE_SNAPSHOT_STORAGE,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newContext()
			poolService := pools_mocks.NewServiceMock()
			execCtx := tasks_mocks.NewExecutionContextMock()

			task := &createDiskFromBaseDiskTask{
				poolService: poolService,
				request: &protos.CreateDiskFromBaseDiskRequest{
					SrcImageId: "image",
					Params: &protos.CreateDiskParams{
						Disk: &types.Disk{
							ZoneId: "zone",
							DiskId: "disk",
						},
					},
				},
				state: &protos.CreateDiskFromBaseDiskTaskState{
					FillSource: tc.fillSource,
				},
			}

			if tc.expectPoolCheck {
				poolService.On("IsPoolConfigured", ctx, "image", "zone").Return(
					tc.poolConfigured,
					nil,
				)
				// The choice is persisted.
				execCtx.On("SaveState", ctx).Return(nil)
			}

			err := task.selectFillSource(ctx, execCtx, "zone")
			mock.AssertExpectationsForObjects(t, poolService, execCtx)
			require.NoError(t, err)
			require.Equal(t, tc.expected, task.state.FillSource)
		})
	}
}
