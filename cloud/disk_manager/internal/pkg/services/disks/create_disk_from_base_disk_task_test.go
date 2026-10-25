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

////////////////////////////////////////////////////////////////////////////////

func TestBaseDiskSlotKind(t *testing.T) {
	for _, tc := range []struct {
		kind     types.DiskKind
		expected types.DiskKind
	}{
		{types.DiskKind_DISK_KIND_SSD_NONREPLICATED, types.DiskKind_DISK_KIND_SSD},
		{types.DiskKind_DISK_KIND_SSD_MIRROR2, types.DiskKind_DISK_KIND_SSD},
		{types.DiskKind_DISK_KIND_SSD_MIRROR3, types.DiskKind_DISK_KIND_SSD},
		{types.DiskKind_DISK_KIND_HDD_NONREPLICATED, types.DiskKind_DISK_KIND_HDD},
		// Already media kinds: unchanged.
		{types.DiskKind_DISK_KIND_SSD, types.DiskKind_DISK_KIND_SSD},
		{types.DiskKind_DISK_KIND_HDD, types.DiskKind_DISK_KIND_HDD},
	} {
		require.Equal(t, tc.expected, baseDiskSlotKind(tc.kind), "%v", tc.kind)
	}
}

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
	storage.On("GetImageMeta", ctx, "image").Return(&resources.ImageMeta{
		ID: "image",
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

	// Base disk slot is acquired for the proxy overlay disk.
	poolService.On(
		"AcquireBaseDisk",
		headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_acquire"),
		&pools_protos.AcquireBaseDiskRequest{
			SrcImageId:      "image",
			OverlayDisk:     proxyOverlayDisk,
			OverlayDiskKind: types.DiskKind_DISK_KIND_SSD, // media kind of nrd
			OverlayDiskSize: (1 << 20) * 4096,
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
	require.True(t, task.state.ProxyOverlayDiskCreated)
	require.True(t, task.state.DataTransferred)
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
	storage.On("GetImageMeta", ctx, "image").Return(&resources.ImageMeta{
		ID: "image",
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

	poolService.On(
		"AcquireBaseDisk",
		headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_acquire"),
		&pools_protos.AcquireBaseDiskRequest{
			SrcImageId:      "image",
			OverlayDisk:     proxyOverlayDisk,
			OverlayDiskKind: types.DiskKind_DISK_KIND_SSD, // media kind of mirror3
			OverlayDiskSize: (1 << 20) * 4096,
		}).Return("acquire", nil)
	scheduler.On("WaitTask", ctx, execCtx, "acquire").Return(
		&pools_protos.AcquireBaseDiskResponse{
			BaseDiskId:           "base",
			BaseDiskCheckpointId: "checkpoint",
		},
		nil,
	)

	// Base disk can't be used as a base for the proxy overlay disk: no
	// fallback to direct reads from the base disk.
	nbsClient.On(
		"CreateProxyOverlayDisk",
		ctx,
		proxyOverlayDisk.DiskId,
		"base",
		"checkpoint",
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
		proxyOverlayDiskIDPrefix: "proxy-",
		request:                  request,
		// Run has selected the cell and created the proxy overlay disk, then
		// the task was cancelled.
		state: &protos.CreateDiskFromBaseDiskTaskState{
			SelectedCellId:          "zone",
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

	// Disk may have been created in NBS after the concurrent deletion has
	// finished, so it should be deleted anyway.
	nbsClient.On("Delete", ctx, "disk").Return(nil)

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
