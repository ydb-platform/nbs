package filesystem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nfs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestDeleteFilesystemTaskDisabledZones(t *testing.T) {
	disabledZone := common.NewDisabledZones([]string{"zone-d"})
	disabledShard := common.NewDisabledZones([]string{"zone-d-shard1"})
	testCases := []struct {
		name          string
		metadataZone  string
		requestZone   string
		disabledZones *common.DisabledZones
		expectedZone  string
		expectNFS     bool
		cancel        bool
		external      bool
		missingMeta   bool
		errorAt       string
	}{
		{
			name: "disabled zone", metadataZone: "zone-d", disabledZones: disabledZone,
		},
		{
			name: "disabled shard", metadataZone: "zone-d-shard1", disabledZones: disabledShard,
		},
		{
			name: "region does not disable shard", metadataZone: "zone-d-shard1",
			disabledZones: disabledZone, expectedZone: "zone-d-shard1", expectNFS: true,
		},
		{
			name: "shard does not disable region", metadataZone: "zone-d",
			disabledZones: disabledShard, expectedZone: "zone-d", expectNFS: true,
		},
		{
			name: "disabled metadata zone overrides request", metadataZone: "zone-d",
			requestZone: "zone-a", disabledZones: disabledZone,
		},
		{
			name: "allowed metadata zone overrides request", metadataZone: "zone-a",
			requestZone: "zone-d", disabledZones: disabledZone,
			expectedZone: "zone-a", expectNFS: true,
		},
		{
			name: "disabled request fallback", requestZone: "zone-d", disabledZones: disabledZone,
		},
		{
			name: "allowed request fallback", requestZone: "zone-a", disabledZones: disabledZone,
			expectedZone: "zone-a", expectNFS: true,
		},
		{
			name: "missing zone", disabledZones: disabledZone,
		},
		{
			name: "missing policy", metadataZone: "zone-d", expectedZone: "zone-d", expectNFS: true,
		},
		{
			name: "empty policy", metadataZone: "zone-d", disabledZones: common.NewDisabledZones(nil),
			expectedZone: "zone-d", expectNFS: true,
		},
		{
			name: "cancel in disabled zone", metadataZone: "zone-d", disabledZones: disabledZone, cancel: true,
		},
		{
			name: "disabled external filesystem", metadataZone: "zone-d", disabledZones: disabledZone, external: true,
		},
		{
			name: "cancel disabled external filesystem", metadataZone: "zone-d",
			disabledZones: disabledZone, external: true, cancel: true,
		},
		{
			name: "allowed external filesystem keeps manual wait", metadataZone: "zone-a",
			disabledZones: disabledZone, external: true, expectedZone: "zone-a", expectNFS: true,
		},
		{
			name: "missing metadata remains noncancellable", requestZone: "zone-d",
			disabledZones: disabledZone, missingMeta: true,
		},
		{
			name: "metadata deletion error", metadataZone: "zone-d", disabledZones: disabledZone,
			errorAt: "DeleteFilesystem",
		},
		{
			name: "mark deleted error", metadataZone: "zone-d", disabledZones: disabledZone,
			errorAt: "FilesystemDeleted",
		},
		{
			name: "cancel mark deleted error", metadataZone: "zone-d", disabledZones: disabledZone,
			cancel: true, errorAt: "FilesystemDeleted",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.WarnLevel))
			storage := resources_mocks.NewStorageMock()
			nfsFactory := nfs_mocks.NewFactoryMock()
			nfsClient := nfs_mocks.NewClientMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			execCtx := newExecutionContextMock()
			task := &deleteFilesystemTask{
				storage: storage, factory: nfsFactory, scheduler: scheduler,
				disabledZones: tc.disabledZones,
				request: &protos.DeleteFilesystemRequest{
					Filesystem: &protos.FilesystemId{ZoneId: tc.requestZone, FilesystemId: "filesystem"},
				},
				state: &protos.DeleteFilesystemTaskState{},
			}
			var meta *resources.FilesystemMeta
			if !tc.missingMeta {
				meta = &resources.FilesystemMeta{ZoneID: tc.metadataZone, IsExternal: tc.external}
			}
			var deleteError, markDeletedError error
			if tc.errorAt == "DeleteFilesystem" {
				deleteError = assert.AnError
			}
			if tc.errorAt == "FilesystemDeleted" {
				markDeletedError = assert.AnError
			}
			storage.On("DeleteFilesystem", ctx, "filesystem", "toplevel_task_id", mock.Anything).
				Return(meta, deleteError).Once()
			if !tc.missingMeta && deleteError == nil && !(tc.external && tc.expectNFS) {
				storage.On("FilesystemDeleted", ctx, "filesystem", mock.Anything).
					Return(markDeletedError).Once()
			}
			if tc.expectNFS {
				nfsFactory.On("NewClient", ctx, tc.expectedZone).Return(nfsClient, nil).Once()
				nfsClient.On("Close").Return(nil).Once()
				if tc.external {
					scheduler.On("ScheduleTask",
						headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_run"),
						"filesystem.DeleteExternalFilesystem", "", task.request,
					).Return("external-task", nil).Once()
					execCtx.On("SaveState", ctx).Return(nil).Once()
					scheduler.On("WaitTask", ctx, execCtx, "external-task").
						Return(nil, errors.NewInterruptExecutionError()).Once()
				} else {
					nfsClient.On("Delete", ctx, "filesystem", false).Return(nil).Once()
				}
			}

			var err error
			if tc.cancel {
				err = task.Cancel(ctx, execCtx)
			} else {
				err = task.Run(ctx, execCtx)
			}
			switch {
			case tc.errorAt != "":
				require.True(t, errors.Is(err, assert.AnError))
				if !tc.cancel {
					retryError, ok := err.(*errors.RetriableError)
					require.True(t, ok)
					require.True(t, retryError.IgnoreRetryLimit)
				}
			case tc.missingMeta:
				require.True(t, errors.Is(err, errors.NewNonCancellableError(nil)))
			case tc.external && tc.expectNFS:
				require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
				require.Equal(t, "external-task", task.state.DeleteExternalFilesystemTaskID)
			default:
				require.NoError(t, err)
			}
			if !tc.expectNFS {
				nfsFactory.AssertNotCalled(t, "NewClient", mock.Anything, mock.Anything)
				nfsClient.AssertNotCalled(t, "Delete", mock.Anything, mock.Anything, mock.Anything)
				nfsClient.AssertNotCalled(t, "Close")
			}
			if !tc.external || !tc.expectNFS {
				scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
				scheduler.AssertNotCalled(t, "WaitTask", mock.Anything, mock.Anything, mock.Anything)
				execCtx.AssertNotCalled(t, "SaveState", mock.Anything)
			}
			mock.AssertExpectationsForObjects(t, storage, nfsFactory, nfsClient, scheduler, execCtx)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDeleteExternalFilesystemTaskDisabledZones(t *testing.T) {
	disabledZone := common.NewDisabledZones([]string{"zone-d"})
	testCases := []struct {
		name          string
		metadataZone  string
		requestZone   string
		disabledZones *common.DisabledZones
		missingMeta   bool
		readError     bool
		expectDone    bool
	}{
		{
			name: "disabled metadata zone overrides request", metadataZone: "zone-d", requestZone: "zone-a",
			disabledZones: disabledZone, expectDone: true,
		},
		{
			name: "allowed metadata zone overrides request", metadataZone: "zone-a", requestZone: "zone-d",
			disabledZones: disabledZone,
		},
		{
			name: "disabled shard", metadataZone: "zone-d-shard1",
			disabledZones: common.NewDisabledZones([]string{"zone-d-shard1"}), expectDone: true,
		},
		{
			name: "region does not disable shard", metadataZone: "zone-d-shard1", disabledZones: disabledZone,
		},
		{
			name: "empty metadata zone falls back to request", requestZone: "zone-d",
			disabledZones: disabledZone, expectDone: true,
		},
		{
			name: "missing metadata falls back to request", requestZone: "zone-d",
			disabledZones: disabledZone, missingMeta: true, expectDone: true,
		},
		{
			name: "missing metadata and zones keeps manual wait", disabledZones: disabledZone, missingMeta: true,
		},
		{
			name: "missing policy keeps manual wait without lookup", requestZone: "zone-d",
		},
		{
			name: "empty policy keeps manual wait", metadataZone: "zone-d", disabledZones: common.NewDisabledZones(nil),
		},
		{
			name: "metadata read error retries", requestZone: "zone-d", disabledZones: disabledZone, readError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.WarnLevel))
			storage := resources_mocks.NewStorageMock()
			nfsFactory := nfs_mocks.NewFactoryMock()
			task := &deleteExternalFilesystemTask{
				storage: storage, factory: nfsFactory, disabledZones: tc.disabledZones,
				request: &protos.DeleteFilesystemRequest{
					Filesystem: &protos.FilesystemId{ZoneId: tc.requestZone, FilesystemId: "filesystem"},
				},
			}
			if tc.disabledZones != nil {
				var meta *resources.FilesystemMeta
				if !tc.missingMeta {
					meta = &resources.FilesystemMeta{ZoneID: tc.metadataZone, IsExternal: true}
				}
				var readError error
				if tc.readError {
					readError = assert.AnError
				}
				storage.On("GetFilesystemMeta", ctx, "filesystem").Return(meta, readError).Once()
			}

			err := task.Run(ctx, tasks_mocks.NewExecutionContextMock())
			switch {
			case tc.readError:
				require.True(t, errors.Is(err, assert.AnError))
				retryError, ok := err.(*errors.RetriableError)
				require.True(t, ok)
				require.True(t, retryError.IgnoreRetryLimit)
			case tc.expectDone:
				require.NoError(t, err)
			default:
				require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
			}
			nfsFactory.AssertNotCalled(t, "NewClient", mock.Anything, mock.Anything)
			storage.AssertNotCalled(t, "FilesystemDeleted", mock.Anything, mock.Anything, mock.Anything)
			if tc.disabledZones == nil {
				storage.AssertNotCalled(t, "GetFilesystemMeta", mock.Anything, mock.Anything)
			}
			mock.AssertExpectationsForObjects(t, storage, nfsFactory)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDeleteExternalFilesystemTaskDisabledZonesResumesParent(t *testing.T) {
	ctx := logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.WarnLevel))
	storage := resources_mocks.NewStorageMock()
	nfsFactory := nfs_mocks.NewFactoryMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	disabledZones := common.NewDisabledZones([]string{"zone-d-shard1"})
	request := &protos.DeleteFilesystemRequest{
		Filesystem: &protos.FilesystemId{ZoneId: "zone-d", FilesystemId: "filesystem"},
	}
	meta := &resources.FilesystemMeta{ZoneID: "zone-d-shard1", IsExternal: true}
	child := &deleteExternalFilesystemTask{
		storage: storage, factory: nfsFactory, disabledZones: disabledZones, request: request,
	}
	storage.On("GetFilesystemMeta", ctx, "filesystem").Return(meta, nil).Once()
	require.NoError(t, child.Run(ctx, tasks_mocks.NewExecutionContextMock()))

	// Once the existing child finishes, the scheduler resumes the parent with its saved state.
	parent := &deleteFilesystemTask{
		storage: storage, factory: nfsFactory, scheduler: scheduler,
		disabledZones: disabledZones, request: request,
		state: &protos.DeleteFilesystemTaskState{DeleteExternalFilesystemTaskID: "existing-child"},
	}
	storage.On("DeleteFilesystem", ctx, "filesystem", "toplevel_task_id", mock.Anything).
		Return(meta, nil).Once()
	storage.On("FilesystemDeleted", ctx, "filesystem", mock.Anything).Return(nil).Once()
	require.NoError(t, parent.Run(ctx, newExecutionContextMock()))

	nfsFactory.AssertNotCalled(t, "NewClient", mock.Anything, mock.Anything)
	scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	scheduler.AssertNotCalled(t, "WaitTask", mock.Anything, mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, storage, nfsFactory, scheduler)
}
