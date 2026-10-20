package disks

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	pools_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestDeleteDiskTaskCells(t *testing.T) {
	config := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			"zone":        {Cells: []string{"cell-a", "cell-b"}},
			"single-zone": {Cells: []string{"cell-a"}},
		},
	}

	testCases := []struct {
		name          string
		requestZone   string
		cells         []string
		withoutConfig bool
		withMetadata  bool
		sync          bool
		cancel        bool
		notFound      bool
		errorAt       string
	}{
		{name: "one cell", requestZone: "single-zone", cells: []string{"cell-a"}},
		{name: "multiple cells", requestZone: "zone", cells: []string{"cell-a", "cell-b"}},
		{name: "sync multiple cells", requestZone: "zone", cells: []string{"cell-a", "cell-b"}, sync: true},
		{name: "cancel multiple cells", requestZone: "zone", cells: []string{"cell-a", "cell-b"}, cancel: true},
		{name: "already absent in NBS", requestZone: "zone", cells: []string{"cell-a", "cell-b"}, notFound: true},
		{name: "sync already absent in NBS", requestZone: "zone", cells: []string{"cell-a", "cell-b"}, sync: true, notFound: true},
		{name: "explicit cell", requestZone: "cell-a", cells: []string{"cell-a"}},
		{name: "cells disabled", requestZone: "zone", cells: []string{"zone"}, withoutConfig: true},
		{name: "saved cell overrides public zone", requestZone: "zone", cells: []string{"cell-b"}, withMetadata: true},
		{name: "saved cell overrides unknown zone", requestZone: "unknown", cells: []string{"cell-b"}, withMetadata: true},
		{name: "schedule failure and retry", requestZone: "zone", cells: []string{"cell-a", "cell-b"}, errorAt: "ScheduleTask"},
		{name: "cleanup failure and retry", requestZone: "zone", cells: []string{"cell-a", "cell-b"}, errorAt: "WaitTask"},
		{name: "client failure and retry", requestZone: "zone", cells: []string{"cell-a", "cell-b"}, errorAt: "GetClient"},
		{name: "delete failure and retry", requestZone: "zone", cells: []string{"cell-a", "cell-b"}, errorAt: "Delete"},
		{name: "sync delete failure and retry", requestZone: "zone", cells: []string{"cell-a", "cell-b"}, sync: true, errorAt: "Delete"},
		{name: "cancel delete failure and retry", requestZone: "zone", cells: []string{"cell-a", "cell-b"}, cancel: true, errorAt: "Delete"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.WarnLevel))
			storage := storage_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			poolService := pools_mocks.NewServiceMock()
			factory := nbs_mocks.NewFactoryMock()
			execCtx := newExecutionContextMock()
			cellsConfig := config
			if tc.withoutConfig {
				cellsConfig = nil
			}
			var meta *resources.DiskMeta
			if tc.withMetadata {
				meta = &resources.DiskMeta{ZoneID: "cell-b"}
			}
			task := &deleteDiskTask{
				storage: storage, scheduler: scheduler, poolService: poolService,
				nbsFactory: factory, cellSelector: cells.NewCellSelector(cellsConfig, nil, nil, nil),
				request: &protos.DeleteDiskRequest{
					Disk:                &types.Disk{DiskId: "disk", ZoneId: tc.requestZone},
					DeleteIfNonExistent: true, Sync: tc.sync,
				},
				state: &protos.DeleteDiskTaskState{},
			}
			// Exercise request/state round-trip as done when a task is resumed.
			state, err := task.Save()
			require.NoError(t, err)
			request, err := proto.Marshal(task.request)
			require.NoError(t, err)
			require.NoError(t, task.Load(request, state))

			var clients []*nbs_mocks.ClientMock
			setup := func(errorAt string) {
				if tc.sync {
					storage.On("GetDiskMeta", ctx, "disk").Return(meta, nil).Once()
				}
				storage.On("DeleteDisk", ctx, "disk", "toplevel_task_id", mock.Anything).Return(meta, nil).Once()
				for _, cellID := range tc.cells {
					// Fail on the second cell after the first cell has succeeded.
					errorFor := func(step string) error {
						if cellID == "cell-b" && step == errorAt {
							return assert.AnError
						}
						return nil
					}
					key := "toplevel_task_id_delete_disk_from_incremental"
					if !tc.withMetadata && cellID != tc.requestZone {
						key += "_" + cellID
					}
					scheduler.On("ScheduleTask", headers.SetIncomingIdempotencyKey(ctx, key),
						"dataplane.DeleteDiskFromIncremental", "",
						&dataplane_protos.DeleteDiskFromIncrementalRequest{
							Disk: &types.Disk{ZoneId: cellID, DiskId: "disk"},
						}).Return(cellID, errorFor("ScheduleTask")).Once()
					if errorFor("ScheduleTask") != nil {
						return
					}
					scheduler.On("WaitTask", ctx, execCtx, cellID).Return(nil, errorFor("WaitTask")).Once()
					if errorFor("WaitTask") != nil {
						return
					}
					client := nbs_mocks.NewClientMock()
					clients = append(clients, client)
					factory.On("GetClient", ctx, cellID).Return(client, errorFor("GetClient")).Once()
					if errorFor("GetClient") != nil {
						return
					}
					method := "Delete"
					if tc.sync {
						method = "DeleteSync"
					}
					deleteErr := errorFor("Delete")
					if tc.notFound && cellID == "cell-a" {
						deleteErr = &nbs_client.ClientError{Code: nbs_client.E_NOT_FOUND}
					}
					client.On(method, ctx, "disk").Return(deleteErr).Once()
					if errorFor("Delete") != nil {
						return
					}
				}
				storage.On("DiskDeleted", ctx, "disk", mock.Anything).Return(nil).Once()
			}
			run := func() error {
				if tc.cancel {
					return task.Cancel(ctx, execCtx)
				}
				return task.Run(ctx, execCtx)
			}
			if tc.errorAt != "" {
				setup(tc.errorAt)
				err := run()
				require.True(t, errors.Is(err, assert.AnError))
				if !tc.cancel {
					retryError, ok := err.(*errors.RetriableError)
					require.True(t, ok)
					require.True(t, retryError.IgnoreRetryLimit)
				}
				storage.AssertNotCalled(t, "DiskDeleted", mock.Anything, mock.Anything, mock.Anything)
			}
			setup("")
			require.NoError(t, run())
			mock.AssertExpectationsForObjects(t, storage, scheduler, poolService, factory, execCtx)
			for _, client := range clients {
				client.AssertExpectations(t)
			}
		})
	}
}
