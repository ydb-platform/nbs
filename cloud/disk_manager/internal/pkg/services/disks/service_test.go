package disks

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	cells_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	disks_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestAreOverlayDisksSupportedForDiskKind(t *testing.T) {
	diskService := &service{
		config: &disks_config.DisksConfig{},
	}

	require.True(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD},
	))
	require.True(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_HDD},
	))
	require.False(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_NONREPLICATED},
	))
	require.False(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_MIRROR2},
	))
	require.False(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_MIRROR3},
	))
	require.False(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_HDD_NONREPLICATED},
	))
	require.False(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_LOCAL},
	))
	require.False(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_HDD_LOCAL},
	))

	require.False(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{
			Kind:     types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
			FolderId: "folder-id",
		},
	))
	diskService.config.OverlayDiskRegistryBasedDisksFolderIdAllowList = []string{"folder-id"}
	require.True(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{
			Kind:     types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
			FolderId: "folder-id",
		},
	))
	require.False(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{
			Kind:     types.DiskKind_DISK_KIND_SSD_LOCAL,
			FolderId: "folder-id",
		},
	))

	diskService.config.EnableOverlayDiskRegistryBasedDisks = proto.Bool(true)

	require.True(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_NONREPLICATED},
	))
	require.True(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_MIRROR2},
	))
	require.True(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_MIRROR3},
	))
	require.True(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_HDD_NONREPLICATED},
	))
	require.False(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_LOCAL},
	))
	require.False(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_HDD_LOCAL},
	))
}

func TestDiskServicegetZoneIDForExistingDisk(
	t *testing.T,
) {
	testCases := []struct {
		name                      string
		actualDiskZoneID          string
		requestedDiskZoneID       string
		requestedZoneContainsCell bool
		expectedZoneID            string
		expectedErrorText         string
	}{
		{
			name:                      "Actual disk's zone ID equals the requested disk's zone ID",
			requestedDiskZoneID:       "zone-a",
			actualDiskZoneID:          "zone-a",
			requestedZoneContainsCell: false,
			expectedZoneID:            "zone-a",
			expectedErrorText:         "",
		},
		{
			name:                      "Actual disk's zone ID is a cell and is equal to the requested zone ID",
			requestedDiskZoneID:       "zone-a",
			actualDiskZoneID:          "zone-a",
			requestedZoneContainsCell: true,
			expectedZoneID:            "zone-a",
			expectedErrorText:         "",
		},
		{
			name:                      "The disk is located in a cell of requested disks's zone ID",
			requestedDiskZoneID:       "zone-a",
			actualDiskZoneID:          "zone-a-shard1",
			requestedZoneContainsCell: true,
			expectedZoneID:            "zone-a-shard1",
			expectedErrorText:         "",
		},
		{
			name:                      "Requested zone ID does not match with an actual zone ID",
			requestedDiskZoneID:       "zone-a",
			actualDiskZoneID:          "zone-b",
			requestedZoneContainsCell: false,
			expectedZoneID:            "",
			expectedErrorText:         "does not match with an actual zone ID",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			storage := storage_mocks.NewStorageMock()
			cellSelector := cells_mocks.NewCellSelectorMock()

			storage.On("GetDiskMeta", ctx, "disk").Return(&resources.DiskMeta{
				ZoneID: testCase.actualDiskZoneID,
			}, nil)

			cellSelector.On(
				"ZoneContainsCell",
				testCase.requestedDiskZoneID,
				testCase.actualDiskZoneID,
			).Return(testCase.requestedZoneContainsCell).Maybe()

			diskService := &service{
				cellSelector:    cellSelector,
				resourceStorage: storage,
			}

			zoneID, err := diskService.getZoneIDForExistingDisk(
				ctx,
				&disk_manager.DiskId{
					DiskId: "disk",
					ZoneId: testCase.requestedDiskZoneID,
				},
			)

			require.Equal(t, testCase.expectedZoneID, zoneID)
			if len(testCase.expectedErrorText) == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.ErrorContains(t, err, testCase.expectedErrorText)
			}

			mock.AssertExpectationsForObjects(t, storage, cellSelector)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestMigrateDiskResolvesDestinationPGCell(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
	scheduler := tasks_mocks.NewSchedulerMock()

	svc := &service{
		taskScheduler:   scheduler,
		resourceStorage: storage,
		cellSelector:    cellSelector,
	}

	// Source disk in zone-a.
	storage.On("GetDiskMeta", ctx, "disk-1").Return(&resources.DiskMeta{
		ZoneID: "zone-a",
	}, nil)

	// Destination PG on zone-b-shard1.
	storage.On("GetPlacementGroupMeta", ctx, "pg-dst").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-dst",
			ZoneID: "zone-b-shard1",
		}, nil,
	)

	cellSelector.On("ResolveCells", "zone-b").Return(
		[]string{"zone-b", "zone-b-shard1"}, nil,
	)

	scheduler.On(
		"ScheduleTask",
		ctx,
		"disks.MigrateDisk",
		"",
		mock.MatchedBy(func(msg proto.Message) bool {
			req, ok := msg.(*protos.MigrateDiskRequest)
			return ok && req.DstZoneId == "zone-b-shard1" &&
				req.DstPlacementGroupId == "pg-dst" &&
				req.Disk.ZoneId == "zone-a" &&
				!req.IsMigrationBetweenCells
		}),
	).Return("task-1", nil)

	_, err := svc.MigrateDisk(ctx, &disk_manager.MigrateDiskRequest{
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: "disk-1",
		},
		DstZoneId:           "zone-b",
		DstPlacementGroupId: "pg-dst",
	})
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, cellSelector, scheduler)
}

func TestMigrateDiskSameZoneWithoutPGRejected(t *testing.T) {
	ctx := context.Background()

	svc := &service{
		taskScheduler:   tasks_mocks.NewSchedulerMock(),
		resourceStorage: storage_mocks.NewStorageMock(),
		cellSelector:    cells_mocks.NewCellSelectorMock(),
	}

	_, err := svc.MigrateDisk(ctx, &disk_manager.MigrateDiskRequest{
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: "disk-1",
		},
		DstZoneId: "zone-a",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "cannot migrate disk to the same zone")
}

func TestMigrateDiskSameZoneDifferentCellViaPG(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
	scheduler := tasks_mocks.NewSchedulerMock()

	svc := &service{
		taskScheduler:   scheduler,
		resourceStorage: storage,
		cellSelector:    cellSelector,
	}

	// Source disk on zone-a-shard1.
	storage.On("GetDiskMeta", ctx, "disk-1").Return(&resources.DiskMeta{
		ZoneID: "zone-a-shard1",
	}, nil)

	cellSelector.On(
		"ZoneContainsCell", "zone-a", "zone-a-shard1",
	).Return(true)

	// Destination PG on zone-a-shard2 (different cell, same zone).
	storage.On("GetPlacementGroupMeta", ctx, "pg-dst").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-dst",
			ZoneID: "zone-a-shard2",
		}, nil,
	)

	cellSelector.On("ResolveCells", "zone-a-shard2").Return(
		[]string{"zone-a-shard2"}, nil,
	)

	scheduler.On(
		"ScheduleTask",
		ctx,
		"disks.MigrateDisk",
		"",
		mock.MatchedBy(func(msg proto.Message) bool {
			req, ok := msg.(*protos.MigrateDiskRequest)
			return ok && req.DstZoneId == "zone-a-shard2" &&
				req.DstPlacementGroupId == "pg-dst" &&
				req.Disk.ZoneId == "zone-a-shard1" &&
				req.IsMigrationBetweenCells
		}),
	).Return("task-1", nil)

	_, err := svc.MigrateDisk(ctx, &disk_manager.MigrateDiskRequest{
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: "disk-1",
		},
		DstZoneId:               "zone-a-shard2",
		DstPlacementGroupId:     "pg-dst",
		IsMigrationBetweenCells: true,
	})
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, cellSelector, scheduler)
}

func TestMigrateDiskSameZoneSameCellViaPGRejected(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

	svc := &service{
		taskScheduler:   tasks_mocks.NewSchedulerMock(),
		resourceStorage: storage,
		cellSelector:    cellSelector,
	}

	// Source disk on zone-a-shard1.
	storage.On("GetDiskMeta", ctx, "disk-1").Return(&resources.DiskMeta{
		ZoneID: "zone-a-shard1",
	}, nil)

	cellSelector.On(
		"ZoneContainsCell", "zone-a", "zone-a-shard1",
	).Return(true)

	// Destination PG on zone-a-shard1 (same cell as disk).
	storage.On("GetPlacementGroupMeta", ctx, "pg-dst").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-dst",
			ZoneID: "zone-a-shard1",
		}, nil,
	)

	cellSelector.On("ResolveCells", "zone-a").Return(
		[]string{"zone-a-shard1", "zone-a-shard2"}, nil,
	)

	_, err := svc.MigrateDisk(ctx, &disk_manager.MigrateDiskRequest{
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: "disk-1",
		},
		DstZoneId:           "zone-a",
		DstPlacementGroupId: "pg-dst",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "cannot migrate disk to the same zone")
}

func TestMigrateDiskDestinationPGZoneMismatch(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

	svc := &service{
		taskScheduler:   tasks_mocks.NewSchedulerMock(),
		resourceStorage: storage,
		cellSelector:    cellSelector,
	}

	// Source disk in zone-a.
	storage.On("GetDiskMeta", ctx, "disk-1").Return(&resources.DiskMeta{
		ZoneID: "zone-a",
	}, nil)

	// Destination PG on zone-c-shard1, but request says zone-b.
	storage.On("GetPlacementGroupMeta", ctx, "pg-dst").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-dst",
			ZoneID: "zone-c-shard1",
		}, nil,
	)

	cellSelector.On("ResolveCells", "zone-b").Return(
		[]string{"zone-b"}, nil,
	)

	_, err := svc.MigrateDisk(ctx, &disk_manager.MigrateDiskRequest{
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: "disk-1",
		},
		DstZoneId:           "zone-b",
		DstPlacementGroupId: "pg-dst",
	})
	require.Error(t, err)
	require.ErrorContains(t, err,
		"destination placement group pg-dst is in zone zone-c-shard1, not in requested destination zone zone-b")
}

func TestMigrateDiskDestinationPGNotFound(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

	svc := &service{
		taskScheduler:   tasks_mocks.NewSchedulerMock(),
		resourceStorage: storage,
		cellSelector:    cellSelector,
	}

	// Source disk in zone-a.
	storage.On("GetDiskMeta", ctx, "disk-1").Return(&resources.DiskMeta{
		ZoneID: "zone-a",
	}, nil)

	// PG not found.
	storage.On("GetPlacementGroupMeta", ctx, "pg-missing").Return(
		(*resources.PlacementGroupMeta)(nil), nil,
	)

	_, err := svc.MigrateDisk(ctx, &disk_manager.MigrateDiskRequest{
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: "disk-1",
		},
		DstZoneId:           "zone-b",
		DstPlacementGroupId: "pg-missing",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "destination placement group pg-missing not found")
}
