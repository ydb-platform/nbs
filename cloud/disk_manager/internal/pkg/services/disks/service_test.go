package disks

import (
	"bytes"
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	cells_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/mocks"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	disks_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	pools_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
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
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_DIRECT_MIRROR3OF5_GROUP},
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
	require.False(t, diskService.areOverlayDisksSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_DIRECT_MIRROR3OF5_GROUP},
	))
}

func TestIsCreationFromBaseDiskSupportedForDiskKind(t *testing.T) {
	diskService := &service{
		config: &disks_config.DisksConfig{},
	}

	// Disabled by default.
	require.False(t, diskService.isCreationFromBaseDiskSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_NONREPLICATED},
	))

	diskService.config.DiskRegistryBasedDisksCreationFromBaseDisksFolderIdAllowList =
		[]string{"folder-id"}

	require.True(t, diskService.isCreationFromBaseDiskSupportedForDiskKind(
		&protos.CreateDiskParams{
			Kind:     types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
			FolderId: "folder-id",
		},
	))
	require.False(t, diskService.isCreationFromBaseDiskSupportedForDiskKind(
		&protos.CreateDiskParams{
			Kind:     types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
			FolderId: "another-folder-id",
		},
	))
	// Only Disk Registry based disks.
	require.False(t, diskService.isCreationFromBaseDiskSupportedForDiskKind(
		&protos.CreateDiskParams{
			Kind:     types.DiskKind_DISK_KIND_SSD,
			FolderId: "folder-id",
		},
	))

	diskService.config.EnableDiskRegistryBasedDisksCreationFromBaseDisks = proto.Bool(true)

	require.True(t, diskService.isCreationFromBaseDiskSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_NONREPLICATED},
	))
	require.True(t, diskService.isCreationFromBaseDiskSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_MIRROR2},
	))
	require.True(t, diskService.isCreationFromBaseDiskSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_MIRROR3},
	))
	require.True(t, diskService.isCreationFromBaseDiskSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_HDD_NONREPLICATED},
	))
	require.False(t, diskService.isCreationFromBaseDiskSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD},
	))
	require.False(t, diskService.isCreationFromBaseDiskSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_HDD},
	))
	require.False(t, diskService.isCreationFromBaseDiskSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_LOCAL},
	))
	require.False(t, diskService.isCreationFromBaseDiskSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_HDD_LOCAL},
	))
	require.False(t, diskService.isCreationFromBaseDiskSupportedForDiskKind(
		&protos.CreateDiskParams{Kind: types.DiskKind_DISK_KIND_SSD_DIRECT_MIRROR3OF5_GROUP},
	))
}

// Cell selection is not idempotent (with the MAX_FREE_BYTES policy it depends
// on the current capacities), and a retried request must schedule the very
// same task, so the cell is selected by the task (and persisted in its state),
// not by the service: the service only checks that a pool is configured for
// the requested zone.
func TestDiskServiceCreateDiskFromBaseDiskDoesNotSelectCell(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	poolService := pools_mocks.NewServiceMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

	diskService := &service{
		config: &disks_config.DisksConfig{
			EnableDiskRegistryBasedDisksCreationFromBaseDisks: proto.Bool(true),
		},
		taskScheduler:   scheduler,
		resourceStorage: storage,
		poolService:     poolService,
		cellSelector:    cellSelector,
	}

	storage.On("GetImageMeta", ctx, "image").Return(&resources.ImageMeta{
		ID:   "image",
		Size: 1 << 30,
	}, nil)

	poolService.On("IsPoolConfigured", ctx, "image", "zone").Return(true, nil)

	scheduler.On(
		"ScheduleTask",
		ctx,
		"disks.CreateDiskFromBaseDisk",
		"",
		mock.MatchedBy(func(request proto.Message) bool {
			typedRequest, ok := request.(*protos.CreateDiskFromBaseDiskRequest)
			return ok &&
				typedRequest.SrcImageId == "image" &&
				typedRequest.Params.Disk.DiskId == "disk" &&
				typedRequest.Params.Disk.ZoneId == "zone"
		}),
	).Return("task", nil)

	taskID, err := diskService.CreateDisk(ctx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcImageId{
			SrcImageId: "image",
		},
		Size: 262144 * 4096,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone",
			DiskId: "disk",
		},
		CloudId:  "cloud",
		FolderId: "folder",
	})
	// No calls to the cell selector are expected.
	mock.AssertExpectationsForObjects(
		t,
		storage,
		scheduler,
		poolService,
		cellSelector,
	)
	require.NoError(t, err)
	require.Equal(t, "task", taskID)
}

func TestIsEncryptionSupportedForCreationFromBaseDisk(t *testing.T) {
	for _, tc := range []struct {
		name           string
		encryptionDesc *types.EncryptionDesc
		allowed        bool
	}{
		{
			name:           "no encryption desc",
			encryptionDesc: nil,
			allowed:        true,
		},
		{
			name: "no encryption",
			encryptionDesc: &types.EncryptionDesc{
				Mode: types.EncryptionMode_NO_ENCRYPTION,
			},
			allowed: true,
		},
		{
			// NBS encrypts the data on its side.
			name: "encryption at rest",
			encryptionDesc: &types.EncryptionDesc{
				Mode: types.EncryptionMode_ENCRYPTION_WITH_ROOT_KMS_PROVIDED_KEY,
			},
			allowed: true,
		},
		{
			// The data read from the proxy overlay disk would need to be
			// encrypted by the transfer.
			name: "client-side encryption",
			encryptionDesc: &types.EncryptionDesc{
				Mode: types.EncryptionMode_ENCRYPTION_AES_XTS,
				Key: &types.EncryptionDesc_KeyHash{
					KeyHash: []byte("hash"),
				},
			},
			allowed: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(
				t,
				tc.allowed,
				isEncryptionSupportedForCreationFromBaseDisk(tc.encryptionDesc),
			)
		})
	}
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

func TestCreateDiskSsdDirectMirror3Of5GroupRequiresStoragePool(t *testing.T) {
	ctx := context.Background()
	diskService := &service{
		config: &disks_config.DisksConfig{},
	}

	_, err := diskService.CreateDisk(ctx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: 4096,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD_DIRECT_MIRROR3OF5_GROUP,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone",
			DiskId: "disk",
		},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "storage_pool_name is required")
}

func TestCreateDiskSsdDirectMirror3Of5GroupRejectsImageSource(t *testing.T) {
	ctx := context.Background()
	diskService := &service{
		config: &disks_config.DisksConfig{},
	}

	_, err := diskService.CreateDisk(ctx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcImageId{
			SrcImageId: "image",
		},
		Size:            4096,
		Kind:            disk_manager.DiskKind_DISK_KIND_SSD_DIRECT_MIRROR3OF5_GROUP,
		StoragePoolName: "ddp1",
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone",
			DiskId: "disk",
		},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "can only be created empty")
}

func TestCreateDiskSsdDirectMirror3Of5GroupRejectsPlacementGroup(t *testing.T) {
	ctx := context.Background()
	diskService := &service{
		config: &disks_config.DisksConfig{},
	}

	_, err := diskService.CreateDisk(ctx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size:             4096,
		Kind:             disk_manager.DiskKind_DISK_KIND_SSD_DIRECT_MIRROR3OF5_GROUP,
		StoragePoolName:  "ddp1",
		PlacementGroupId: "pg",
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone",
			DiskId: "disk",
		},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "don't support placement groups")
}

func TestCreateDiskSsdDirectMirror3Of5GroupRejectsEncryption(t *testing.T) {
	ctx := context.Background()
	diskService := &service{
		config: &disks_config.DisksConfig{},
	}

	_, err := diskService.CreateDisk(ctx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size:            4096,
		Kind:            disk_manager.DiskKind_DISK_KIND_SSD_DIRECT_MIRROR3OF5_GROUP,
		StoragePoolName: "ddp1",
		EncryptionDesc: &disk_manager.EncryptionDesc{
			Mode: disk_manager.EncryptionMode_ENCRYPTION_WITH_ROOT_KMS_PROVIDED_KEY,
		},
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone",
			DiskId: "disk",
		},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "don't support encryption")
}

func TestDiskOperationGuardModePersistsAcrossWorkerConfigs(t *testing.T) {
	for _, operation := range []struct {
		name     string
		taskType string
		kind     string
		schedule func(context.Context, *service) (string, error)
	}{
		{"resize", "disks.ResizeDisk", resources.DiskOperationResize, func(ctx context.Context, svc *service) (string, error) {
			return svc.ResizeDisk(ctx, &disk_manager.ResizeDiskRequest{
				DiskId: &disk_manager.DiskId{ZoneId: "cell", DiskId: "disk"}, Size: 8192,
			})
		}},
		{"alter", "disks.AlterDisk", resources.DiskOperationAlter, func(ctx context.Context, svc *service) (string, error) {
			return svc.AlterDisk(ctx, &disk_manager.AlterDiskRequest{
				DiskId: &disk_manager.DiskId{ZoneId: "cell", DiskId: "disk"}, CloudId: "cloud", FolderId: "folder",
			})
		}},
		{"migrate", "disks.MigrateDisk", resources.DiskOperationMigrate, func(ctx context.Context, svc *service) (string, error) {
			return svc.MigrateDisk(ctx, &disk_manager.MigrateDiskRequest{
				DiskId: &disk_manager.DiskId{ZoneId: "cell", DiskId: "disk"}, DstZoneId: "target",
			})
		}},
	} {
		for _, mode := range []struct {
			name    string
			enabled bool
		}{{"default-disabled", false}, {"enabled", true}} {
			t.Run(operation.name+"/"+mode.name, func(t *testing.T) {
				ctx := logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.DebugLevel))
				storage := storage_mocks.NewStorageMock()
				scheduler := tasks_mocks.NewSchedulerMock()
				factory := nbs_mocks.NewFactoryMock()
				exec := tasks_mocks.NewExecutionContextMock()
				config := &disks_config.DisksConfig{}
				if mode.enabled {
					config.EnableDiskOperationGuard = proto.Bool(true)
				}
				svc := &service{config: config, resourceStorage: storage, taskScheduler: scheduler}
				storage.On("GetDiskMeta", ctx, "disk").Return(&resources.DiskMeta{ZoneID: "cell"}, nil).Once()
				var request []byte
				scheduler.On("ScheduleTask", ctx, operation.taskType, "", mock.Anything).Run(func(args mock.Arguments) {
					message := args.Get(3).(interface {
						proto.Message
						GetDisk() *types.Disk
						GetUseDiskOperationGuard() bool
					})
					require.Equal(t, "disk", message.GetDisk().GetDiskId())
					require.Equal(t, "cell", message.GetDisk().GetZoneId())
					require.Equal(t, mode.enabled, message.GetUseDiskOperationGuard())
					var err error
					request, err = proto.Marshal(message)
					require.NoError(t, err)
				}).Return("operation", nil).Once()
				taskID, err := operation.schedule(ctx, svc)
				require.NoError(t, err)
				require.Equal(t, "operation", taskID)

				// Execute on a fresh worker with the opposite local setting.
				workerConfig := &disks_config.DisksConfig{EnableDiskOperationGuard: proto.Bool(!mode.enabled)}
				registry := tasks.NewRegistry()
				scheduler.On("ScheduleRegularTasks", ctx, "disks.ClearDeletedDisks", mock.Anything).Once()
				require.NoError(t, RegisterForExecution(ctx, workerConfig, nil, "", storage, nil, registry, scheduler, nil, factory, nil))
				task, err := registry.NewTask(operation.taskType)
				require.NoError(t, err)
				var initialState []byte
				if operation.name == "migrate" {
					initialState, err = proto.Marshal(&protos.MigrateDiskTaskState{
						Status: protos.MigrationStatus_Replicating, ReplicateTaskID: "replicate", FillGeneration: 7,
					})
					require.NoError(t, err)
					scheduler.On("WaitTask", ctx, mock.Anything, "replicate").Return(nil, errors.NewInterruptExecutionError()).Once()
				} else {
					factory.On("GetClient", ctx, "cell").Return(nil, errors.NewInterruptExecutionError()).Once()
				}
				exec.On("GetTaskID").Return("operation").Maybe()
				if mode.enabled {
					expectDiskOperationCheckpoint(t, ctx, exec, nil)
					storage.On("ReserveDiskOperation", ctx, mock.Anything, resources.DiskOperation{
						DiskID: "disk", CellID: "cell", TaskID: "operation", Kind: operation.kind,
					}).Return(&resources.DiskMeta{}, nil).Once()
				}
				require.NoError(t, task.Load(request, initialState))
				require.ErrorIs(t, task.Run(ctx, exec), errors.NewInterruptExecutionError())
				state, err := task.Save()
				require.NoError(t, err)
				guarded := task.(*diskOperationTask)
				if mode.enabled {
					require.True(t, guarded.state().Acquired)
				} else {
					require.True(t, bytes.Equal(initialState, state), "unguarded execution must preserve the original task state")
				}

				// Cancellation after reload follows the same persisted mode.
				restored, err := registry.NewTask(operation.taskType)
				require.NoError(t, err)
				require.NoError(t, restored.Load(request, state))
				if operation.name == "migrate" {
					target := nbs_mocks.NewClientMock()
					exec.On("HasEvent", ctx, int64(protos.MigrateDiskTaskEvents_FINISH_MIGRATION)).Return(false).Once()
					factory.On("GetClient", ctx, "target").Return(target, nil).Once()
					target.On("DeleteWithFillGeneration", ctx, "disk", uint64(7)).Return(nil).Once()
					defer target.AssertExpectations(t)
				}
				if mode.enabled {
					expectDiskOperationCheckpoint(t, ctx, exec, nil)
					storage.On("ReleaseDiskOperation", ctx, mock.Anything, "disk", "operation").Return(nil).Once()
				}
				require.NoError(t, restored.Cancel(ctx, exec))
				if mode.enabled {
					require.True(t, restored.(*diskOperationTask).state().Released)
				}
				mock.AssertExpectationsForObjects(t, storage, scheduler, factory, exec)
			})
		}
	}
}
