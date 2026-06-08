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
