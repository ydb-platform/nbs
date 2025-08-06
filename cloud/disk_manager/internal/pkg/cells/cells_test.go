package cells

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestCellsIsFolderAllowed(t *testing.T) {
	testCases := []struct {
		name            string
		excludedFolders []string
		includedFolders []string
		mustBeAllowed   []string
		mustBeDenied    []string
	}{
		{name: "Empty config", mustBeAllowed: []string{"anyFolder"}},
		{
			name:            "Excluded folders only",
			excludedFolders: []string{"excludedFolderID"},
			mustBeDenied:    []string{"excludedFolderID"},
			mustBeAllowed:   []string{"otherFolderID"},
		},
		{
			name:            "Excluded and included folders",
			excludedFolders: []string{"excludedFolderID"},
			includedFolders: []string{"includedFolderID"},
			mustBeDenied:    []string{"excludedFolderID", "otherFolderID"},
			mustBeAllowed:   []string{"includedFolderID"},
		},
		{
			name:            "Included and excluded folders are the same",
			excludedFolders: []string{"includedAndExcludedFolderID"},
			includedFolders: []string{"includedAndExcludedFolderID"},
			mustBeDenied:    []string{"includedAndExcludedFolderID"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			config := &cells_config.CellsConfig{
				FolderDenyList:  testCase.excludedFolders,
				FolderAllowList: testCase.includedFolders,
			}

			selector := cellSelector{
				config: config,
			}

			for _, folderID := range testCase.mustBeAllowed {
				require.True(t, selector.isFolderAllowed(folderID))
			}

			for _, folderID := range testCase.mustBeDenied {
				require.False(t, selector.isFolderAllowed(folderID))
			}
		})
	}
}

func TestDiskServiceGetZoneIDForExistingDisk(
	t *testing.T,
) {
	testCases := []struct {
		name                string
		actualDiskZoneID    string
		requestedDiskZoneID string
		isCellOfZone        bool
		expectedZoneID      string
		expectedErrorText   string
	}{
		{
			name:                "Actual disk's zone ID equals the requested disk's zone ID",
			requestedDiskZoneID: "zone-a",
			actualDiskZoneID:    "zone-a",
			isCellOfZone:        false,
			expectedZoneID:      "zone-a",
			expectedErrorText:   "",
		},
		{
			name:                "Actual disk's zone ID is a cell and is equal to the requested zone ID",
			requestedDiskZoneID: "zone-a",
			actualDiskZoneID:    "zone-a",
			isCellOfZone:        true,
			expectedZoneID:      "zone-a",
			expectedErrorText:   "",
		},
		{
			name:                "The disk is located in a cell of requested disks's zone ID",
			requestedDiskZoneID: "zone-a",
			actualDiskZoneID:    "zone-a-shard1",
			isCellOfZone:        true,
			expectedZoneID:      "zone-a-shard1",
			expectedErrorText:   "",
		},
		{
			name:                "Requested zone ID does not match with an actual zone ID",
			requestedDiskZoneID: "zone-a",
			actualDiskZoneID:    "zone-b",
			isCellOfZone:        false,
			expectedZoneID:      "",
			expectedErrorText:   "does not match with an actual zone ID",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()

			config := &cells_config.CellsConfig{}
			if testCase.isCellOfZone {
				config.Cells = map[string]*cells_config.ZoneCells{
					testCase.requestedDiskZoneID: {
						Cells: []string{testCase.actualDiskZoneID},
					},
				}
			}
			nbsFactory := nbs_mocks.NewFactoryMock()
			storage := storage_mocks.NewStorageMock()
			cellSelector := NewCellSelector(config, nbsFactory, storage)

			storage.On("GetDiskMeta", ctx, "disk").Return(&resources.DiskMeta{
				ZoneID: testCase.actualDiskZoneID,
			}, nil)

			zoneID, err := cellSelector.GetZoneIDForExistingDisk(
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
		})
	}
}
