package disks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	cells_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestDiskServicegetZoneIDForExistingDisk(
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
			storage := storage_mocks.NewStorageMock()
			cellSelector := cells_mocks.NewCellSelectorMock()

			storage.On("GetDiskMeta", ctx, "disk").Return(&resources.DiskMeta{
				ZoneID: testCase.actualDiskZoneID,
			}, nil)
			cellSelector.On(
				"IsCellOfZone",
				testCase.actualDiskZoneID,
				testCase.requestedDiskZoneID,
			).Return(testCase.isCellOfZone)

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
		})
	}

}
