package cells

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

const (
	shardedZoneID = "zone-a"
	cellID1       = "zone-a"
	cellID2       = "zone-a-shard1"
	otherZoneID   = "zone-b"
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

func TestCellSelectorSelectsCorrectCell(t *testing.T) {
	ctx := context.Background()

	policy := cells_config.CellSelectionPolicy_FIRST_IN_CONFIG
	config := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			shardedZoneID: {Cells: []string{cellID2, cellID1}},
			otherZoneID:   {Cells: []string{otherZoneID}},
		},
		CellSelectionPolicy: &policy,
	}

	selector := cellSelector{
		config: config,
	}

	selectedCell, err := selector.selectCell(
		ctx,
		shardedZoneID,
		"folder",
		types.DiskKind_DISK_KIND_SSD,
	)
	require.NoError(t, err)
	require.Equal(t, cellID2, selectedCell) // First in the config.

	selectedCell, err = selector.selectCell(
		ctx,
		cellID2,
		"folder",
		types.DiskKind_DISK_KIND_SSD,
	)
	require.NoError(t, err)
	require.Equal(t, cellID2, selectedCell)

	selectedCell, err = selector.selectCell(
		ctx,
		otherZoneID,
		"folder",
		types.DiskKind_DISK_KIND_SSD,
	)
	require.NoError(t, err)
	require.Equal(t, otherZoneID, selectedCell)

	selectedCell, err = selector.selectCell(
		ctx,
		"incorrectZoneID",
		"folder",
		types.DiskKind_DISK_KIND_SSD,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "incorrect zone ID provided")
	require.Empty(t, selectedCell)
}

func TestCellSelectorReturnsCorrectNBSClientIfConfigsIsNotSet(t *testing.T) {
	ctx := context.Background()
	cellSelector := cellSelector{}

	selectedCell, err := cellSelector.selectCell(
		ctx,
		otherZoneID,
		"folder",
		types.DiskKind_DISK_KIND_SSD,
	)
	require.NoError(t, err)
	require.Equal(t, otherZoneID, selectedCell)
}

func TestCellSelectorReturnsCorrectCellMaxFreeBytesPolicy(t *testing.T) {
	ctx := context.Background()
	cellStorage := storage_mocks.NewStorageMock()

	policy := cells_config.CellSelectionPolicy_MAX_FREE_BYTES
	config := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			shardedZoneID: {Cells: []string{cellID2, cellID1}},
			otherZoneID:   {Cells: []string{otherZoneID}},
		},
		CellSelectionPolicy: &policy,
	}

	cellStorage.On(
		"GetRecentClusterCapacities",
		ctx,
		shardedZoneID,
		types.DiskKind_DISK_KIND_SSD,
	).Return([]storage.ClusterCapacity{
		{FreeBytes: 2048, CellID: cellID1},
		{FreeBytes: 1024, CellID: cellID2},
	}, nil)

	selector := cellSelector{
		config:  config,
		storage: cellStorage,
	}

	selectedCell, err := selector.selectCell(
		ctx,
		shardedZoneID,
		"folder",
		types.DiskKind_DISK_KIND_SSD,
	)
	require.NoError(t, err)
	require.Equal(t, cellID1, selectedCell)
}
