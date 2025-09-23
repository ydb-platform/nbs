package cells

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
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
	config := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			"zone1": {Cells: []string{"zone1-cell1", "zone1"}},
			"zone2": {Cells: []string{"zone2"}},
		},
	}

	selector := cellSelector{
		config: config,
	}

	selectedCell, err := selector.selectCell("zone1", "folder")
	require.NoError(t, err)
	require.Equal(t, "zone1-cell1", selectedCell) // First in the config.

	selectedCell, err = selector.selectCell("zone1-cell1", "folder")
	require.NoError(t, err)
	require.Equal(t, "zone1-cell1", selectedCell)

	selectedCell, err = selector.selectCell("zone2", "folder")
	require.NoError(t, err)
	require.Equal(t, "zone2", selectedCell)

	selectedCell, err = selector.selectCell("zone3", "folder")
	require.Error(t, err)
	require.ErrorContains(t, err, "incorrect zone ID provided")
}

func TestCellSelectorReturnsCorrectNBSClientIfConfigsIsNotSet(t *testing.T) {
	cellSelector := cellSelector{}

	selectedCell, err := cellSelector.selectCell("zone", "folder")
	require.NoError(t, err)
	require.Equal(t, "zone", selectedCell)
}

func TestCellSelectorReturnsCorrectNBSClientForLocalDisk(t *testing.T) {
	ctx := context.Background()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient1 := nbs_mocks.NewClientMock()
	nbsClient2 := nbs_mocks.NewClientMock()
	config := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			"zone-a": {Cells: []string{"zone-a", "zone-a-cell1"}},
		},
	}
	agent := []string{"agent1"}

	nbsFactory.On("GetClient", mock.Anything, "zone-a").Return(nbsClient1, nil)
	nbsFactory.On(
		"GetClient",
		mock.Anything,
		"zone-a-cell1",
	).Return(nbsClient2, nil)

	nbsClient1.On("QueryAvailableStorage", mock.Anything, agent).Return(
		[]nbs.AvailableStorageInfo{
			{
				AgentID:    "agent1",
				ChunkSize:  4096,
				ChunkCount: 10,
			},
		},
		nil,
	)
	nbsClient2.On("QueryAvailableStorage", mock.Anything, agent).Return(
		[]nbs.AvailableStorageInfo{
			{
				AgentID:    "agent1",
				ChunkSize:  0,
				ChunkCount: 0,
			},
		},
		nil,
	)

	cellSelector := cellSelector{
		config:     config,
		nbsFactory: nbsFactory,
	}

	selectedClient, err := cellSelector.SelectCellForLocalDisk(
		ctx,
		"zone-a",
		agent,
	)
	require.NoError(t, err)
	require.Equal(t, nbsClient1, selectedClient)
}
