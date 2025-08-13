package cells

import (
	"testing"

	"github.com/stretchr/testify/require"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
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
