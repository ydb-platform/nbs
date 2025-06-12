package cells

import (
	"testing"

	"github.com/stretchr/testify/require"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
)

func TestCellsIsFolderAllowed(t *testing.T) {
	{
		config := &cells_config.CellsConfig{}

		selector := cellSelector{
			config: config,
		}

		require.True(t, selector.isFolderAllowed("anyFolder"))
	}

	{
		config := &cells_config.CellsConfig{
			ExcludedFolders: []string{"excludedFolderID"},
		}

		selector := cellSelector{
			config: config,
		}

		require.False(t, selector.isFolderAllowed("excludedFolderID"))
		require.True(t, selector.isFolderAllowed("otherFolderID"))
	}

	{
		config := &cells_config.CellsConfig{
			ExcludedFolders: []string{"excludedFolderID"},
			IncludedFolders: []string{"includedFolderID"},
		}

		selector := cellSelector{
			config: config,
		}

		require.False(t, selector.isFolderAllowed("excludedFolderID"))
		require.True(t, selector.isFolderAllowed("includedFolderID"))
		require.False(t, selector.isFolderAllowed("otherFolderID"))
	}

	{
		config := &cells_config.CellsConfig{
			ExcludedFolders: []string{"includedAndExcludedFolderID"},
			IncludedFolders: []string{"includedAndExcludedFolderID"},
		}

		selector := cellSelector{
			config: config,
		}

		require.False(t, selector.isFolderAllowed("includedAndExcludedFolderID"))
	}
}
