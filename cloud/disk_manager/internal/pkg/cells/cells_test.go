package cells

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

const (
	shardedZoneID = "zone-a"
	cellID1       = "zone-a"
	cellID2       = "zone-a-shard1"
	otherZoneID   = "zone-b"
)

////////////////////////////////////////////////////////////////////////////////

func testSelectCellForLocalDiskCellReturnsAnError(
	t *testing.T,
	correctCellResponseLatency time.Duration,
	emptyCellResponseLatency time.Duration,
	correctCellReturnsError bool,
) {

	ctx := context.Background()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClientCorrectCell := nbs_mocks.NewClientMock()
	nbsClientEmptyCell := nbs_mocks.NewClientMock()
	config := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			shardedZoneID: {Cells: []string{cellID1, cellID2}},
		},
	}
	agentIDs := []string{"agent1"}

	nbsFactory.On(
		"GetClient",
		mock.Anything,
		cellID1,
	).Return(nbsClientCorrectCell, nil)

	// If got a result, don't wait for other cells.
	nbsFactory.On(
		"GetClient",
		mock.Anything,
		cellID2,
	).Return(nbsClientEmptyCell, nil)

	correctCellError := error(nil)
	emptyCellError := assert.AnError
	if correctCellReturnsError {
		correctCellError = assert.AnError
		emptyCellError = nil
	}

	nbsClientCorrectCell.On("QueryAvailableStorage", mock.Anything, agentIDs).
		After(correctCellResponseLatency).
		Return(
			[]nbs.AvailableStorageInfo{
				{
					AgentID:    "agent1",
					ChunkSize:  4096,
					ChunkCount: 10,
				},
			},
			correctCellError,
		)
	nbsClientEmptyCell.On("QueryAvailableStorage", mock.Anything, agentIDs).
		After(emptyCellResponseLatency).
		Return(
			[]nbs.AvailableStorageInfo(nil),
			emptyCellError,
		)

	cellSelector := cellSelector{
		config:     config,
		nbsFactory: nbsFactory,
	}

	selectedClient, err := cellSelector.SelectCellForLocalDisk(
		ctx,
		cellID1,
		agentIDs,
	)
	if correctCellReturnsError {
		require.Error(t, err)
		require.Nil(t, selectedClient)
	} else {
		require.NoError(t, err)
		require.Equal(t, nbsClientCorrectCell, selectedClient)
	}

	mock.AssertExpectationsForObjects(t, nbsFactory, nbsClientCorrectCell, nbsClientEmptyCell)
}

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

////////////////////////////////////////////////////////////////////////////////

func TestCellSelectorSelectsCorrectCell(t *testing.T) {
	ctx := newContext()

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
	ctx := newContext()
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

func TestCellSelectorReturnsCorrectCellWithMaxFreeBytesPolicy(t *testing.T) {
	ctx := newContext()
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

func TestCellSelectorReturnsCorrectCellWithMaxFreeBytesPolicyIfNoCapacities(
	t *testing.T,
) {

	ctx := newContext()
	cellStorage := storage_mocks.NewStorageMock()

	policy := cells_config.CellSelectionPolicy_MAX_FREE_BYTES
	config := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			shardedZoneID: {Cells: []string{cellID1, cellID2}},
		},
		CellSelectionPolicy: &policy,
	}

	cellStorage.On(
		"GetRecentClusterCapacities",
		ctx,
		shardedZoneID,
		types.DiskKind_DISK_KIND_SSD,
	).Return([]storage.ClusterCapacity{}, nil)

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

////////////////////////////////////////////////////////////////////////////////

func TestSelectCellForLocalDiskReturnsCorrectNBSClient(t *testing.T) {
	ctx := context.Background()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient1 := nbs_mocks.NewClientMock()
	nbsClient2 := nbs_mocks.NewClientMock()
	config := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			"zone-a": {Cells: []string{"zone-a", "zone-a-cell1"}},
		},
	}
	agentIDs := []string{"agent1"}

	nbsFactory.On("GetClient", mock.Anything, "zone-a").Return(nbsClient1, nil)
	nbsFactory.On(
		"GetClient",
		mock.Anything,
		"zone-a-cell1",
	).Return(nbsClient2, nil)

	nbsClient1.On("QueryAvailableStorage", mock.Anything, agentIDs).Return(
		[]nbs.AvailableStorageInfo{
			{
				AgentID:    "agent1",
				ChunkSize:  4096,
				ChunkCount: 10,
			},
		},
		nil,
	)
	nbsClient2.On("QueryAvailableStorage", mock.Anything, agentIDs).Return(
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
		agentIDs,
	)
	require.NoError(t, err)
	require.Equal(t, nbsClient1, selectedClient)
	mock.AssertExpectationsForObjects(t, nbsFactory, nbsClient1, nbsClient2)
}

func TestSelectCellForLocalDiskShouldReturnErrorIfNoAvailableAgentsFound(
	t *testing.T,
) {

	ctx := context.Background()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient1 := nbs_mocks.NewClientMock()
	nbsClient2 := nbs_mocks.NewClientMock()
	config := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			"zone-a": {Cells: []string{"zone-a", "zone-a-cell1"}},
		},
	}
	agentIDs := []string{"agent1"}

	nbsFactory.On("GetClient", mock.Anything, "zone-a").Return(nbsClient1, nil)
	nbsFactory.On(
		"GetClient",
		mock.Anything, // ctx.
		"zone-a-cell1",
	).Return(nbsClient2, nil)

	// Agent is unavailable.
	nbsClient1.On("QueryAvailableStorage", mock.Anything, agentIDs).Return(
		[]nbs.AvailableStorageInfo{
			{
				AgentID:    "agent1",
				ChunkSize:  0,
				ChunkCount: 0,
			},
		},
		nil,
	)
	// No such agent in cell.
	nbsClient2.On("QueryAvailableStorage", mock.Anything, agentIDs).Return(
		[]nbs.AvailableStorageInfo{},
		nil,
	)

	cellSelector := cellSelector{
		config:     config,
		nbsFactory: nbsFactory,
	}

	selectedClient, err := cellSelector.SelectCellForLocalDisk(
		ctx,
		"zone-a",
		agentIDs,
	)
	require.Nil(t, selectedClient)
	require.Error(t, err)
	require.ErrorContains(
		t,
		err,
		"no cells with such agents in zone",
	)
	mock.AssertExpectationsForObjects(t, nbsFactory, nbsClient1, nbsClient2)
}

func TestSelectCellForLocalDiskCellReturnsAnError(t *testing.T) {
	testCases := []struct {
		name                       string
		correctCellResponseLatency time.Duration
		emptyCellResponseLatency   time.Duration
	}{
		{
			name:                       "responses are not ordered",
			correctCellResponseLatency: 0,
			emptyCellResponseLatency:   0,
		},
		{
			name:                       "valid response is faster",
			correctCellResponseLatency: 0,
			emptyCellResponseLatency:   10 * time.Second,
		},
		{
			name:                       "error response is faster",
			correctCellResponseLatency: 10 * time.Second,
			emptyCellResponseLatency:   0,
		},
	}

	for _, testCase := range testCases {
		for _, correctCellReturnsError := range []bool{false, true} {
			testCaseName := testCase.name
			if correctCellReturnsError {
				testCaseName += " correct cell returns error"
			}

			t.Run(testCaseName, func(t *testing.T) {
				testSelectCellForLocalDiskCellReturnsAnError(
					t,
					testCase.correctCellResponseLatency,
					testCase.emptyCellResponseLatency,
					correctCellReturnsError,
				)
			})
		}
	}
}
