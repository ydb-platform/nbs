package pools

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	pools_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestDeleteBaseDisksTaskDisabledZones(t *testing.T) {
	disabledZone := common.NewDisabledZones([]string{"zone-d"})
	disabledShard := common.NewDisabledZones([]string{"zone-d-shard1"})
	disabledDisk := pools_storage.BaseDisk{ID: "disabled", ZoneID: "zone-d"}
	allowedDisk := pools_storage.BaseDisk{ID: "allowed", ZoneID: "zone-a"}
	shardDisk := pools_storage.BaseDisk{ID: "shard", ZoneID: "zone-d-shard1"}

	testCases := []struct {
		name          string
		baseDisks     []pools_storage.BaseDisk
		disabledZones *common.DisabledZones
		nbsDisks      []pools_storage.BaseDisk
		errorAt       string
	}{
		{
			name:          "disabled zone",
			baseDisks:     []pools_storage.BaseDisk{disabledDisk},
			disabledZones: disabledZone,
		},
		{
			name:          "mixed batch",
			baseDisks:     []pools_storage.BaseDisk{disabledDisk, allowedDisk},
			disabledZones: disabledZone,
			nbsDisks:      []pools_storage.BaseDisk{allowedDisk},
		},
		{
			name:          "disabled shard",
			baseDisks:     []pools_storage.BaseDisk{shardDisk},
			disabledZones: disabledShard,
		},
		{
			name:          "region does not disable shard",
			baseDisks:     []pools_storage.BaseDisk{shardDisk},
			disabledZones: disabledZone,
			nbsDisks:      []pools_storage.BaseDisk{shardDisk},
		},
		{
			name:          "shard does not disable region",
			baseDisks:     []pools_storage.BaseDisk{disabledDisk},
			disabledZones: disabledShard,
			nbsDisks:      []pools_storage.BaseDisk{disabledDisk},
		},
		{
			name:      "missing policy",
			baseDisks: []pools_storage.BaseDisk{disabledDisk},
			nbsDisks:  []pools_storage.BaseDisk{disabledDisk},
		},
		{
			name:          "empty policy",
			baseDisks:     []pools_storage.BaseDisk{disabledDisk},
			disabledZones: common.NewDisabledZones(nil),
			nbsDisks:      []pools_storage.BaseDisk{disabledDisk},
		},
		{
			name:          "empty batch",
			baseDisks:     []pools_storage.BaseDisk{},
			disabledZones: disabledZone,
		},
		{
			name:          "nbs client error in allowed zone",
			baseDisks:     []pools_storage.BaseDisk{disabledDisk, allowedDisk},
			disabledZones: disabledZone,
			nbsDisks:      []pools_storage.BaseDisk{allowedDisk},
			errorAt:       "GetClient",
		},
		{
			name:          "nbs deletion error in allowed zone",
			baseDisks:     []pools_storage.BaseDisk{disabledDisk, allowedDisk},
			disabledZones: disabledZone,
			nbsDisks:      []pools_storage.BaseDisk{allowedDisk},
			errorAt:       "Delete",
		},
		{
			name:          "mark deleted error in disabled zone",
			baseDisks:     []pools_storage.BaseDisk{disabledDisk},
			disabledZones: disabledZone,
			errorAt:       "BaseDisksDeleted",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newContext()
			storage := storage_mocks.NewStorageMock()
			nbsFactory := nbs_mocks.NewFactoryMock()
			nbsClient := nbs_mocks.NewClientMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			task := &deleteBaseDisksTask{
				storage:       storage,
				nbsFactory:    nbsFactory,
				disabledZones: tc.disabledZones,
				limit:         10,
			}

			errorFor := func(step string) error {
				if tc.errorAt == step {
					return assert.AnError
				}
				return nil
			}
			setupExpectations := func() {
				storage.On("GetBaseDisksToDelete", ctx, uint64(10)).
					Return(tc.baseDisks, nil).Once()
				for _, disk := range tc.nbsDisks {
					nbsFactory.On("GetClient", ctx, disk.ZoneID).
						Return(nbsClient, errorFor("GetClient")).Once()
					if tc.errorAt == "GetClient" {
						return
					}
					nbsClient.On("Delete", ctx, disk.ID).
						Return(errorFor("Delete")).Once()
					if tc.errorAt == "Delete" {
						return
					}
				}
				storage.On("BaseDisksDeleted", ctx, tc.baseDisks).
					Return(errorFor("BaseDisksDeleted")).Once()
			}
			setupExpectations()

			err := task.Run(ctx, execCtx)
			if tc.errorAt != "" {
				require.True(t, errors.Is(err, assert.AnError))
				storage.AssertNotCalled(t, "BaseDisksDeleted", mock.Anything, mock.Anything)
			} else {
				require.NoError(t, err)
			}
			if len(tc.nbsDisks) == 0 {
				nbsFactory.AssertNotCalled(t, "GetClient", mock.Anything, mock.Anything)
				nbsClient.AssertNotCalled(t, "Delete", mock.Anything, mock.Anything)
			}
			mock.AssertExpectationsForObjects(t, storage, nbsFactory, nbsClient, execCtx)
		})
	}
}

func TestDeleteBaseDisksTaskUnknownZoneIsNotSkipped(t *testing.T) {
	ctx := newContext()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	task := &deleteBaseDisksTask{
		storage:       storage,
		nbsFactory:    nbsFactory,
		disabledZones: common.NewDisabledZones([]string{"zone-d"}),
		limit:         10,
	}

	unknownZoneErr := errors.NewNonRetriableErrorf("unknown zone %q", "zone-b")
	baseDisks := []pools_storage.BaseDisk{{ID: "base", ZoneID: "zone-b"}}
	storage.On("GetBaseDisksToDelete", ctx, uint64(10)).Return(baseDisks, nil).Once()
	nbsFactory.On("GetClient", ctx, "zone-b").Return(nil, unknownZoneErr).Once()

	err := task.Run(ctx, execCtx)
	require.Error(t, err)
	require.True(t, errors.Is(err, unknownZoneErr))
	storage.AssertNotCalled(t, "BaseDisksDeleted", mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, storage, nbsFactory, execCtx)
}
