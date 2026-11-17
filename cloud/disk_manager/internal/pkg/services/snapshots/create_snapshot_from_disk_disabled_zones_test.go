package snapshots

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	cells_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	internal_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/codes"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type snapshotCreationTestEnv struct {
	ctx          context.Context
	task         *createSnapshotFromDiskTask
	storage      *resources_mocks.StorageMock
	factory      *nbs_mocks.FactoryMock
	selector     *cells_mocks.CellSelectorMock
	scheduler    *tasks_mocks.SchedulerMock
	execCtx      *tasks_mocks.ExecutionContextMock
	resolvedDisk *types.Disk
}

func newSnapshotCreationTestEnv(
	t *testing.T,
	disabledZones *internal_common.DisabledZones,
	resolvedZone string,
	resumed bool,
) *snapshotCreationTestEnv {

	t.Helper()

	env := &snapshotCreationTestEnv{
		ctx: logging.SetLogger(
			context.Background(),
			logging.NewStderrLogger(logging.WarnLevel),
		),
		storage:   resources_mocks.NewStorageMock(),
		factory:   nbs_mocks.NewFactoryMock(),
		selector:  cells_mocks.NewCellSelectorMock(),
		scheduler: tasks_mocks.NewSchedulerMock(),
		execCtx:   tasks_mocks.NewExecutionContextMock(),
		resolvedDisk: &types.Disk{
			ZoneId: resolvedZone,
			DiskId: "disk",
		},
	}

	env.task = &createSnapshotFromDiskTask{
		disabledZones: disabledZones,
		scheduler:     env.scheduler,
		storage:       env.storage,
		nbsFactory:    env.factory,
		cellSelector:  env.selector,
	}

	request, err := proto.Marshal(&protos.CreateSnapshotFromDiskRequest{
		SrcDisk: &types.Disk{
			ZoneId: "zone-a",
			DiskId: "disk",
		},
		DstSnapshotId:                    "snapshot",
		UseProxyOverlayDisk:              true,
		RetryBrokenDRBasedDiskCheckpoint: true,
	})
	require.NoError(t, err)

	state := &protos.CreateSnapshotFromDiskTaskState{}
	if resumed {
		state.DataplaneTaskID = "transfer"
		state.Progress = 0.5
		state.Offset = 2
		state.SnapshotSize = 4096
		state.SnapshotStorageSize = 2048
	}

	savedState, err := proto.Marshal(state)
	require.NoError(t, err)

	err = env.task.Load(request, savedState)
	require.NoError(t, err)

	t.Cleanup(func() {
		env.selector.AssertExpectations(t)
		env.storage.AssertExpectations(t)
		env.factory.AssertExpectations(t)
		env.scheduler.AssertExpectations(t)
		env.execCtx.AssertExpectations(t)
	})

	return env
}

func (env *snapshotCreationTestEnv) expectDiskLocationChecks(count int) {
	if env.task.disabledZones != nil {
		env.storage.On("GetDiskMeta", env.ctx, "disk").Return(
			&resources.DiskMeta{ID: "disk", ZoneID: env.resolvedDisk.ZoneId},
			nil,
		).Times(count)
	}

	if !env.task.disabledZones.IsDisabled(env.resolvedDisk.ZoneId) {
		env.selector.On(
			"ReplaceZoneIdWithCellIdInDiskMeta",
			env.ctx,
			env.storage,
			env.task.request.SrcDisk,
		).Return(env.resolvedDisk, nil).Times(count)
	}
}

func (env *snapshotCreationTestEnv) expectDeleteSnapshot(err error) {
	env.scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.DeleteSnapshot",
		"",
		mock.MatchedBy(func(request *dataplane_protos.DeleteSnapshotRequest) bool {
			return proto.Equal(request, &dataplane_protos.DeleteSnapshotRequest{
				SnapshotId: "snapshot",
			})
		}),
	).Return("delete", err).Once()
}

func requireSnapshotZoneDisabled(t *testing.T, err error) {
	t.Helper()

	var nonRetriable *errors.NonRetriableError
	require.ErrorAs(t, err, &nonRetriable)

	var detailed *errors.DetailedError
	require.ErrorAs(t, err, &detailed)
	require.Equal(t, &errors.ErrorDetails{
		Code:     codes.Aborted,
		Message:  `can't create snapshot "snapshot" from disk "disk": source zone is disabled`,
		Internal: true,
	}, detailed.Details)

	var taskState tasks_storage.TaskState
	taskState.SetError(err)
	require.Equal(t, detailed.Details, taskState.ErrorDetails)
}

// Checks that a disabled resolved zone aborts new and resumed tasks before NBS or metadata changes.
func TestCreateSnapshotDisabledZone(t *testing.T) {
	testCases := []struct {
		name         string
		resolvedZone string
		resumed      bool
	}{
		// A disabled region rejects a new task before acquiring an NBS client.
		{
			name:         "new regional task",
			resolvedZone: "zone-a",
		},
		// A disabled actual cell rejects a task whose request contains the region.
		{
			name:         "new cell task",
			resolvedZone: "zone-a-shard1",
		},
		// Saved transfer state does not bypass the disabled-cell check.
		{
			name:         "resumed cell task",
			resolvedZone: "zone-a-shard1",
			resumed:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := newSnapshotCreationTestEnv(
				t,
				internal_common.NewDisabledZones([]string{tc.resolvedZone}),
				tc.resolvedZone,
				tc.resumed,
			)

			stateBefore, err := env.task.Save()
			require.NoError(t, err)

			env.expectDiskLocationChecks(1)

			err = env.task.Run(env.ctx, env.execCtx)

			requireSnapshotZoneDisabled(t, err)

			stateAfter, err := env.task.Save()
			require.NoError(t, err)
			require.Equal(t, stateBefore, stateAfter)
			require.Empty(t, env.factory.Calls)
			env.storage.AssertNumberOfCalls(t, "GetDiskMeta", 1)
			require.Len(t, env.storage.Calls, 1)
			require.Empty(t, env.scheduler.Calls)
			require.Empty(t, env.execCtx.Calls)
		})
	}
}

// Checks that allowed resolved zones preserve NBS client errors in Run and Cancel.
func TestCreateSnapshotAllowedZones(t *testing.T) {
	testCases := []struct {
		name          string
		disabledZones *internal_common.DisabledZones
	}{
		// A nil policy preserves client acquisition in the actual cell.
		{
			name: "nil policy",
		},
		// Disabling another region does not block the actual cell.
		{
			name: "another region",
			disabledZones: internal_common.NewDisabledZones(
				[]string{"zone-b"},
			),
		},
		// Matching the request's region does not match its resolved cell.
		{
			name: "request region only",
			disabledZones: internal_common.NewDisabledZones(
				[]string{"zone-a"},
			),
		},
		// Disabling a sibling cell does not block the actual cell.
		{
			name: "sibling cell",
			disabledZones: internal_common.NewDisabledZones(
				[]string{"zone-a-shard2"},
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := newSnapshotCreationTestEnv(
				t,
				tc.disabledZones,
				"zone-a-shard1",
				false,
			)
			expectedErr := fmt.Errorf("client acquisition failed")
			metadataReads := 0
			if tc.disabledZones != nil {
				metadataReads = 2
			}

			env.expectDiskLocationChecks(2)
			env.execCtx.On("GetTaskID").Return("create").Once()
			env.factory.On(
				"GetClient",
				env.ctx,
				"zone-a-shard1",
			).Return(nil, expectedErr).Twice()

			runErr := env.task.Run(env.ctx, env.execCtx)
			cancelErr := env.task.Cancel(env.ctx, env.execCtx)

			require.Same(t, expectedErr, runErr)
			require.Same(t, expectedErr, cancelErr)
			env.storage.AssertNumberOfCalls(t, "GetDiskMeta", metadataReads)
			require.Len(t, env.storage.Calls, metadataReads)
			require.Empty(t, env.scheduler.Calls)
		})
	}
}

// Checks that disabled-zone cancellation skips NBS while retaining metadata and dataplane cleanup.
func TestCreateSnapshotDisabledZoneCleanup(t *testing.T) {
	testCases := []struct {
		name         string
		resumed      bool
		missing      bool
		deleteTaskID string
	}{
		// A task without saved transfer state still cleans up existing metadata.
		{
			name:         "new task",
			deleteTaskID: "create",
		},
		// A resumed task performs the same cleanup without consulting NBS.
		{
			name:         "resumed task",
			resumed:      true,
			deleteTaskID: "create",
		},
		// Missing metadata ends cancellation without scheduling deletion.
		{
			name:    "missing metadata",
			missing: true,
		},
		// Existing deletion by another task is awaited without starting another.
		{
			name:         "existing deletion",
			deleteTaskID: "other-delete",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := newSnapshotCreationTestEnv(
				t,
				internal_common.NewDisabledZones([]string{"zone-a-shard1"}),
				"zone-a-shard1",
				tc.resumed,
			)

			stateBefore, err := env.task.Save()
			require.NoError(t, err)

			var meta *resources.SnapshotMeta
			if !tc.missing {
				meta = &resources.SnapshotMeta{DeleteTaskID: tc.deleteTaskID}
			}

			env.expectDiskLocationChecks(1)
			env.execCtx.On("GetTaskID").Return("create").Once()
			env.storage.On(
				"DeleteSnapshot", env.ctx, "snapshot", "create", mock.Anything,
			).Return(meta, nil).Once()

			if tc.deleteTaskID == "create" {
				env.expectDeleteSnapshot(nil)
				env.scheduler.On(
					"WaitTask", env.ctx, env.execCtx, "delete",
				).Return(nil, nil).Once()
				env.storage.On(
					"SnapshotDeleted", env.ctx, "snapshot", mock.Anything,
				).Return(nil).Once()
			} else if !tc.missing {
				env.scheduler.On(
					"WaitTaskEnded", env.ctx, tc.deleteTaskID,
				).Return(nil).Once()
			}

			err = env.task.Cancel(env.ctx, env.execCtx)

			require.NoError(t, err)
			require.Empty(t, env.factory.Calls)

			stateAfter, err := env.task.Save()
			require.NoError(t, err)
			require.Equal(t, stateBefore, stateAfter)
		})
	}
}

// Checks that disabled-zone cancellation preserves errors from each remaining cleanup stage.
func TestCreateSnapshotCleanupErrors(t *testing.T) {
	testCases := []struct {
		name  string
		stage int
	}{
		// Metadata deletion failure prevents scheduling dataplane deletion.
		{
			name:  "delete metadata",
			stage: 0,
		},
		// Scheduling failure prevents waiting or finalizing metadata.
		{
			name:  "schedule deletion",
			stage: 1,
		},
		// A dataplane deletion error prevents finalizing metadata.
		{
			name:  "wait for deletion",
			stage: 2,
		},
		// Final metadata failure remains visible after dataplane deletion.
		{
			name:  "finalize metadata",
			stage: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := newSnapshotCreationTestEnv(
				t,
				internal_common.NewDisabledZones([]string{"zone-a-shard1"}),
				"zone-a-shard1",
				true,
			)
			expectedErr := fmt.Errorf("cleanup failed")
			stageErrors := make([]error, 4)
			stageErrors[tc.stage] = expectedErr

			env.expectDiskLocationChecks(1)
			env.execCtx.On("GetTaskID").Return("create").Once()
			env.storage.On(
				"DeleteSnapshot", env.ctx, "snapshot", "create", mock.Anything,
			).Return(
				&resources.SnapshotMeta{DeleteTaskID: "create"}, stageErrors[0],
			).Once()

			if tc.stage >= 1 {
				env.expectDeleteSnapshot(stageErrors[1])
			}

			if tc.stage >= 2 {
				env.scheduler.On(
					"WaitTask", env.ctx, env.execCtx, "delete",
				).Return(nil, stageErrors[2]).Once()
			}

			if tc.stage >= 3 {
				env.storage.On(
					"SnapshotDeleted", env.ctx, "snapshot", mock.Anything,
				).Return(stageErrors[3]).Once()
			}

			err := env.task.Cancel(env.ctx, env.execCtx)

			require.Same(t, expectedErr, err)
			require.Empty(t, env.factory.Calls)
		})
	}
}

// Checks that allowed-zone cancellation still deletes the checkpoint and returns its NBS error.
func TestCreateSnapshotCheckpointCleanup(t *testing.T) {
	testCases := []struct {
		name          string
		disabledZones *internal_common.DisabledZones
	}{
		// A nil policy preserves checkpoint cleanup in the actual cell.
		{
			name: "nil policy",
		},
		// A disabled request region does not suppress cleanup in its allowed cell.
		{
			name: "allowed cell",
			disabledZones: internal_common.NewDisabledZones(
				[]string{"zone-a"},
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := newSnapshotCreationTestEnv(
				t,
				tc.disabledZones,
				"zone-a-shard1",
				false,
			)
			client := nbs_mocks.NewClientMock()
			expectedErr := fmt.Errorf("checkpoint deletion failed")
			metadataReads := 0
			if tc.disabledZones != nil {
				metadataReads = 1
			}

			env.expectDiskLocationChecks(1)
			env.factory.On(
				"GetClient", env.ctx, "zone-a-shard1",
			).Return(client, nil).Once()
			env.execCtx.On("GetTaskID").Return("create").Once()
			client.On(
				"Describe", env.ctx, "disk",
			).Return(nbs.DiskParams{}, nil).Once()
			client.On(
				"DeleteCheckpoint", env.ctx, "disk", "snapshot",
			).Return(expectedErr).Once()

			err := env.task.Cancel(env.ctx, env.execCtx)

			require.Same(t, expectedErr, err)
			env.storage.AssertNumberOfCalls(t, "GetDiskMeta", metadataReads)
			require.Len(t, env.storage.Calls, metadataReads)
			require.Empty(t, env.scheduler.Calls)
			client.AssertExpectations(t)
		})
	}
}

// Checks disabled-zone rejection and cleanup regardless of cells configuration,
// using the request zone only when source metadata is missing.
func TestCreateSnapshotDisabledZoneResolution(t *testing.T) {
	missingCellConfig := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			"zone-a": {Cells: []string{"zone-a-shard2"}},
		},
	}
	missingRegionConfig := &cells_config.CellsConfig{}

	testCases := []struct {
		name         string
		config       *cells_config.CellsConfig
		disabledZone string
		resumed      bool
		missingDisk  bool
	}{
		// A missing cell mapping cannot block rejection or cleanup of a new task.
		{
			name:         "new task missing cell",
			config:       missingCellConfig,
			disabledZone: "zone-a-shard1",
		},
		// A resumed task also bypasses a missing cell mapping during cleanup.
		{
			name:         "resumed task missing cell",
			config:       missingCellConfig,
			disabledZone: "zone-a-shard1",
			resumed:      true,
		},
		// A missing region mapping cannot block rejection or cleanup of a new task.
		{
			name:         "new task missing region",
			config:       missingRegionConfig,
			disabledZone: "zone-a-shard1",
		},
		// A resumed task also bypasses a missing region mapping during cleanup.
		{
			name:         "resumed task missing region",
			config:       missingRegionConfig,
			disabledZone: "zone-a-shard1",
			resumed:      true,
		},
		// Without cells configuration, the stored cell still overrides the request zone.
		{
			name:         "new task without cells",
			disabledZone: "zone-a-shard1",
		},
		// A resumed task also checks the stored cell without cells configuration.
		{
			name:         "resumed task without cells",
			disabledZone: "zone-a-shard1",
			resumed:      true,
		},
		// Missing metadata uses the disabled request zone before mapping checks.
		{
			name:         "new task missing disk",
			config:       missingRegionConfig,
			disabledZone: "zone-a",
			missingDisk:  true,
		},
		// A resumed task still completes cleanup when its source metadata is missing.
		{
			name:         "resumed task missing disk",
			config:       missingRegionConfig,
			disabledZone: "zone-a",
			resumed:      true,
			missingDisk:  true,
		},
		// The request-zone fallback also works without cells configuration.
		{
			name:         "new task missing disk without cells",
			disabledZone: "zone-a",
			missingDisk:  true,
		},
		// A resumed no-cells task uses the same fallback and completes cleanup.
		{
			name:         "resumed task missing disk without cells",
			disabledZone: "zone-a",
			resumed:      true,
			missingDisk:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := newSnapshotCreationTestEnv(
				t,
				internal_common.NewDisabledZones([]string{tc.disabledZone}),
				tc.disabledZone,
				tc.resumed,
			)
			env.task.cellSelector = cells.NewCellSelector(tc.config, nil, nil, nil)

			stateBefore, err := env.task.Save()
			require.NoError(t, err)

			var diskMeta *resources.DiskMeta
			if !tc.missingDisk {
				diskMeta = &resources.DiskMeta{ID: "disk", ZoneID: "zone-a-shard1"}
			}

			env.storage.On("GetDiskMeta", env.ctx, "disk").Return(diskMeta, nil).Twice()
			env.execCtx.On("GetTaskID").Return("create").Once()
			env.storage.On(
				"DeleteSnapshot", env.ctx, "snapshot", "create", mock.Anything,
			).Return(&resources.SnapshotMeta{DeleteTaskID: "create"}, nil).Once()

			env.expectDeleteSnapshot(nil)
			env.scheduler.On(
				"WaitTask", env.ctx, env.execCtx, "delete",
			).Return(nil, nil).Once()
			env.storage.On(
				"SnapshotDeleted", env.ctx, "snapshot", mock.Anything,
			).Return(nil).Once()

			runErr := env.task.Run(env.ctx, env.execCtx)
			cancelErr := env.task.Cancel(env.ctx, env.execCtx)

			requireSnapshotZoneDisabled(t, runErr)
			require.NoError(t, cancelErr)
			env.storage.AssertNumberOfCalls(t, "GetDiskMeta", 2)
			require.Empty(t, env.factory.Calls)
			require.Empty(t, env.selector.Calls)

			stateAfter, err := env.task.Save()
			require.NoError(t, err)
			require.Equal(t, stateBefore, stateAfter)
		})
	}
}

// Checks allowed-zone validation, metadata read counts, and the no-cells path with the real selector.
func TestCreateSnapshotAllowedZoneResolution(t *testing.T) {
	validConfig := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			"zone-a": {Cells: []string{"zone-a-shard1"}},
		},
	}
	missingCellConfig := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			"zone-a": {Cells: []string{"zone-a-shard2"}},
		},
	}
	policy := internal_common.NewDisabledZones([]string{"zone-b"})

	testCases := []struct {
		name          string
		config        *cells_config.CellsConfig
		disabledZones *internal_common.DisabledZones
		metadataReads int
		invalidZone   bool
		missingDisk   bool
	}{
		// A nil policy retains one metadata read per action in the old resolver.
		{
			name:          "nil policy",
			config:        validConfig,
			metadataReads: 2,
		},
		// An enabled zone is read once for the policy and again by the old resolver.
		{
			name:          "enabled policy",
			config:        validConfig,
			disabledZones: policy,
			metadataReads: 4,
		},
		// Without a policy, an absent cell mapping keeps the original validation error.
		{
			name:          "nil policy missing cell",
			config:        missingCellConfig,
			metadataReads: 2,
			invalidZone:   true,
		},
		// An allowed cell absent from the mapping still fails in the old resolver.
		{
			name:          "enabled policy missing cell",
			config:        missingCellConfig,
			disabledZones: policy,
			metadataReads: 4,
			invalidZone:   true,
		},
		// An allowed cell with no region mapping also keeps the validation error.
		{
			name:          "enabled policy missing region",
			config:        &cells_config.CellsConfig{},
			disabledZones: policy,
			metadataReads: 4,
			invalidZone:   true,
		},
		// Without cells, the policy reads metadata while NBS still uses the request zone.
		{
			name:          "enabled policy without cells",
			disabledZones: policy,
			metadataReads: 2,
		},
		// Missing metadata falls back to an allowed request zone and reaches NBS.
		{
			name:          "missing disk without cells",
			disabledZones: policy,
			metadataReads: 2,
			missingDisk:   true,
		},
		// Nil policy and configuration retain the original path without metadata reads.
		{
			name: "nil policy without cells",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := newSnapshotCreationTestEnv(
				t,
				tc.disabledZones,
				"zone-a-shard1",
				false,
			)
			env.task.cellSelector = cells.NewCellSelector(tc.config, nil, nil, nil)
			expectedErr := fmt.Errorf("client acquisition failed")

			var diskMeta *resources.DiskMeta
			if !tc.missingDisk {
				diskMeta = &resources.DiskMeta{ID: "disk", ZoneID: "zone-a-shard1"}
			}

			if tc.metadataReads != 0 {
				env.storage.On("GetDiskMeta", env.ctx, "disk").Return(
					diskMeta, nil,
				).Times(tc.metadataReads)
			}

			env.execCtx.On("GetTaskID").Return("create").Once()
			if !tc.invalidZone {
				zoneID := "zone-a-shard1"
				if tc.config == nil {
					zoneID = "zone-a"
				}

				env.factory.On(
					"GetClient", env.ctx, zoneID,
				).Return(nil, expectedErr).Twice()
			}

			runErr := env.task.Run(env.ctx, env.execCtx)
			cancelErr := env.task.Cancel(env.ctx, env.execCtx)

			if tc.invalidZone {
				var runError *errors.NonCancellableError
				require.ErrorAs(t, runErr, &runError)
				require.Equal(t, "disk disk is not in zone zone-a", runError.Err.Error())

				var cancelError *errors.NonCancellableError
				require.ErrorAs(t, cancelErr, &cancelError)
				require.Equal(t, runError.Err.Error(), cancelError.Err.Error())
				require.Empty(t, env.factory.Calls)
			} else {
				require.Same(t, expectedErr, runErr)
				require.Same(t, expectedErr, cancelErr)
			}

			env.storage.AssertNumberOfCalls(t, "GetDiskMeta", tc.metadataReads)
			require.Len(t, env.storage.Calls, tc.metadataReads)
			require.Empty(t, env.scheduler.Calls)
			require.Empty(t, env.selector.Calls)
		})
	}
}

// Checks that metadata errors propagate and an allowed fallback retains the old
// resolver's missing-disk error when cells are configured.
func TestCreateSnapshotSourceMetadataErrors(t *testing.T) {
	requestZoneDisabled := internal_common.NewDisabledZones([]string{"zone-a"})

	testCases := []struct {
		name          string
		config        *cells_config.CellsConfig
		disabledZones *internal_common.DisabledZones
		missing       bool
		metadataReads int
	}{
		// A read error is returned even when the request zone is disabled.
		{
			name:          "policy lookup error",
			config:        &cells_config.CellsConfig{},
			disabledZones: requestZoneDisabled,
			metadataReads: 2,
		},
		// Without cells, a read error must not fall back to the disabled request zone.
		{
			name:          "policy lookup error without cells",
			disabledZones: requestZoneDisabled,
			metadataReads: 2,
		},
		// An allowed request fallback still reaches the old resolver's missing-disk check.
		{
			name:          "policy lookup missing disk",
			config:        &cells_config.CellsConfig{},
			disabledZones: internal_common.NewDisabledZones([]string{"zone-a-shard1"}),
			missing:       true,
			metadataReads: 4,
		},
		// A nil policy preserves the old resolver's metadata error.
		{
			name:          "nil policy lookup error",
			config:        &cells_config.CellsConfig{},
			metadataReads: 2,
		},
		// A nil policy preserves the old resolver's missing-disk error.
		{
			name:          "nil policy lookup missing disk",
			config:        &cells_config.CellsConfig{},
			missing:       true,
			metadataReads: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := newSnapshotCreationTestEnv(
				t,
				tc.disabledZones,
				"zone-a-shard1",
				false,
			)
			env.task.cellSelector = cells.NewCellSelector(tc.config, nil, nil, nil)
			var expectedErr error
			if !tc.missing {
				expectedErr = fmt.Errorf("source metadata unavailable")
			}
			reachesResolver := tc.disabledZones == nil || tc.missing

			env.storage.On("GetDiskMeta", env.ctx, "disk").Return(
				(*resources.DiskMeta)(nil), expectedErr,
			).Times(tc.metadataReads)

			if reachesResolver {
				env.execCtx.On("GetTaskID").Return("create").Once()
			}

			runErr := env.task.Run(env.ctx, env.execCtx)
			cancelErr := env.task.Cancel(env.ctx, env.execCtx)

			if tc.missing {
				var runError *errors.NonCancellableError
				require.ErrorAs(t, runErr, &runError)
				require.Equal(t, "no such disk: disk", runError.Err.Error())

				var cancelError *errors.NonCancellableError
				require.ErrorAs(t, cancelErr, &cancelError)
				require.Equal(t, runError.Err.Error(), cancelError.Err.Error())
			} else {
				require.Same(t, expectedErr, runErr)
				require.Same(t, expectedErr, cancelErr)
			}

			env.storage.AssertNumberOfCalls(t, "GetDiskMeta", tc.metadataReads)
			require.Len(t, env.storage.Calls, tc.metadataReads)
			require.Empty(t, env.factory.Calls)
			require.Empty(t, env.scheduler.Calls)
			require.Empty(t, env.selector.Calls)
			if !reachesResolver {
				require.Empty(t, env.execCtx.Calls)
			}
		})
	}
}
