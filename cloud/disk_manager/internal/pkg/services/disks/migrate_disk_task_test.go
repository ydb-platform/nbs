package disks

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	pools_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/mocks"
	pools_storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	"google.golang.org/protobuf/types/known/timestamppb"
)

////////////////////////////////////////////////////////////////////////////////

func TestMigrateDiskTaskProgress(t *testing.T) {
	ctx := context.Background()

	scheduler := tasks_mocks.NewSchedulerMock()
	poolService := pools_mocks.NewServiceMock()
	resourceStorage := storage_mocks.NewStorageMock()
	poolStorage := pools_storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()

	request := &protos.MigrateDiskRequest{
		Disk: &types.Disk{
			ZoneId: "zone",
			DiskId: t.Name(),
		},
		DstZoneId: "other",
	}

	task := &migrateDiskTask{
		scheduler:       scheduler,
		poolService:     poolService,
		resourceStorage: resourceStorage,
		poolStorage:     poolStorage,
		nbsFactory:      nbsFactory,
		request:         request,
		state:           &protos.MigrateDiskTaskState{},
	}

	metadataRaw, err := task.GetMetadata(ctx)
	require.NoError(t, err)

	metadata, ok := metadataRaw.(*disk_manager.MigrateDiskMetadata)
	require.True(t, ok)

	expectedMetadata := &disk_manager.MigrateDiskMetadata{
		Progress:         0,
		SecondsRemaining: math.MaxInt64,
	}

	require.Equal(t, expectedMetadata.Progress, metadata.Progress)
	require.Equal(t, expectedMetadata.SecondsRemaining, metadata.SecondsRemaining)
	require.Equal(t, expectedMetadata.UpdatedAt, metadata.UpdatedAt)

	task.state = &protos.MigrateDiskTaskState{
		ReplicateTaskID: "not_empty",
	}

	replicateDiskTaskProgress := 0.55
	replicateDiskTaskSecondsRemaining := int64(100)
	replicateDiskTaskUpdatedAt := timestamppb.New(time.Date(2023, 10, 5, 0, 0, 0, 0, time.UTC))

	scheduler.On(
		"GetTaskMetadata",
		mock.Anything,
		"not_empty",
	).Return(&dataplane_protos.ReplicateDiskTaskMetadata{
		Progress:         replicateDiskTaskProgress,
		SecondsRemaining: replicateDiskTaskSecondsRemaining,
		UpdatedAt:        replicateDiskTaskUpdatedAt,
	}, nil)

	metadataRaw, err = task.GetMetadata(ctx)
	require.NoError(t, err)

	metadata, ok = metadataRaw.(*disk_manager.MigrateDiskMetadata)
	require.True(t, ok)

	expectedMetadata = &disk_manager.MigrateDiskMetadata{
		Progress:         replicateDiskTaskProgress,
		SecondsRemaining: replicateDiskTaskSecondsRemaining,
		UpdatedAt:        replicateDiskTaskUpdatedAt,
	}

	require.Equal(t, expectedMetadata.Progress, metadata.Progress)
	require.Equal(t, expectedMetadata.SecondsRemaining, metadata.SecondsRemaining)
	require.Equal(t, expectedMetadata.UpdatedAt, metadata.UpdatedAt)

	mock.AssertExpectationsForObjects(t, scheduler)
}

func TestMigrateDiskTaskIncrementFillGenerationUsesSavedGuardMode(
	t *testing.T,
) {

	for _, test := range []struct {
		name              string
		preventConcurrent bool
		lockTaskID        string
	}{
		{"legacy", false, ""},
		{"guarded", true, "migration-task"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := logging.SetLogger(
				context.Background(),
				logging.NewStderrLogger(logging.DebugLevel),
			)
			resourceStorage := storage_mocks.NewStorageMock()
			exec := tasks_mocks.NewExecutionContextMock()
			exec.On("GetTaskID").Return("migration-task")
			resourceStorage.On(
				"IncrementFillGeneration", ctx, "disk", test.lockTaskID,
			).Return(uint64(7), nil).Once()
			task := &migrateDiskTask{
				resourceStorage: resourceStorage,
				request: &protos.MigrateDiskRequest{
					Disk: &types.Disk{
						DiskId: "disk",
						ZoneId: "source-cell",
					},
					PreventConcurrentDiskModifications: test.preventConcurrent,
				},
			}

			generation, err := task.incrementFillGeneration(ctx, exec)
			require.NoError(t, err)
			require.EqualValues(t, 7, generation)
			mock.AssertExpectationsForObjects(t, resourceStorage, exec)
		})
	}
}

func TestMigrateDiskTaskFinishesWithLockOwnerID(t *testing.T) {
	for _, test := range []struct {
		name              string
		preventConcurrent bool
		lockTaskID        string
	}{
		{"legacy", false, ""},
		{"guarded", true, "migration-task"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := logging.SetLogger(
				context.Background(),
				logging.NewStderrLogger(logging.DebugLevel),
			)
			resourceStorage := storage_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			nbsFactory := nbs_mocks.NewFactoryMock()
			source := nbs_mocks.NewClientMock()
			target := nbs_mocks.NewClientMock()
			exec := tasks_mocks.NewExecutionContextMock()
			exec.On("GetTaskID").Return("migration-task")
			exec.On("SaveState", ctx).Return(nil)
			task := &migrateDiskTask{
				resourceStorage: resourceStorage,
				scheduler:       scheduler,
				nbsFactory:      nbsFactory,
				request: &protos.MigrateDiskRequest{
					Disk: &types.Disk{
						DiskId: "disk",
						ZoneId: "source-cell",
					},
					DstZoneId:                          "target-cell",
					PreventConcurrentDiskModifications: test.preventConcurrent,
				},
				state: &protos.MigrateDiskTaskState{
					Status:         protos.MigrationStatus_Finishing,
					FillGeneration: 7,
					RelocateInfo:   &protos.RelocateInfo{},
				},
			}

			nbsFactory.On(
				"GetClient", ctx, "target-cell",
			).Return(target, nil).Twice()
			nbsFactory.On(
				"GetClient", ctx, "source-cell",
			).Return(source, nil).Once()
			target.On(
				"FinishFillDisk", ctx, mock.Anything, "disk", uint64(7),
			).Run(func(args mock.Arguments) {
				require.NoError(t, args.Get(1).(func() error)())
			}).Return(nil).Once()
			source.On("Delete", ctx, "disk").Return(nil).Once()
			target.On(
				"ModifyTags",
				ctx,
				mock.Anything,
				"disk",
				[]string{},
				[]string{"source-disk-id=source-cell_disk"},
			).Run(func(args mock.Arguments) {
				require.NoError(t, args.Get(1).(func() error)())
			}).Return(nil).Once()
			scheduler.On(
				"ScheduleTask",
				mock.Anything,
				"dataplane.DeleteDiskFromIncremental",
				"",
				&dataplane_protos.DeleteDiskFromIncrementalRequest{
					Disk: task.request.Disk,
				},
			).Return("incremental-cleanup-task", nil).Once()
			scheduler.On(
				"WaitTask", ctx, mock.Anything, "incremental-cleanup-task",
			).Return(nil, nil).Once()

			relocated := false
			resourceStorage.On(
				"DiskRelocated",
				ctx,
				mock.Anything,
				"disk",
				"source-cell",
				"target-cell",
				test.lockTaskID,
			).Run(func(_ mock.Arguments) {
				relocated = true
			}).Return(nil).Once()
			if test.preventConcurrent {
				resourceStorage.On(
					"UnlockDisk", ctx, mock.Anything, "disk", "migration-task",
				).Run(func(_ mock.Arguments) {
					require.True(t, relocated)
				}).Return(nil).Once()
			}

			exec.On(
				"FinishWithPreparation", ctx, mock.Anything,
			).Run(func(args mock.Arguments) {
				prepare := args.Get(1).(func(
					context.Context, *persistence.Transaction,
				) error)
				require.NoError(t, prepare(ctx, nil))
			}).Return(nil).Once()

			locked := &preparationLockedExecutionContext{
				ExecutionContext: exec,
				t:                t,
			}

			var execution tasks.ExecutionContext = locked
			if test.preventConcurrent {
				guard := newDiskTaskWithLocking(
					task, resourceStorage,
				).(*diskTaskWithLocking)
				execution = &diskOperationExecutionContext{
					ExecutionContext: locked,
					task:             guard,
				}
			}

			require.NoError(t, task.finishMigration(ctx, execution))
			if !test.preventConcurrent {
				resourceStorage.AssertNotCalled(
					t, "UnlockDisk", mock.Anything, mock.Anything,
					mock.Anything, mock.Anything,
				)
			}

			mock.AssertExpectationsForObjects(
				t, resourceStorage, scheduler, nbsFactory, source, target, exec,
			)
		})
	}
}
