package pools

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	pools_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestRetireBaseDiskTaskUsesExplicitOwnSource(t *testing.T) {
	ctx := newContext()
	s := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("retire")

	request, err := proto.Marshal(&protos.RetireBaseDiskRequest{
		BaseDiskId:       "base",
		UseBaseDiskAsSrc: true,
	})
	require.NoError(t, err)

	task := &retireBaseDiskTask{storage: s, scheduler: scheduler}
	require.NoError(t, task.Load(request, nil))
	s.On("RetireBaseDiskUsingBaseDiskAsSource", ctx, "base", uint64(0)).
		Return([]pools_storage.RebaseInfo{}, nil).Once()
	s.On("IsBaseDiskRetired", ctx, "base").Return(true, nil).Once()

	require.NoError(t, task.Run(ctx, execCtx))
	mock.AssertExpectationsForObjects(t, s, scheduler, execCtx)
}

func TestRetireBaseDiskTaskPreservesLegacySource(t *testing.T) {
	for _, testCase := range []struct {
		name string
		src  *types.Disk
	}{
		{name: "image"},
		{name: "explicit_disk", src: &types.Disk{
			ZoneId: "source_zone",
			DiskId: "source_disk",
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := newContext()
			s := storage_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			execCtx.On("GetTaskID").Return("retire")

			request, err := proto.Marshal(&protos.RetireBaseDiskRequest{
				BaseDiskId: "base",
				SrcDisk:    testCase.src,
			})
			require.NoError(t, err)
			task := &retireBaseDiskTask{storage: s, scheduler: scheduler}
			require.NoError(t, task.Load(request, nil))

			s.On("RetireBaseDisk", ctx, "base",
				mock.MatchedBy(func(src *types.Disk) bool {
					if testCase.src == nil {
						return src == nil
					}
					return src != nil &&
						src.ZoneId == testCase.src.ZoneId &&
						src.DiskId == testCase.src.DiskId
				}), uint64(0),
			).Return([]pools_storage.RebaseInfo{}, nil).Once()
			s.On("IsBaseDiskRetired", ctx, "base").Return(true, nil).Once()

			require.NoError(t, task.Run(ctx, execCtx))
			mock.AssertExpectationsForObjects(t, s, scheduler, execCtx)
		})
	}
}

func TestRetireBaseDiskTaskPropagatesOwnSourceError(t *testing.T) {
	ctx := newContext()
	s := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("retire")

	task := &retireBaseDiskTask{
		storage: s, scheduler: scheduler,
		request: &protos.RetireBaseDiskRequest{
			BaseDiskId: "base", UseBaseDiskAsSrc: true,
		},
	}
	s.On("RetireBaseDiskUsingBaseDiskAsSource", ctx, "base", uint64(0)).
		Return([]pools_storage.RebaseInfo{}, task_errors.NewNonRetriableErrorf(
			"UseBaseDiskAsSrc requires HoldBaseDisksWithInflightDependents=true",
		)).Once()

	err := task.Run(ctx, execCtx)
	require.ErrorContains(t, err, "HoldBaseDisksWithInflightDependents")
	require.False(t, task_errors.CanRetry(err))
	mock.AssertExpectationsForObjects(t, s, scheduler, execCtx)
}

func TestRetireBaseDiskTaskRejectsConflictingSources(t *testing.T) {
	ctx := newContext()
	s := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("retire")

	task := &retireBaseDiskTask{
		storage: s, scheduler: scheduler,
		request: &protos.RetireBaseDiskRequest{
			BaseDiskId: "base", UseBaseDiskAsSrc: true,
			SrcDisk: &types.Disk{ZoneId: "zone", DiskId: "other"},
		},
	}
	err := task.Run(ctx, execCtx)
	require.ErrorContains(t, err, "cannot both be specified")
	require.False(t, task_errors.CanRetry(err))
	mock.AssertExpectationsForObjects(t, s, scheduler, execCtx)
}
