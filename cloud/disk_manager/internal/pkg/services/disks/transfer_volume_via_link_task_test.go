package disks

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	cell_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/operation"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
)

func volumeLinkTaskRequest(sameCell bool) *protos.TransferVolumeViaLinkRequest {
	request := &protos.TransferVolumeViaLinkRequest{
		LogicalDiskId: "leader",
		DstCellId:     "target-cell",
	}
	if sameCell {
		request.DstCellId = "source-cell"
	}
	return request
}

type volumeTransferTest struct {
	t         *testing.T
	ctx       context.Context
	factory   *nbs_mocks.FactoryMock
	source    *nbs_mocks.ClientMock
	storage   *storage_mocks.StorageMock
	scheduler *tasks_mocks.SchedulerMock
	exec      *tasks_mocks.ExecutionContextMock
	task      *transferVolumeViaLinkTask
	link      nbs.VolumeLink
	selector  *cell_mocks.CellSelectorMock
}

func newVolumeTransferTest(t *testing.T, sameCell bool) *volumeTransferTest {
	e := &volumeTransferTest{t: t, ctx: context.Background(), factory: nbs_mocks.NewFactoryMock(), selector: cell_mocks.NewCellSelectorMock(),
		source: nbs_mocks.NewClientMock(), storage: storage_mocks.NewStorageMock(),
		scheduler: tasks_mocks.NewSchedulerMock(), exec: tasks_mocks.NewExecutionContextMock(),
		link: nbs.VolumeLink{LeaderDiskID: "leader", LeaderCellID: "source-cell", FollowerDiskID: "leader", FollowerCellID: "target-cell"},
	}
	state := &protos.TransferVolumeViaLinkTaskState{
		SrcDisk:             &types.Disk{ZoneId: "source-cell", DiskId: "leader"},
		DstDisk:             &types.Disk{ZoneId: "target-cell", DiskId: "leader"},
		SourceCreateTaskId:  "create-task",
		ReservationAcquired: true,
	}
	if sameCell {
		e.link.FollowerDiskID = "leader-copy"
		e.link.FollowerCellID = "source-cell"
		state.DstDisk = &types.Disk{ZoneId: "source-cell", DiskId: "leader-copy"}
	}
	e.task = &transferVolumeViaLinkTask{nbsFactory: e.factory, storage: e.storage, scheduler: e.scheduler, cellSelector: e.selector,
		request: volumeLinkTaskRequest(sameCell), state: state}
	e.exec.On("IsNonCancellable").Return(true).Maybe()
	e.exec.On("GetTaskID").Return("task-id").Maybe()
	e.factory.On("GetClient", e.ctx, "source-cell").Return(e.source, nil).Maybe()
	return e
}

func (e *volumeTransferTest) allowSaves() {
	e.exec.On("SaveState", e.ctx).Return(nil).Maybe()
}

func readyDiskMeta() *resources.DiskMeta {
	return &resources.DiskMeta{
		ID: "leader", ZoneID: "source-cell", CreateTaskID: "create-task", CreatedAt: time.Unix(1, 0),
	}
}

func (e *volumeTransferTest) expectReservation() {
	e.t.Helper()
	e.storage.On("ReserveDiskOperation", e.ctx, mock.Anything, resources.DiskOperation{
		DiskID: "leader", TaskID: "task-id", Kind: resources.DiskOperationTransfer,
		CellID: "source-cell", CreateTaskID: "create-task", Resume: true,
	}).Return(readyDiskMeta(), nil).Once()
	e.expectAcquisitionCheckpoint(nil)
}

func (e *volumeTransferTest) expectInitialReservation(meta *resources.DiskMeta, err error) *mock.Call {
	e.t.Helper()
	readMeta := meta
	if err != nil {
		readMeta = readyDiskMeta()
	}
	e.storage.On("GetDiskMeta", e.ctx, e.task.request.LogicalDiskId).Return(readMeta, nil).Once()
	if readMeta == nil {
		return nil
	}
	e.storage.On("ReserveDiskOperation", e.ctx, mock.Anything, resources.DiskOperation{
		DiskID: e.task.request.LogicalDiskId, TaskID: "task-id", Kind: resources.DiskOperationTransfer, CreateTaskID: "create-task",
	}).Return(meta, err).Once()
	return e.expectAcquisitionCheckpoint(err)
}

func (e *volumeTransferTest) expectAcquisitionCheckpoint(result error) *mock.Call {
	e.t.Helper()
	return e.exec.On("SaveStateWithPreparation", e.ctx, mock.MatchedBy(func(prepare func(context.Context, *persistence.Transaction) error) bool {
		return !e.task.state.ReservationReleased
	})).Run(func(args mock.Arguments) {
		err := args.Get(1).(func(context.Context, *persistence.Transaction) error)(e.ctx, nil)
		if result == nil {
			require.NoError(e.t, err)
		} else {
			require.Error(e.t, err)
			require.Equal(e.t, errors.ErrorForTracing(result), errors.ErrorForTracing(err))
		}
	}).Return(result).Once()
}

func (e *volumeTransferTest) restartTask(state []byte) {
	e.t.Helper()
	request, err := proto.Marshal(e.task.request)
	require.NoError(e.t, err)
	restored := &transferVolumeViaLinkTask{
		nbsFactory: e.factory, storage: e.storage, scheduler: e.scheduler, cellSelector: e.selector,
	}
	require.NoError(e.t, restored.Load(request, state))
	e.task = restored
}

func (e *volumeTransferTest) expectPreparation(physicalID string, meta *resources.DiskMeta) {
	e.task.state = &protos.TransferVolumeViaLinkTaskState{}
	e.expectInitialReservation(meta, nil)
	e.factory.On("HasClient", "source-cell").Return(true).Maybe()
	if e.task.request.DstCellId != "source-cell" {
		e.factory.On("HasClient", "target-cell").Return(true).Once()
	}
	e.source.On("ResolveVolumeLinkSource", e.ctx, e.task.request.LogicalDiskId).Return(physicalID, nil).Once()
	e.selector.On("CellsShareZone", "source-cell", e.link.FollowerCellID).Return(true).Once()
}

func (e *volumeTransferTest) expectFinish(execCtx tasks.ExecutionContext, catalogError error) {
	if !e.task.state.TargetFinalized {
		e.factory.On("FinalizeVolumeLinkTarget", e.ctx, e.link, "task-id").Return(nil).Once()
	}
	if e.task.state.IncrementalCleanupTaskId == "" {
		key := "task-id_delete_disk_from_incremental"
		if e.task.state.IncrementalCleanupAttempt != 0 {
			key += fmt.Sprintf("_%d", e.task.state.IncrementalCleanupAttempt)
		}
		e.scheduler.On("ScheduleTask", mock.MatchedBy(func(ctx context.Context) bool {
			return headers.GetIdempotencyKey(ctx) == key
		}), "dataplane.DeleteDiskFromIncremental", "", &dataplane_protos.DeleteDiskFromIncrementalRequest{
			Disk: &types.Disk{ZoneId: "source-cell", DiskId: "leader"},
		}).Return("cleanup-id", nil).Once()
	}
	e.scheduler.On("WaitTask", e.ctx, execCtx, "cleanup-id").Return(&empty.Empty{}, nil).Once()
	e.storage.On("FinishDiskTransfer", e.ctx, mock.Anything, "leader", "source-cell", e.link.FollowerCellID, "create-task", "task-id").Return(catalogError).Once()
	e.exec.On("FinishWithPreparation", e.ctx, mock.Anything).Run(func(args mock.Arguments) {
		err := args.Get(1).(func(context.Context, *persistence.Transaction) error)(e.ctx, nil)
		if catalogError == nil {
			require.NoError(e.t, err)
		} else {
			require.ErrorIs(e.t, err, catalogError)
		}
	}).Return(catalogError).Once()
}

func (e *volumeTransferTest) assertExpectations() {
	mock.AssertExpectationsForObjects(e.t, e.factory, e.source, e.storage, e.scheduler, e.exec, e.selector)
}

func TestTransferVolumeViaLinkCreatesTargetAndLink(t *testing.T) {
	for _, test := range []struct {
		name     string
		sameCell bool
		imageID  string
	}{
		{"cross-cell", false, ""},
		{"same-cell", true, ""},
		{"cross-cell/from-image", false, "image"},
		{"same-cell/from-image", true, "image"},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := newVolumeTransferTest(t, test.sameCell)
			meta := readyDiskMeta()
			meta.SrcImageID = test.imageID
			e.expectPreparation("leader", meta)
			e.allowSaves()
			e.factory.On("CreateVolumeLinkTarget", e.ctx, e.link, "task-id").Return(nil).Once()
			e.source.On("GetVolumeLinkStatus", e.ctx, e.link).Return(nbs.VolumeLinkStatus{State: nbs.VolumeLinkStateNotFound}, nil).Once()
			e.source.On("CreateVolumeLink", e.ctx, e.link).Return(nil).Once()
			e.source.On("GetVolumeLinkStatus", e.ctx, e.link).Return(nbs.VolumeLinkStatus{
				State: nbs.VolumeLinkStateCompleted, LinkUUID: "uuid",
			}, nil).Once()
			e.expectFinish(e.exec, nil)
			require.NoError(t, e.task.Run(e.ctx, e.exec))
			require.Equal(t, "uuid", e.task.state.LinkUUID)
			require.True(t, e.task.state.ReservationReleased)
			metadata, err := e.task.GetMetadata(e.ctx)
			require.NoError(t, err)
			reported := metadata.(*disk_manager.TransferVolumeViaLinkMetadata)
			require.Equal(t, "uuid", reported.LinkUuid)
			require.Equal(t, string(nbs.VolumeLinkStateCompleted), reported.Status)
			e.assertExpectations()
		})
	}
}

func TestTransferVolumeViaLinkWaitsForNbsCompletion(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.ctx = ctx
	e.factory.On("GetClient", ctx, "source-cell").Return(e.source, nil).Once()
	e.allowSaves()
	e.task.state.TargetCreated = true
	e.task.state.LinkCreated = true
	e.source.On("GetVolumeLinkStatus", ctx, e.link).Run(func(mock.Arguments) { cancel() }).Return(nbs.VolumeLinkStatus{
		State: nbs.VolumeLinkStateLeadershipTransferred, LinkUUID: "uuid",
	}, nil).Once()
	e.expectReservation()
	require.ErrorIs(t, e.task.Run(ctx, e.exec), context.Canceled)
	require.Equal(t, string(nbs.VolumeLinkStateLeadershipTransferred), e.task.state.Status)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkDoesNotRecreateDisappearedLink(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.allowSaves()
	e.task.state.TargetCreated = true
	e.task.state.LinkCreated = true
	e.task.state.LinkUUID = "uuid"
	e.source.On("GetVolumeLinkStatus", e.ctx, e.link).Return(nbs.VolumeLinkStatus{State: nbs.VolumeLinkStateNotFound}, nil).Once()
	e.expectReservation()
	require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyRetriableError())
	e.source.AssertNotCalled(t, "CreateVolumeLink", mock.Anything, mock.Anything)
	e.factory.AssertNotCalled(t, "DeleteVolumeLinkTarget", mock.Anything, mock.Anything, mock.Anything)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkRejectsMissingRequiredParameters(t *testing.T) {
	request := volumeLinkTaskRequest(false)
	request.LogicalDiskId = ""
	task := &transferVolumeViaLinkTask{request: request, state: &protos.TransferVolumeViaLinkTaskState{}}
	require.ErrorIs(t, task.Run(context.Background(), tasks_mocks.NewExecutionContextMock()), errors.NewEmptyNonCancellableError())
}

func TestTransferVolumeViaLinkUsesPreparedNamesAfterRestart(t *testing.T) {
	e := newVolumeTransferTest(t, true)
	e.allowSaves()
	e.link.LeaderDiskID = "leader-copy"
	e.link.FollowerDiskID = "leader"
	e.task.state.SrcDisk.DiskId = "leader-copy"
	e.task.state.DstDisk.DiskId = "leader"
	e.task.state.TargetCreated = true
	e.task.state.LinkCreated = true
	e.task.state.LinkCreationStarted = true
	e.task.state.Status = string(nbs.VolumeLinkStateCompleted)
	e.task.state.LinkUUID = "uuid"
	state, err := e.task.Save()
	require.NoError(t, err)
	e.restartTask(state)
	e.expectFinish(e.exec, nil)
	e.expectReservation()
	require.NoError(t, e.task.Run(e.ctx, e.exec))
	e.source.AssertNotCalled(t, "ResolveVolumeLinkSource", mock.Anything, mock.Anything)
	e.source.AssertNotCalled(t, "GetVolumeLinkStatus", mock.Anything, mock.Anything)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkRetriesCatalogFinalization(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.allowSaves()
	e.task.state.Status = string(nbs.VolumeLinkStateCompleted)
	e.task.state.TargetFinalized = true
	e.task.state.IncrementalCleanupTaskId = "cleanup-id"
	failure := fmt.Errorf("database unavailable")
	e.expectFinish(e.exec, failure)
	e.expectReservation()
	err := e.task.Run(e.ctx, e.exec)
	require.ErrorIs(t, err, errors.NewEmptyRetriableError())
	require.False(t, errors.Is(err, errors.NewEmptyNonCancellableError()))
	e.expectFinish(e.exec, nil)
	e.expectReservation()
	require.NoError(t, e.task.Run(e.ctx, e.exec))
	e.source.AssertNotCalled(t, "GetVolumeLinkStatus", mock.Anything, mock.Anything)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkCleansUpBeforeLinkCreation(t *testing.T) {
	for _, test := range []struct {
		name    string
		failure error
	}{
		{"task-error", errors.NewNonRetriableErrorf("invalid response")},
		{"nbs-error", &nbs_client.ClientError{Code: nbs_client.E_ARGUMENT, Message: "invalid request"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := newVolumeTransferTest(t, false)
			e.expectFailureRelease()
			e.allowSaves()
			e.task.state.TargetCreated = true
			e.source.On("GetVolumeLinkStatus", e.ctx, e.link).Return(nbs.VolumeLinkStatus{}, test.failure).Once()
			e.factory.On("DeleteVolumeLinkTarget", e.ctx, e.link, "task-id").Return(nil).Once()
			e.expectReservation()
			require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyNonCancellableError())
			require.True(t, e.task.state.ReservationReleased)
			require.False(t, e.task.state.TargetCreated)
			e.assertExpectations()
		})
	}
}

func TestTransferVolumeViaLinkRetainsTargetAfterAmbiguousCreate(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.task.state.TargetCreated = true
	var persisted []byte
	e.exec.On("SaveState", e.ctx).Run(func(mock.Arguments) {
		var err error
		persisted, err = e.task.Save()
		require.NoError(t, err)
	}).Return(nil)
	e.source.On("GetVolumeLinkStatus", e.ctx, e.link).Return(nbs.VolumeLinkStatus{State: nbs.VolumeLinkStateNotFound}, nil).Once()
	e.source.On("CreateVolumeLink", e.ctx, e.link).Run(func(mock.Arguments) {
		require.True(t, e.task.state.LinkCreationStarted)
		require.NotEmpty(t, persisted)
	}).Return(&nbs_client.ClientError{Code: nbs_client.E_TIMEOUT, Message: "lost reply"}).Once()
	e.expectReservation()
	require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyRetriableError())
	e.factory.AssertNotCalled(t, "DeleteVolumeLinkTarget", mock.Anything, mock.Anything, mock.Anything)
	e.restartTask(persisted)
	e.source.On("GetVolumeLinkStatus", e.ctx, e.link).Return(nbs.VolumeLinkStatus{State: nbs.VolumeLinkStateCompleted, LinkUUID: "uuid"}, nil).Once()
	e.expectFinish(e.exec, nil)
	e.expectReservation()
	require.NoError(t, e.task.Run(e.ctx, e.exec))
	e.assertExpectations()
}

func TestTransferVolumeViaLinkDoesNotAdvanceAfterPrepareSaveFails(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.task.state = &protos.TransferVolumeViaLinkTaskState{}
	e.expectInitialReservation(readyDiskMeta(), nil).Return(errors.NewNonRetriableErrorf("storage unavailable"))
	require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyRetriableError())
	e.factory.AssertNotCalled(t, "CreateVolumeLinkTarget", mock.Anything, mock.Anything, mock.Anything)
	e.factory.AssertNotCalled(t, "DeleteVolumeLinkTarget", mock.Anything, mock.Anything, mock.Anything)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkRejectsUnreadyCatalogDisk(t *testing.T) {
	for _, name := range []string{"missing", "creating", "deleting", "placement-group"} {
		t.Run(name, func(t *testing.T) {
			e := newVolumeTransferTest(t, false)
			e.allowSaves()
			e.task.state = &protos.TransferVolumeViaLinkTaskState{}
			meta := readyDiskMeta()
			switch name {
			case "missing":
				meta = nil
			case "creating":
				meta.CreatedAt = time.Time{}
			case "deleting":
				meta.DeleteTaskID = "delete-task"
			case "placement-group":
				meta.PlacementGroupID = "pg"
			}
			e.expectInitialReservation(meta, nil)
			if meta != nil {
				e.expectFailureRelease()
			}
			require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyNonCancellableError())
			e.assertExpectations()
		})
	}
}

func TestTransferVolumeViaLinkRejectsUnknownDestinationCell(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.allowSaves()
	e.expectFailureRelease()
	e.task.state = &protos.TransferVolumeViaLinkTaskState{}
	e.expectInitialReservation(readyDiskMeta(), nil)
	e.factory.On("HasClient", "source-cell").Return(true).Once()
	e.factory.On("HasClient", "target-cell").Return(false).Once()
	require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyNonCancellableError())
	e.source.AssertNotCalled(t, "ResolveVolumeLinkSource", mock.Anything, mock.Anything)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkRejectsChangedUUID(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.allowSaves()
	e.task.state.TargetCreated = true
	e.task.state.LinkCreated = true
	e.task.state.LinkUUID = "old-uuid"
	e.source.On("GetVolumeLinkStatus", e.ctx, e.link).Return(nbs.VolumeLinkStatus{
		State: nbs.VolumeLinkStatePreparing, LinkUUID: "another-uuid",
	}, nil).Once()
	e.expectReservation()
	require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyRetriableError())
	require.Equal(t, "old-uuid", e.task.state.LinkUUID)
	e.factory.AssertNotCalled(t, "DeleteVolumeLinkTarget", mock.Anything, mock.Anything, mock.Anything)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkRetriesTargetFinalization(t *testing.T) {
	for _, test := range []struct {
		name     string
		failure  error
		recovery bool
	}{
		{"temporary", errors.NewRetriableErrorf("temporarily unavailable"), false},
		{"permanent", errors.NewNonRetriableErrorf("target ownership changed"), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := newVolumeTransferTest(t, false)
			e.allowSaves()
			e.task.state.Status = string(nbs.VolumeLinkStateCompleted)
			e.factory.On("FinalizeVolumeLinkTarget", e.ctx, e.link, "task-id").Return(test.failure).Once()
			e.expectReservation()
			err := e.task.Run(e.ctx, e.exec)
			require.ErrorIs(t, err, errors.NewEmptyRetriableError())
			require.False(t, isTerminalDiskOperationError(err))
			require.Equal(t, string(nbs.VolumeLinkStateCompleted), e.task.state.Status)
			require.Equal(t, test.recovery, e.task.state.RecoveryRequired)
			require.True(t, e.task.state.ReservationAcquired)
			require.False(t, e.task.state.ReservationReleased)
			e.exec.AssertNotCalled(t, "FinishWithPreparation", mock.Anything, mock.Anything)
			e.factory.AssertNotCalled(t, "DeleteVolumeLinkTarget", mock.Anything, mock.Anything, mock.Anything)
			e.storage.AssertNotCalled(t, "ReleaseDiskOperation", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			e.assertExpectations()
		})
	}
}

func TestTransferVolumeViaLinkResumesCleanupWithoutRestartingCopy(t *testing.T) {
	for _, test := range []struct {
		name     string
		failure  error
		recovery bool
	}{
		{"temporary", fmt.Errorf("cleanup unavailable"), false},
		{"permanent", errors.NewNonRetriableErrorf("cleanup rejected"), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := newVolumeTransferTest(t, false)
			e.task.state.TargetCreated = true
			var persisted []byte
			e.exec.On("SaveState", e.ctx).Run(func(mock.Arguments) {
				var err error
				persisted, err = e.task.Save()
				require.NoError(t, err)
			}).Return(nil)
			e.source.On("GetVolumeLinkStatus", e.ctx, e.link).Return(nbs.VolumeLinkStatus{}, errors.NewNonRetriableErrorf("invalid response")).Once()
			e.factory.On("DeleteVolumeLinkTarget", e.ctx, e.link, "task-id").Return(test.failure).Once()
			e.expectReservation()
			err := e.task.Run(e.ctx, e.exec)
			require.ErrorIs(t, err, errors.NewEmptyRetriableError())
			require.False(t, isTerminalDiskOperationError(err))
			require.True(t, e.task.state.FailurePending)
			require.Equal(t, test.recovery, e.task.state.RecoveryRequired)
			e.storage.AssertNotCalled(t, "ReleaseDiskOperation", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			e.restartTask(persisted)
			e.factory.On("DeleteVolumeLinkTarget", e.ctx, e.link, "task-id").Return(nil).Once()
			e.expectFailureRelease()
			require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyNonCancellableError())
			require.False(t, e.task.state.TargetCreated)
			require.True(t, e.task.state.ReservationReleased)
			require.False(t, e.task.state.RecoveryRequired)
			e.source.AssertNotCalled(t, "CreateVolumeLink", mock.Anything, mock.Anything)
			e.assertExpectations()
		})
	}
}

func (e *volumeTransferTest) expectFailureRelease() {
	e.storage.On("ReleaseDiskOperation", e.ctx, mock.Anything, "leader", "task-id").Return(nil).Once()
	e.exec.On("SaveStateWithPreparation", e.ctx, mock.MatchedBy(func(prepare func(context.Context, *persistence.Transaction) error) bool {
		return e.task.state.ReservationReleased
	})).Run(func(args mock.Arguments) {
		require.NoError(e.t, args.Get(1).(func(context.Context, *persistence.Transaction) error)(e.ctx, nil))
	}).Return(nil).Once()
}

func TestTransferVolumeViaLinkFinishesConfirmedCreateRejection(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.allowSaves()
	e.expectFailureRelease()
	e.task.state.TargetCreated = true
	e.source.On("GetVolumeLinkStatus", e.ctx, e.link).Return(nbs.VolumeLinkStatus{State: nbs.VolumeLinkStateNotFound}, nil).Once()
	e.source.On("CreateVolumeLink", e.ctx, e.link).Return(&nbs_client.ClientError{Code: nbs_client.E_NOT_IMPLEMENTED}).Once()
	e.factory.On("DeleteVolumeLinkTarget", e.ctx, e.link, "task-id").Return(nil).Once()
	e.expectReservation()
	require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyNonCancellableError())
	require.False(t, e.task.state.LinkCreationStarted)
	require.True(t, e.task.state.ReservationReleased)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkDoesNotAdoptForeignCompletedLink(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.allowSaves()
	e.task.state.TargetCreated = true
	e.source.On("GetVolumeLinkStatus", e.ctx, e.link).Return(nbs.VolumeLinkStatus{
		State: nbs.VolumeLinkStateCompleted, LinkUUID: "historical-uuid",
	}, nil).Once()
	e.expectReservation()
	require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyRetriableError())
	require.True(t, e.task.state.RecoveryRequired)
	require.Empty(t, e.task.state.LinkUUID)
	require.NotEqual(t, string(nbs.VolumeLinkStateCompleted), e.task.state.Status)
	e.factory.AssertNotCalled(t, "DeleteVolumeLinkTarget", mock.Anything, mock.Anything, mock.Anything)
	e.storage.AssertNotCalled(t, "ReleaseDiskOperation", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkDoesNotTouchBusyDisk(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.task.state = &protos.TransferVolumeViaLinkTaskState{}
	e.expectInitialReservation(nil, errors.NewNonRetriableError(fmt.Errorf("%w", resources.ErrDiskOperationInProgress)))
	require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyNonCancellableError())
	e.factory.AssertNotCalled(t, "CreateVolumeLinkTarget", mock.Anything, mock.Anything, mock.Anything)
	e.storage.AssertNotCalled(t, "ReleaseDiskOperation", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkDoesNotCleanUpAfterEarlierUnknownCreate(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.allowSaves()
	e.task.state.TargetCreated = true
	e.task.state.LinkCreationStarted = true
	e.source.On("GetVolumeLinkStatus", e.ctx, e.link).Return(nbs.VolumeLinkStatus{State: nbs.VolumeLinkStateNotFound}, nil).Once()
	e.source.On("CreateVolumeLink", e.ctx, e.link).Return(&nbs_client.ClientError{Code: nbs_client.E_ARGUMENT}).Once()
	e.expectReservation()
	require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyRetriableError())
	require.True(t, e.task.state.LinkCreationStarted)
	require.True(t, e.task.state.RecoveryRequired)
	require.False(t, e.task.state.ReservationReleased)
	e.factory.AssertNotCalled(t, "DeleteVolumeLinkTarget", mock.Anything, mock.Anything, mock.Anything)
	e.storage.AssertNotCalled(t, "ReleaseDiskOperation", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkResumesRecoveryAndReleasesReservation(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.task.state.TargetCreated = true
	e.task.state.LinkCreationStarted = true
	e.task.state.LinkCreated = true
	e.task.state.LinkUUID = "uuid"
	var saved []byte
	e.exec.On("SaveState", e.ctx).Run(func(mock.Arguments) {
		var err error
		saved, err = e.task.Save()
		require.NoError(t, err)
	}).Return(nil)
	e.source.On("GetVolumeLinkStatus", e.ctx, e.link).Return(nbs.VolumeLinkStatus{
		State: nbs.VolumeLinkStateError, LinkUUID: "uuid", ErrorMessage: "copy failed",
	}, nil).Once()
	e.expectReservation()
	err := e.task.Run(e.ctx, e.exec)
	require.ErrorIs(t, err, errors.NewEmptyRetriableError())
	require.False(t, isTerminalDiskOperationError(err))
	require.True(t, e.task.state.RecoveryRequired)
	require.False(t, e.task.state.ReservationReleased)
	require.Equal(t, string(nbs.VolumeLinkStateError), e.task.state.Status)
	e.factory.AssertNotCalled(t, "DeleteVolumeLinkTarget", mock.Anything, mock.Anything, mock.Anything)
	e.storage.AssertNotCalled(t, "FinishDiskTransfer", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	e.restartTask(saved)
	e.source.On("GetVolumeLinkStatus", e.ctx, e.link).Return(nbs.VolumeLinkStatus{
		State: nbs.VolumeLinkStateCompleted, LinkUUID: "uuid",
	}, nil).Once()
	e.expectReservation()
	e.expectFinish(e.exec, nil)
	require.NoError(t, e.task.Run(e.ctx, e.exec))
	require.True(t, e.task.state.ReservationReleased)
	require.False(t, e.task.state.RecoveryRequired)
	require.Empty(t, e.task.state.ErrorMessage)
	finished, err := e.task.Save()
	require.NoError(t, err)
	e.restartTask(finished)
	require.NoError(t, e.task.Run(e.ctx, e.exec))
	e.assertExpectations()
}

func TestTransferVolumeViaLinkChoosesNameRelativeToLogicalDiskID(t *testing.T) {
	for _, test := range []struct {
		name, logicalID, physicalID, targetID string
		sameCell                              bool
	}{
		{"same-cell/base", "volume", "volume", "volume-copy", true},
		{"same-cell/copy", "volume", "volume-copy", "volume", true},
		{"cross-cell/base", "volume", "volume", "volume", false},
		{"cross-cell/copy", "volume", "volume-copy", "volume", false},
		{"same-cell/literal-suffix/base", "volume-copy", "volume-copy", "volume-copy-copy", true},
		{"same-cell/literal-suffix/copy", "volume-copy", "volume-copy-copy", "volume-copy", true},
		{"cross-cell/literal-suffix/base", "volume-copy", "volume-copy", "volume-copy", false},
		{"cross-cell/literal-suffix/copy", "volume-copy", "volume-copy-copy", "volume-copy", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := newVolumeTransferTest(t, test.sameCell)
			e.task.request.LogicalDiskId = test.logicalID
			e.link.LeaderDiskID = test.physicalID
			e.link.FollowerDiskID = test.targetID
			meta := readyDiskMeta()
			meta.ID = test.logicalID
			e.expectPreparation(test.physicalID, meta)
			e.allowSaves()
			stop := errors.NewRetriableErrorf("stop after preparation")
			e.factory.On("CreateVolumeLinkTarget", e.ctx, e.link, "task-id").Return(stop).Once()
			require.ErrorIs(t, e.task.Run(e.ctx, e.exec), stop)
			require.Equal(t, &types.Disk{ZoneId: "source-cell", DiskId: test.physicalID}, e.task.state.SrcDisk)
			require.Equal(t, &types.Disk{ZoneId: e.link.FollowerCellID, DiskId: test.targetID}, e.task.state.DstDisk)
			e.assertExpectations()
		})
	}
}

func TestTransferVolumeViaLinkRetriesFailedIncrementalCleanup(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.task.state.Status = string(nbs.VolumeLinkStateCompleted)
	e.task.state.TargetFinalized = true
	e.task.state.IncrementalCleanupTaskId = "failed-cleanup-id"
	var saved []byte
	e.exec.On("SaveState", e.ctx).Run(func(mock.Arguments) {
		require.Empty(t, e.task.state.IncrementalCleanupTaskId)
		require.Equal(t, uint64(1), e.task.state.IncrementalCleanupAttempt)
		var err error
		saved, err = e.task.Save()
		require.NoError(t, err)
	}).Return(nil).Once()
	e.scheduler.On("WaitTask", e.ctx, e.exec, "failed-cleanup-id").Return(nil, errors.NewNonRetriableErrorf("cleanup failed")).Once()
	e.scheduler.On("GetOperation", e.ctx, "failed-cleanup-id").Return(&operation.Operation{
		Done: true, Result: &operation.Operation_Error{Error: grpc_status.New(grpc_codes.Internal, "cleanup failed").Proto()},
	}, nil).Once()
	e.scheduler.On("WaitTaskEnded", e.ctx, "failed-cleanup-id").Run(func(mock.Arguments) {
		require.Equal(t, "failed-cleanup-id", e.task.state.IncrementalCleanupTaskId)
		require.Zero(t, e.task.state.IncrementalCleanupAttempt)
	}).Return(nil).Once()
	e.expectReservation()
	err := e.task.Run(e.ctx, e.exec)
	require.ErrorIs(t, err, errors.NewEmptyRetriableError())
	require.False(t, isTerminalDiskOperationError(err))
	require.True(t, e.task.state.RecoveryRequired)
	require.False(t, e.task.state.ReservationReleased)
	e.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	e.restartTask(saved)
	e.allowSaves()
	e.expectReservation()
	e.expectFinish(e.exec, nil)
	require.NoError(t, e.task.Run(e.ctx, e.exec))
	require.Equal(t, "cleanup-id", e.task.state.IncrementalCleanupTaskId)
	require.Equal(t, uint64(1), e.task.state.IncrementalCleanupAttempt)
	require.True(t, e.task.state.ReservationReleased)
	require.False(t, e.task.state.RecoveryRequired)
	require.Empty(t, e.task.state.ErrorMessage)
	e.source.AssertNotCalled(t, "CreateVolumeLink", mock.Anything, mock.Anything)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkKeepsCleanupAttemptOnWaitErrors(t *testing.T) {
	for _, test := range []struct {
		name      string
		child     *operation.Operation
		lookupErr error
		endErr    error
	}{
		{name: "status-unavailable", lookupErr: fmt.Errorf("database unavailable")},
		{name: "running", child: &operation.Operation{}},
		{name: "cancelling", child: &operation.Operation{Done: true, Result: &operation.Operation_Error{
			Error: grpc_status.New(grpc_codes.Canceled, "cancelling").Proto(),
		}}, endErr: errors.NewInterruptExecutionError()},
		{name: "already-succeeded", child: &operation.Operation{Done: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := newVolumeTransferTest(t, false)
			e.task.state.Status = string(nbs.VolumeLinkStateCompleted)
			e.task.state.TargetFinalized = true
			e.task.state.IncrementalCleanupTaskId = "cleanup-id"
			e.scheduler.On("WaitTask", e.ctx, e.exec, "cleanup-id").Return(nil, fmt.Errorf("waiting failed")).Once()
			e.scheduler.On("GetOperation", e.ctx, "cleanup-id").Return(test.child, test.lookupErr).Once()
			if test.endErr != nil {
				e.scheduler.On("WaitTaskEnded", e.ctx, "cleanup-id").Return(test.endErr).Once()
			}
			e.expectReservation()
			err := e.task.Run(e.ctx, e.exec)
			if test.endErr != nil {
				require.ErrorIs(t, err, test.endErr)
			} else {
				require.ErrorIs(t, err, errors.NewEmptyRetriableError())
			}
			require.False(t, isTerminalDiskOperationError(err))
			require.Equal(t, "cleanup-id", e.task.state.IncrementalCleanupTaskId)
			require.Zero(t, e.task.state.IncrementalCleanupAttempt)
			require.False(t, e.task.state.ReservationReleased)
			if test.endErr != nil {
				e.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
				e.exec.AssertNotCalled(t, "FinishWithPreparation", mock.Anything, mock.Anything)
				e.assertExpectations()
				return
			}
			e.expectReservation()
			e.expectFinish(e.exec, nil)
			require.NoError(t, e.task.Run(e.ctx, e.exec))
			require.True(t, e.task.state.ReservationReleased)
			e.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			e.assertExpectations()
		})
	}
}

func TestTransferVolumeViaLinkReusesCleanupKeyAfterLostScheduleReply(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.allowSaves()
	e.task.state.Status = string(nbs.VolumeLinkStateCompleted)
	e.task.state.TargetFinalized = true
	e.task.state.IncrementalCleanupAttempt = 1
	e.scheduler.On("ScheduleTask", mock.MatchedBy(func(ctx context.Context) bool {
		return headers.GetIdempotencyKey(ctx) == "task-id_delete_disk_from_incremental_1"
	}), "dataplane.DeleteDiskFromIncremental", "", &dataplane_protos.DeleteDiskFromIncrementalRequest{
		Disk: &types.Disk{ZoneId: "source-cell", DiskId: "leader"},
	}).Return("", fmt.Errorf("reply lost after scheduling")).Once()
	e.expectReservation()
	require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyRetriableError())
	require.Empty(t, e.task.state.IncrementalCleanupTaskId)
	require.Equal(t, uint64(1), e.task.state.IncrementalCleanupAttempt)
	saved, err := e.task.Save()
	require.NoError(t, err)
	e.restartTask(saved)
	e.expectReservation()
	e.expectFinish(e.exec, nil)
	require.NoError(t, e.task.Run(e.ctx, e.exec))
	e.scheduler.AssertNumberOfCalls(t, "ScheduleTask", 2)
	require.Equal(t, uint64(1), e.task.state.IncrementalCleanupAttempt)
	require.True(t, e.task.state.ReservationReleased)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkSavesCleanupAttemptBeforeScheduling(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.task.state.Status = string(nbs.VolumeLinkStateCompleted)
	e.task.state.TargetFinalized = true
	e.task.state.IncrementalCleanupTaskId = "failed-cleanup-id"
	durable, err := e.task.Save()
	require.NoError(t, err)
	e.exec.On("SaveState", e.ctx).Return(fmt.Errorf("checkpoint unavailable")).Once()
	child := &operation.Operation{Done: true, Result: &operation.Operation_Error{
		Error: grpc_status.New(grpc_codes.Internal, "cleanup failed").Proto(),
	}}
	e.scheduler.On("WaitTask", e.ctx, e.exec, "failed-cleanup-id").Return(nil, fmt.Errorf("cleanup failed")).Twice()
	e.scheduler.On("GetOperation", e.ctx, "failed-cleanup-id").Return(child, nil).Twice()
	e.scheduler.On("WaitTaskEnded", e.ctx, "failed-cleanup-id").Return(nil).Twice()
	e.expectReservation()
	require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyRetriableError())
	e.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	e.restartTask(durable)
	e.allowSaves()
	e.expectReservation()
	require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyRetriableError())
	require.Equal(t, uint64(1), e.task.state.IncrementalCleanupAttempt)
	e.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	e.expectReservation()
	e.expectFinish(e.exec, nil)
	require.NoError(t, e.task.Run(e.ctx, e.exec))
	require.Equal(t, uint64(1), e.task.state.IncrementalCleanupAttempt)
	require.True(t, e.task.state.ReservationReleased)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkKeepsCleanupAttemptOnExecutionInterruption(t *testing.T) {
	for _, test := range []struct {
		name          string
		failure       error
		cancelContext bool
	}{
		{"interrupted", errors.NewInterruptExecutionError(), false},
		{"wrong-generation", errors.NewWrongGenerationError(), false},
		{"context-cancelled", context.Canceled, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := newVolumeTransferTest(t, false)
			ctx, cancel := context.WithCancel(e.ctx)
			defer cancel()
			e.ctx = ctx
			e.task.state.Status = string(nbs.VolumeLinkStateCompleted)
			e.task.state.TargetFinalized = true
			e.task.state.IncrementalCleanupTaskId = "cleanup-id"
			e.scheduler.On("WaitTask", ctx, e.exec, "cleanup-id").Run(func(mock.Arguments) {
				if test.cancelContext {
					cancel()
				}
			}).Return(nil, test.failure).Once()
			e.expectReservation()
			require.ErrorIs(t, e.task.Run(ctx, e.exec), test.failure)
			require.Equal(t, "cleanup-id", e.task.state.IncrementalCleanupTaskId)
			require.Zero(t, e.task.state.IncrementalCleanupAttempt)
			e.scheduler.AssertNotCalled(t, "GetOperation", mock.Anything, mock.Anything)
			e.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			e.exec.AssertNotCalled(t, "FinishWithPreparation", mock.Anything, mock.Anything)
			e.assertExpectations()
		})
	}
}

func TestTransferVolumeViaLinkChecksExpectedZoneAndDefaultCell(t *testing.T) {
	for _, test := range []struct {
		name         string
		zoneID       string
		dstCellID    string
		sourceCellID string
		wantCellID   string
		valid        bool
		withoutCells bool
	}{
		{name: "source-only", wantCellID: "source-cell", valid: true},
		{name: "destination-without-zone", dstCellID: "target-cell", wantCellID: "target-cell", valid: true},
		{name: "expected-zone-default-cell", zoneID: "zone-a", wantCellID: "source-cell", valid: true},
		{name: "expected-zone-destination", zoneID: "zone-a", dstCellID: "target-cell", wantCellID: "target-cell", valid: true},
		{name: "wrong-source-zone", zoneID: "zone-b", dstCellID: "target-cell"},
		{name: "unknown-zone", zoneID: "unknown-zone"},
		{name: "wrong-destination-zone", zoneID: "zone-a", dstCellID: "other-cell"},
		{name: "cross-zone-without-expected-zone", dstCellID: "other-cell"},
		{name: "main-cell", zoneID: "zone-a", sourceCellID: "zone-a", wantCellID: "zone-a", valid: true},
		{name: "without-sharding", zoneID: "zone-a", sourceCellID: "zone-a", wantCellID: "zone-a", valid: true, withoutCells: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := newVolumeTransferTest(t, false)
			e.allowSaves()
			e.task.state = &protos.TransferVolumeViaLinkTaskState{}
			e.task.request = &protos.TransferVolumeViaLinkRequest{LogicalDiskId: "leader", ZoneId: test.zoneID, DstCellId: test.dstCellID}
			config := &cells_config.CellsConfig{Cells: map[string]*cells_config.ZoneCells{
				"zone-a": {Cells: []string{"source-cell", "target-cell"}},
				"zone-b": {Cells: []string{"other-cell"}},
			}}
			if test.withoutCells {
				config = nil
			}
			e.task.cellSelector = cells.NewCellSelector(config, nil, nil, nil)
			meta := readyDiskMeta()
			if test.sourceCellID != "" {
				meta.ZoneID = test.sourceCellID
			}
			e.expectInitialReservation(meta, nil)
			targetCell := test.dstCellID
			if targetCell == "" {
				targetCell = meta.ZoneID
			}
			if targetCell == meta.ZoneID {
				e.factory.On("HasClient", meta.ZoneID).Return(true).Twice()
			} else {
				e.factory.On("HasClient", meta.ZoneID).Return(true).Once()
				e.factory.On("HasClient", targetCell).Return(true).Once()
			}
			if test.valid {
				if meta.ZoneID != "source-cell" {
					e.factory.On("GetClient", e.ctx, meta.ZoneID).Return(e.source, nil).Twice()
				}
				e.source.On("ResolveVolumeLinkSource", e.ctx, "leader").Return("leader", nil).Once()
				targetID := "leader"
				if test.wantCellID == meta.ZoneID {
					targetID = "leader-copy"
				}
				link := nbs.VolumeLink{LeaderDiskID: "leader", LeaderCellID: meta.ZoneID, FollowerDiskID: targetID, FollowerCellID: test.wantCellID}
				stop := errors.NewRetriableErrorf("stop after preparation")
				e.factory.On("CreateVolumeLinkTarget", e.ctx, link, "task-id").Return(stop).Once()
				require.ErrorIs(t, e.task.Run(e.ctx, e.exec), stop)
				require.Equal(t, meta.ZoneID, e.task.state.SrcDisk.ZoneId)
				require.Equal(t, test.wantCellID, e.task.state.DstDisk.ZoneId)
				require.Equal(t, targetID, e.task.state.DstDisk.DiskId)
				require.Equal(t, test.dstCellID, e.task.request.DstCellId)
			} else {
				e.expectFailureRelease()
				err := e.task.Run(e.ctx, e.exec)
				require.ErrorIs(t, err, errors.NewEmptyNonCancellableError())
				require.Contains(t, errors.ErrorForTracing(err), "zone")
				require.True(t, e.task.state.ReservationReleased)
				e.factory.AssertNotCalled(t, "GetClient", mock.Anything, mock.Anything)
				e.factory.AssertNotCalled(t, "CreateVolumeLinkTarget", mock.Anything, mock.Anything, mock.Anything)
			}
			e.assertExpectations()
		})
	}
}

func TestTransferVolumeViaLinkKeepsDefaultDestinationAfterRestart(t *testing.T) {
	e := newVolumeTransferTest(t, true)
	e.expectPreparation("leader", readyDiskMeta())
	e.task.request.DstCellId = ""
	e.allowSaves()
	stop := errors.NewRetriableErrorf("stop after preparation")
	e.factory.On("CreateVolumeLinkTarget", e.ctx, e.link, "task-id").Return(stop).Once()
	require.ErrorIs(t, e.task.Run(e.ctx, e.exec), stop)
	require.Equal(t, "source-cell", e.task.state.DstDisk.ZoneId)
	saved, err := e.task.Save()
	require.NoError(t, err)
	e.restartTask(saved)
	e.task.state.Status = string(nbs.VolumeLinkStateCompleted)
	e.task.state.TargetFinalized = true
	e.task.state.IncrementalCleanupTaskId = "cleanup-id"
	e.expectReservation()
	e.expectFinish(e.exec, nil)
	require.NoError(t, e.task.Run(e.ctx, e.exec))
	require.Empty(t, e.task.request.DstCellId)
	require.Equal(t, "source-cell", e.task.state.DstDisk.ZoneId)
	require.True(t, e.task.state.ReservationReleased)
	e.source.AssertNumberOfCalls(t, "ResolveVolumeLinkSource", 1)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkCleansUpAfterIntentSaveFailureAndReload(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.task.state.TargetCreated = true
	e.source.On("GetVolumeLinkStatus", e.ctx, e.link).Return(nbs.VolumeLinkStatus{State: nbs.VolumeLinkStateNotFound}, nil).Twice()
	e.exec.On("SaveState", e.ctx).Run(func(mock.Arguments) {
		require.True(t, e.task.state.LinkCreationStarted)
	}).Return(fmt.Errorf("intent persistence failed")).Once()
	e.expectReservation()
	require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyRetriableError())
	require.False(t, e.task.state.LinkCreationStarted)
	require.False(t, e.task.state.LinkCreated)
	e.source.AssertNotCalled(t, "CreateVolumeLink", mock.Anything, mock.Anything)
	e.factory.AssertNotCalled(t, "DeleteVolumeLinkTarget", mock.Anything, mock.Anything, mock.Anything)
	e.storage.AssertNotCalled(t, "ReleaseDiskOperation", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	// incrementRetriableErrorCount serializes the current task through Save.
	accounted, err := e.task.Save()
	require.NoError(t, err)
	e.restartTask(accounted)
	require.False(t, e.task.state.LinkCreationStarted)
	e.allowSaves()
	e.expectReservation()
	e.source.On("CreateVolumeLink", e.ctx, e.link).Return(&nbs_client.ClientError{Code: nbs_client.E_NOT_IMPLEMENTED}).Once()
	e.factory.On("DeleteVolumeLinkTarget", e.ctx, e.link, "task-id").Return(nil).Once()
	e.expectFailureRelease()
	require.ErrorIs(t, e.task.Run(e.ctx, e.exec), errors.NewEmptyNonCancellableError())
	require.False(t, e.task.state.LinkCreationStarted)
	require.False(t, e.task.state.TargetCreated)
	require.False(t, e.task.state.ReservationAcquired)
	require.True(t, e.task.state.ReservationReleased)
	require.False(t, e.task.state.RecoveryRequired)
	e.source.AssertNumberOfCalls(t, "CreateVolumeLink", 1)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkSavesOnlyChangedLinkStatus(t *testing.T) {
	e := newVolumeTransferTest(t, false)
	e.task.state.LinkCreated = true
	e.task.state.LinkCreationStarted = true
	e.task.state.LinkUUID = "uuid"
	e.task.state.Status = string(nbs.VolumeLinkStatePreparing)
	status := nbs.VolumeLinkStatus{State: nbs.VolumeLinkStatePreparing, LinkUUID: "uuid"}
	for i := 0; i != 3; i++ {
		require.NoError(t, e.task.updateLinkStatus(e.ctx, e.exec, status))
	}
	e.exec.AssertNotCalled(t, "SaveState", mock.Anything)
	e.exec.On("SaveState", e.ctx).Return(nil).Once()
	status.State = nbs.VolumeLinkStateLeadershipTransferring
	require.NoError(t, e.task.updateLinkStatus(e.ctx, e.exec, status))
	require.Equal(t, string(nbs.VolumeLinkStateLeadershipTransferring), e.task.state.Status)
	e.assertExpectations()
}

func TestTransferVolumeViaLinkReleasesReservationWithoutReenteringExecutionContext(t *testing.T) {
	for _, name := range []string{"finish", "cleanup"} {
		t.Run(name, func(t *testing.T) {
			e := newVolumeTransferTest(t, false)
			locked := &preparationLockedExecutionContext{ExecutionContext: e.exec, t: t}
			if name == "finish" {
				e.task.state.Status = string(nbs.VolumeLinkStateCompleted)
				e.task.state.TargetFinalized = true
				e.task.state.IncrementalCleanupTaskId = "cleanup-id"
				e.expectFinish(locked, nil)
				require.NoError(t, e.task.finish(e.ctx, locked))
			} else {
				e.task.state.FailurePending = true
				e.task.state.ErrorMessage = "creation rejected"
				e.factory.On("DeleteVolumeLinkTarget", e.ctx, e.link, "task-id").Return(nil).Once()
				e.expectFailureRelease()
				require.ErrorIs(t, e.task.cleanupAndFail(e.ctx, locked), errors.NewEmptyNonCancellableError())
			}
			require.True(t, e.task.state.ReservationReleased)
			require.False(t, e.task.state.ReservationAcquired)
			e.assertExpectations()
		})
	}
}
