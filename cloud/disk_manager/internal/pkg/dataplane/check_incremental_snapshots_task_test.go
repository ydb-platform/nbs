package dataplane

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

type checkIncrementalSnapshotsFixture struct {
	ctx        context.Context
	storage    *storage_mocks.StorageMock
	nbsFactory *nbs_mocks.FactoryMock
	nbsClient  *nbs_mocks.ClientMock
	execCtx    *tasks_mocks.ExecutionContextMock
	task       *checkIncrementalSnapshotsTask
}

func newCheckIncrementalSnapshotsFixture(
	pageSize int,
) *checkIncrementalSnapshotsFixture {

	f := &checkIncrementalSnapshotsFixture{
		ctx:        newContext(),
		storage:    storage_mocks.NewStorageMock(),
		nbsFactory: nbs_mocks.NewFactoryMock(),
		nbsClient:  nbs_mocks.NewClientMock(),
		execCtx:    tasks_mocks.NewExecutionContextMock(),
	}

	f.task = &checkIncrementalSnapshotsTask{
		storage:    f.storage,
		nbsFactory: f.nbsFactory,
		registry:   metrics.NewEmptyRegistry(),
		pageSize:   pageSize,
		state:      &protos.CheckIncrementalSnapshotsTaskState{},
	}

	f.execCtx.On("SaveState", f.ctx).Return(nil)
	return f
}

func (f *checkIncrementalSnapshotsFixture) assertExpectations(t *testing.T) {
	mock.AssertExpectationsForObjects(
		t,
		f.storage,
		f.nbsFactory,
		f.nbsClient,
		f.execCtx,
	)
}

func matchDisk(zoneID string, diskID string) interface{} {
	return mock.MatchedBy(func(disk *types.Disk) bool {
		if len(diskID) == 0 {
			return disk == nil
		}

		return disk != nil && disk.ZoneId == zoneID && disk.DiskId == diskID
	})
}

func incrementalEntry(
	zoneID string,
	diskID string,
	snapshotID string,
	createdAt int64,
) storage.IncrementalEntry {

	return storage.IncrementalEntry{
		Disk:         &types.Disk{ZoneId: zoneID, DiskId: diskID},
		SnapshotID:   snapshotID,
		CheckpointID: snapshotID + "-checkpoint",
		CreatedAt:    time.Unix(createdAt, 0),
	}
}

// Expects the listing of the whole incremental table in one page.
func (f *checkIncrementalSnapshotsFixture) expectList(
	entries ...storage.IncrementalEntry,
) {

	f.storage.On(
		"ListIncremental",
		f.ctx,
		matchDisk("", ""),
		f.task.pageSize,
	).Return(entries, nil).Once()
}

func (f *checkIncrementalSnapshotsFixture) expectSnapshot(
	snapshotID string,
	ready bool,
) {

	f.storage.On("GetSnapshotMeta", f.ctx, snapshotID).Return(
		&storage.SnapshotMeta{ID: snapshotID, Ready: ready},
		nil,
	).Once()
}

func (f *checkIncrementalSnapshotsFixture) expectBase(
	zoneID string,
	diskID string,
	snapshotID string,
) {

	f.storage.On(
		"GetIncremental",
		f.ctx,
		matchDisk(zoneID, diskID),
	).Return(snapshotID, snapshotID+"-checkpoint", nil).Once()
}

func (f *checkIncrementalSnapshotsFixture) expectDisk(
	zoneID string,
	diskID string,
	checkpoints ...string,
) {

	f.nbsFactory.On("GetClient", f.ctx, zoneID).Return(f.nbsClient, nil)
	f.nbsClient.On("GetCheckpoints", f.ctx, diskID).Return(
		checkpoints,
		nil,
	).Once()
}

// The disk is described only if the checkpoint of its base snapshot is missing.
func (f *checkIncrementalSnapshotsFixture) expectDescribe(
	diskID string,
	diskParams nbs.DiskParams,
	err error,
) {

	f.nbsClient.On("Describe", f.ctx, diskID).Return(diskParams, err).Once()
}

func (f *checkIncrementalSnapshotsFixture) requireProblems(
	t *testing.T,
	expected map[string]uint64,
	expectedDisks ...*protos.DiskIncrementalSnapshotsProblems,
) {

	state := f.task.state
	if len(expected) == 0 {
		require.Empty(t, state.ProblemCounts)
	} else {
		require.Equal(t, expected, state.ProblemCounts)
	}

	require.Len(t, state.DisksWithProblems, len(expectedDisks))
	for i, expectedDisk := range expectedDisks {
		actual := state.DisksWithProblems[i]
		require.Equal(t, expectedDisk.Disk.ZoneId, actual.Disk.ZoneId)
		require.Equal(t, expectedDisk.Disk.DiskId, actual.Disk.DiskId)
		require.Equal(t, expectedDisk.BaseSnapshotId, actual.BaseSnapshotId)
		require.Equal(t, expectedDisk.BaseCheckpointId, actual.BaseCheckpointId)
		require.Equal(t, expectedDisk.Problems, actual.Problems)
	}
}

func diskProblems(
	zoneID string,
	diskID string,
	baseSnapshotID string,
	problems ...string,
) *protos.DiskIncrementalSnapshotsProblems {

	return &protos.DiskIncrementalSnapshotsProblems{
		Disk:             &types.Disk{ZoneId: zoneID, DiskId: diskID},
		BaseSnapshotId:   baseSnapshotID,
		BaseCheckpointId: baseSnapshotID + "-checkpoint",
		Problems:         problems,
	}
}

func newNbsNotFoundError() error {
	return errors.NewSilentNonRetriableError(
		&nbs_client.ClientError{Code: nbs_client.E_NOT_FOUND},
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestCheckIncrementalSnapshotsTaskNoDisks(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(10)
	f.expectList()

	err := f.task.Run(f.ctx, f.execCtx)
	require.NoError(t, err)
	require.Zero(t, f.task.state.CheckedDiskCount)
	f.requireProblems(t, nil)
	f.assertExpectations(t)
}

func TestCheckIncrementalSnapshotsTaskHealthyDisk(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(10)
	f.expectList(incrementalEntry("zone-a", "disk", "snapshot", 1))
	f.expectSnapshot("snapshot", true)
	// Checkpoints of other snapshots do not matter.
	f.expectDisk("zone-a", "disk", "other-checkpoint", "snapshot-checkpoint")

	err := f.task.Run(f.ctx, f.execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 1, f.task.state.CheckedDiskCount)
	require.Zero(t, f.task.state.SkippedDiskCount)
	f.requireProblems(t, nil)
	f.assertExpectations(t)
}

func TestCheckIncrementalSnapshotsTaskBaseCheckpointNotFound(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(10)
	f.expectList(incrementalEntry("zone-a", "disk", "snapshot", 1))
	f.expectSnapshot("snapshot", true)
	f.expectDisk("zone-a", "disk", "other-checkpoint")
	f.expectDescribe("disk", nbs.DiskParams{}, nil)
	f.expectBase("zone-a", "disk", "snapshot")

	err := f.task.Run(f.ctx, f.execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 1, f.task.state.CheckedDiskCount)
	f.requireProblems(
		t,
		map[string]uint64{incrementalProblemBaseCheckpointNotFound: 1},
		diskProblems(
			"zone-a",
			"disk",
			"snapshot",
			incrementalProblemBaseCheckpointNotFound,
		),
	)
	f.assertExpectations(t)
}

// Checkpoint of a snapshot is deleted right after the next snapshot of the
// disk replaces it in the incremental table.
func TestCheckIncrementalSnapshotsTaskChainChangedDuringCheck(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(10)
	f.expectList(incrementalEntry("zone-a", "disk", "snapshot", 1))
	f.expectSnapshot("snapshot", true)
	f.expectDisk("zone-a", "disk", "next-snapshot-checkpoint")
	f.expectDescribe("disk", nbs.DiskParams{}, nil)
	f.expectBase("zone-a", "disk", "next-snapshot")

	err := f.task.Run(f.ctx, f.execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 1, f.task.state.CheckedDiskCount)
	f.requireProblems(t, nil)
	f.assertExpectations(t)
}

func TestCheckIncrementalSnapshotsTaskDiskRegistryBasedDisk(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(10)
	f.expectList(incrementalEntry("zone-a", "disk", "snapshot", 1))
	f.expectSnapshot("snapshot", true)
	// Checkpoints of such disks are not kept.
	f.expectDisk("zone-a", "disk")
	f.expectDescribe(
		"disk",
		nbs.DiskParams{IsDiskRegistryBasedDisk: true},
		nil,
	)

	err := f.task.Run(f.ctx, f.execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 1, f.task.state.CheckedDiskCount)
	f.requireProblems(t, nil)
	f.assertExpectations(t)
}

func TestCheckIncrementalSnapshotsTaskDiskNotFound(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(10)
	f.expectList(incrementalEntry("zone-a", "disk", "snapshot", 1))
	f.expectSnapshot("snapshot", true)
	f.nbsFactory.On("GetClient", f.ctx, "zone-a").Return(f.nbsClient, nil)
	f.nbsClient.On("GetCheckpoints", f.ctx, "disk").Return(
		nil,
		newNbsNotFoundError(),
	).Once()
	f.expectBase("zone-a", "disk", "snapshot")

	err := f.task.Run(f.ctx, f.execCtx)
	require.NoError(t, err)
	f.requireProblems(
		t,
		map[string]uint64{incrementalProblemDiskNotFound: 1},
		diskProblems(
			"zone-a",
			"disk",
			"snapshot",
			incrementalProblemDiskNotFound,
		),
	)
	f.assertExpectations(t)
}

// The disk is deleted after its checkpoints are received.
func TestCheckIncrementalSnapshotsTaskDiskDeletedDuringCheck(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(10)
	f.expectList(incrementalEntry("zone-a", "disk", "snapshot", 1))
	f.expectSnapshot("snapshot", true)
	f.expectDisk("zone-a", "disk")
	f.expectDescribe("disk", nbs.DiskParams{}, newNbsNotFoundError())
	f.expectBase("zone-a", "disk", "snapshot")

	err := f.task.Run(f.ctx, f.execCtx)
	require.NoError(t, err)
	f.requireProblems(
		t,
		map[string]uint64{incrementalProblemDiskNotFound: 1},
		diskProblems(
			"zone-a",
			"disk",
			"snapshot",
			incrementalProblemDiskNotFound,
		),
	)
	f.assertExpectations(t)
}

func TestCheckIncrementalSnapshotsTaskBaseSnapshotNotReady(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(10)
	f.expectList(
		incrementalEntry("zone-a", "disk1", "snapshot1", 1),
		incrementalEntry("zone-a", "disk2", "snapshot2", 1),
	)
	// The checkpoint is not needed, so NBS should not be requested.
	f.storage.On("GetSnapshotMeta", f.ctx, "snapshot1").Return(
		(*storage.SnapshotMeta)(nil),
		nil,
	).Once()
	f.expectBase("zone-a", "disk1", "snapshot1")
	f.expectSnapshot("snapshot2", false)
	f.expectBase("zone-a", "disk2", "snapshot2")

	err := f.task.Run(f.ctx, f.execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 2, f.task.state.CheckedDiskCount)
	f.requireProblems(
		t,
		map[string]uint64{incrementalProblemBaseSnapshotNotReady: 2},
		diskProblems(
			"zone-a",
			"disk1",
			"snapshot1",
			incrementalProblemBaseSnapshotNotReady,
		),
		diskProblems(
			"zone-a",
			"disk2",
			"snapshot2",
			incrementalProblemBaseSnapshotNotReady,
		),
	)
	f.assertExpectations(t)
}

func TestCheckIncrementalSnapshotsTaskMultipleHeads(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(10)
	// Rows are ordered by snapshot ID, the latest snapshot goes first.
	f.expectList(
		incrementalEntry("zone-a", "disk", "snapshot-a", 2),
		incrementalEntry("zone-a", "disk", "snapshot-b", 1),
	)
	// The base is requested to find out which head is used and then to confirm
	// the problems.
	f.storage.On(
		"GetIncremental",
		f.ctx,
		matchDisk("zone-a", "disk"),
	).Return("snapshot-b", "snapshot-b-checkpoint", nil).Twice()
	f.expectSnapshot("snapshot-b", true)
	f.expectDisk(
		"zone-a",
		"disk",
		"snapshot-a-checkpoint",
		"snapshot-b-checkpoint",
	)

	err := f.task.Run(f.ctx, f.execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 1, f.task.state.CheckedDiskCount)
	f.requireProblems(
		t,
		map[string]uint64{
			incrementalProblemMultipleHeads:           1,
			incrementalProblemBaseSnapshotIsNotLatest: 1,
		},
		diskProblems(
			"zone-a",
			"disk",
			"snapshot-b",
			incrementalProblemMultipleHeads,
			incrementalProblemBaseSnapshotIsNotLatest,
		),
	)
	f.assertExpectations(t)
}

func TestCheckIncrementalSnapshotsTaskMultipleHeadsLatestIsBase(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(10)
	f.expectList(
		incrementalEntry("zone-a", "disk", "snapshot-a", 1),
		incrementalEntry("zone-a", "disk", "snapshot-b", 2),
	)
	f.storage.On(
		"GetIncremental",
		f.ctx,
		matchDisk("zone-a", "disk"),
	).Return("snapshot-b", "snapshot-b-checkpoint", nil).Twice()
	f.expectSnapshot("snapshot-b", true)
	// The checkpoint of the head that is not used is not needed.
	f.expectDisk("zone-a", "disk", "snapshot-b-checkpoint")

	err := f.task.Run(f.ctx, f.execCtx)
	require.NoError(t, err)
	f.requireProblems(
		t,
		map[string]uint64{incrementalProblemMultipleHeads: 1},
		diskProblems(
			"zone-a",
			"disk",
			"snapshot-b",
			incrementalProblemMultipleHeads,
		),
	)
	f.assertExpectations(t)
}

func TestCheckIncrementalSnapshotsTaskDisabledZone(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(10)
	f.task.disabledZones = common.NewDisabledZones([]string{"zone-a"})
	f.expectList(
		incrementalEntry("zone-a", "disk1", "snapshot1", 1),
		incrementalEntry("zone-b", "disk2", "snapshot2", 1),
	)
	f.expectSnapshot("snapshot1", true)
	f.expectSnapshot("snapshot2", true)
	f.expectDisk("zone-b", "disk2", "snapshot2-checkpoint")

	err := f.task.Run(f.ctx, f.execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 2, f.task.state.CheckedDiskCount)
	require.EqualValues(t, 1, f.task.state.SkippedDiskCount)
	f.requireProblems(t, nil)
	f.assertExpectations(t)
}

// Requests to an unavailable zone should not be repeated for every disk of the
// zone.
func TestCheckIncrementalSnapshotsTaskUnavailableZone(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(10)
	f.expectList(
		incrementalEntry("zone-a", "disk1", "snapshot1", 1),
		incrementalEntry("zone-a", "disk2", "snapshot2", 1),
		incrementalEntry("zone-a", "disk3", "snapshot3", 1),
		incrementalEntry("zone-a", "disk4", "snapshot4", 1),
		incrementalEntry("zone-b", "disk5", "snapshot5", 1),
	)
	for _, snapshotID := range []string{
		"snapshot1", "snapshot2", "snapshot3", "snapshot4", "snapshot5",
	} {
		f.expectSnapshot(snapshotID, true)
	}

	f.nbsFactory.On("GetClient", f.ctx, "zone-a").Return(f.nbsClient, nil)
	for _, diskID := range []string{"disk1", "disk2", "disk3"} {
		f.nbsClient.On("GetCheckpoints", f.ctx, diskID).Return(
			nil,
			errors.NewRetriableError(assert.AnError),
		).Once()
	}
	f.expectDisk("zone-b", "disk5", "snapshot5-checkpoint")

	err := f.task.Run(f.ctx, f.execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 5, f.task.state.CheckedDiskCount)
	require.EqualValues(t, 4, f.task.state.SkippedDiskCount)
	f.requireProblems(t, nil)
	f.assertExpectations(t)
}

// A failed check of a disk should not affect the rest of the disks of the
// zone.
func TestCheckIncrementalSnapshotsTaskNbsErrorForOneDisk(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(10)
	f.expectList(
		incrementalEntry("zone-a", "disk1", "snapshot1", 1),
		incrementalEntry("zone-a", "disk2", "snapshot2", 1),
	)
	f.expectSnapshot("snapshot1", true)
	f.expectSnapshot("snapshot2", true)

	f.expectDisk("zone-a", "disk1")
	f.expectDescribe(
		"disk1",
		nbs.DiskParams{},
		errors.NewRetriableError(assert.AnError),
	)
	f.expectDisk("zone-a", "disk2")
	f.expectDescribe("disk2", nbs.DiskParams{}, nil)
	f.expectBase("zone-a", "disk2", "snapshot2")

	err := f.task.Run(f.ctx, f.execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 2, f.task.state.CheckedDiskCount)
	require.EqualValues(t, 1, f.task.state.SkippedDiskCount)
	f.requireProblems(
		t,
		map[string]uint64{incrementalProblemBaseCheckpointNotFound: 1},
		diskProblems(
			"zone-a",
			"disk2",
			"snapshot2",
			incrementalProblemBaseCheckpointNotFound,
		),
	)
	f.assertExpectations(t)
}

func TestCheckIncrementalSnapshotsTaskPagination(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(3)

	// The page is full and ends with a part of the rows of disk2, so disk2 is
	// left for the next page.
	f.storage.On("ListIncremental", f.ctx, matchDisk("", ""), 3).Return(
		[]storage.IncrementalEntry{
			incrementalEntry("zone-a", "disk1", "snapshot1", 1),
			incrementalEntry("zone-a", "disk2", "snapshot2-a", 1),
			incrementalEntry("zone-a", "disk2", "snapshot2-b", 2),
		},
		nil,
	).Once()
	// All rows of the page belong to the same disk: it is checked with these
	// rows only.
	f.storage.On(
		"ListIncremental",
		f.ctx,
		matchDisk("zone-a", "disk1"),
		3,
	).Return(
		[]storage.IncrementalEntry{
			incrementalEntry("zone-a", "disk2", "snapshot2-a", 1),
			incrementalEntry("zone-a", "disk2", "snapshot2-b", 2),
			incrementalEntry("zone-a", "disk2", "snapshot2-c", 3),
		},
		nil,
	).Once()
	f.storage.On(
		"ListIncremental",
		f.ctx,
		matchDisk("zone-a", "disk2"),
		3,
	).Return(
		[]storage.IncrementalEntry{
			incrementalEntry("zone-b", "disk3", "snapshot3", 1),
		},
		nil,
	).Once()

	f.expectSnapshot("snapshot1", true)
	f.expectDisk("zone-a", "disk1", "snapshot1-checkpoint")

	f.storage.On(
		"GetIncremental",
		f.ctx,
		matchDisk("zone-a", "disk2"),
	).Return("snapshot2-c", "snapshot2-c-checkpoint", nil).Twice()
	f.expectSnapshot("snapshot2-c", true)
	f.expectDisk("zone-a", "disk2", "snapshot2-c-checkpoint")

	f.expectSnapshot("snapshot3", true)
	f.expectDisk("zone-b", "disk3", "snapshot3-checkpoint")

	err := f.task.Run(f.ctx, f.execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 3, f.task.state.CheckedDiskCount)
	require.Equal(t, "zone-b", f.task.state.LastCheckedDisk.ZoneId)
	require.Equal(t, "disk3", f.task.state.LastCheckedDisk.DiskId)
	f.requireProblems(
		t,
		map[string]uint64{incrementalProblemMultipleHeads: 1},
		diskProblems(
			"zone-a",
			"disk2",
			"snapshot2-c",
			incrementalProblemMultipleHeads,
		),
	)
	f.execCtx.AssertNumberOfCalls(t, "SaveState", 3)
	f.assertExpectations(t)
}

// The task continues from the disk saved in the state.
func TestCheckIncrementalSnapshotsTaskResume(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(10)
	f.task.state = &protos.CheckIncrementalSnapshotsTaskState{
		LastCheckedDisk:  &types.Disk{ZoneId: "zone-a", DiskId: "disk1"},
		CheckedDiskCount: 1,
	}

	state, err := f.task.Save()
	require.NoError(t, err)
	err = f.task.Load(nil, state)
	require.NoError(t, err)

	f.storage.On(
		"ListIncremental",
		f.ctx,
		matchDisk("zone-a", "disk1"),
		10,
	).Return(
		[]storage.IncrementalEntry{
			incrementalEntry("zone-a", "disk2", "snapshot2", 1),
		},
		nil,
	).Once()
	f.expectSnapshot("snapshot2", true)
	f.expectDisk("zone-a", "disk2", "snapshot2-checkpoint")

	err = f.task.Run(f.ctx, f.execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 2, f.task.state.CheckedDiskCount)
	f.requireProblems(t, nil)
	f.assertExpectations(t)
}

func TestCheckIncrementalSnapshotsTaskStorageError(t *testing.T) {
	f := newCheckIncrementalSnapshotsFixture(10)
	f.execCtx = tasks_mocks.NewExecutionContextMock()
	f.expectList(incrementalEntry("zone-a", "disk", "snapshot", 1))
	f.storage.On("GetSnapshotMeta", f.ctx, "snapshot").Return(
		(*storage.SnapshotMeta)(nil),
		assert.AnError,
	).Once()

	err := f.task.Run(f.ctx, f.execCtx)
	require.Equal(t, assert.AnError, err)
	require.Nil(t, f.task.state.LastCheckedDisk)
	f.assertExpectations(t)
}
