package dataplane

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

// These tests run the task in the test process, it works with NBS and YDB of
// the recipe.

func runCheckIncrementalSnapshotsTask(
	t *testing.T,
	ctx context.Context,
	env *restoreTestEnv,
) *protos.CheckIncrementalSnapshotsTaskState {

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("SaveState", ctx).Return(nil)

	task := &checkIncrementalSnapshotsTask{
		storage:    env.storage,
		nbsFactory: env.nbsFactory,
		registry:   metrics.NewEmptyRegistry(),
		pageSize:   2,
		state:      &protos.CheckIncrementalSnapshotsTaskState{},
	}

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)

	return task.state
}

// Does what snapshot creation does to the checkpoints of the disk and to the
// incremental table.
func createSnapshotOfDiskWithoutData(
	t *testing.T,
	ctx context.Context,
	env *restoreTestEnv,
	disk *types.Disk,
	snapshotID string,
) {

	err := env.nbsClient.CreateCheckpoint(ctx, nbs_client.CheckpointParams{
		DiskID:       disk.DiskId,
		CheckpointID: snapshotID,
	})
	require.NoError(t, err)

	snapshotMeta, err := env.storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{
			ID:           snapshotID,
			Disk:         disk,
			CheckpointID: snapshotID,
			CreateTaskID: "create_" + snapshotID,
		},
	)
	require.NoError(t, err)

	err = env.storage.SnapshotCreated(ctx, snapshotID, 0, 0, 0, nil)
	require.NoError(t, err)

	if len(snapshotMeta.BaseCheckpointID) != 0 {
		err = env.nbsClient.DeleteCheckpoint(
			ctx,
			disk.DiskId,
			snapshotMeta.BaseCheckpointID,
		)
		require.NoError(t, err)
	}

	err = env.nbsClient.DeleteCheckpointData(ctx, disk.DiskId, snapshotID)
	require.NoError(t, err)
}

func getChangedBlocksSinceCheckpoint(
	ctx context.Context,
	env *restoreTestEnv,
	disk *types.Disk,
	checkpointID string,
) error {

	_, err := env.nbsClient.GetChangedBlocks(
		ctx,
		disk.DiskId,
		0, // startIndex
		restoreTestBlocksInChunk,
		checkpointID, // baseCheckpointID
		"",           // checkpointID
		false,        // ignoreBaseDisk
	)
	return err
}

////////////////////////////////////////////////////////////////////////////////

func TestCheckIncrementalSnapshotsTaskWithNbs(t *testing.T) {
	ctx := test.NewContext()

	env, closeFunc := newRestoreTestEnv(t, ctx)
	defer closeFunc()

	healthyDisk := env.createDisk(
		t,
		ctx,
		newRestoreTestID(t, "healthy"),
		restoreTestChunkCount,
		nil, // encryption
	)
	brokenDisk := env.createDisk(
		t,
		ctx,
		newRestoreTestID(t, "broken"),
		restoreTestChunkCount,
		nil, // encryption
	)
	deletedDisk := env.createDisk(
		t,
		ctx,
		newRestoreTestID(t, "deleted"),
		restoreTestChunkCount,
		nil, // encryption
	)

	state := runCheckIncrementalSnapshotsTask(t, ctx, env)
	require.Zero(t, state.CheckedDiskCount)

	// A chain of two snapshots: only the checkpoint of the last one is kept.
	healthySnapshotID := newRestoreTestID(t, "healthy_snapshot2")
	createSnapshotOfDiskWithoutData(
		t,
		ctx,
		env,
		healthyDisk,
		newRestoreTestID(t, "healthy_snapshot1"),
	)
	createSnapshotOfDiskWithoutData(t, ctx, env, healthyDisk, healthySnapshotID)

	brokenSnapshotID := newRestoreTestID(t, "broken_snapshot")
	createSnapshotOfDiskWithoutData(t, ctx, env, brokenDisk, brokenSnapshotID)

	deletedSnapshotID := newRestoreTestID(t, "deleted_snapshot")
	createSnapshotOfDiskWithoutData(t, ctx, env, deletedDisk, deletedSnapshotID)

	checkpoints, err := env.nbsClient.GetCheckpoints(ctx, healthyDisk.DiskId)
	require.NoError(t, err)
	require.Equal(t, []string{healthySnapshotID}, checkpoints)

	state = runCheckIncrementalSnapshotsTask(t, ctx, env)
	require.EqualValues(t, 3, state.CheckedDiskCount)
	require.Zero(t, state.SkippedDiskCount)
	require.Empty(t, state.ProblemCounts)
	require.Empty(t, state.DisksWithProblems)

	for _, disk := range []*types.Disk{healthyDisk, brokenDisk} {
		baseSnapshotID, baseCheckpointID, err := env.storage.GetIncremental(
			ctx,
			disk,
		)
		require.NoError(t, err)
		require.NotEmpty(t, baseSnapshotID)

		err = getChangedBlocksSinceCheckpoint(ctx, env, disk, baseCheckpointID)
		require.NoError(t, err)
	}

	// Break the chains.
	err = env.nbsClient.DeleteCheckpoint(
		ctx,
		brokenDisk.DiskId,
		brokenSnapshotID,
	)
	require.NoError(t, err)

	err = env.nbsClient.Delete(ctx, deletedDisk.DiskId)
	require.NoError(t, err)

	state = runCheckIncrementalSnapshotsTask(t, ctx, env)
	require.EqualValues(t, 3, state.CheckedDiskCount)
	require.Zero(t, state.SkippedDiskCount)
	require.Equal(
		t,
		map[string]uint64{
			incrementalProblemBaseCheckpointNotFound: 1,
			incrementalProblemDiskNotFound:           1,
		},
		state.ProblemCounts,
	)

	problemsByDisk := make(map[string][]string)
	for _, disk := range state.DisksWithProblems {
		require.Equal(t, restoreTestZoneID, disk.Disk.ZoneId)
		problemsByDisk[disk.Disk.DiskId] = disk.Problems
	}
	require.Equal(
		t,
		map[string][]string{
			brokenDisk.DiskId:  {incrementalProblemBaseCheckpointNotFound},
			deletedDisk.DiskId: {incrementalProblemDiskNotFound},
		},
		problemsByDisk,
	)

	// The problem is real: the next snapshot of the broken disk is based on
	// the missing checkpoint, and it is impossible to find out what has
	// changed since it.
	err = getChangedBlocksSinceCheckpoint(
		ctx,
		env,
		brokenDisk,
		brokenSnapshotID,
	)
	require.Error(t, err)
	require.True(t, nbs_client.IsNotFoundError(err))

	err = getChangedBlocksSinceCheckpoint(
		ctx,
		env,
		healthyDisk,
		healthySnapshotID,
	)
	require.NoError(t, err)

	// Deletion of the base snapshot repairs the disk: its next snapshot is
	// full.
	_, err = env.storage.DeletingSnapshot(ctx, brokenSnapshotID, "delete")
	require.NoError(t, err)

	state = runCheckIncrementalSnapshotsTask(t, ctx, env)
	require.EqualValues(t, 2, state.CheckedDiskCount)
	require.Equal(
		t,
		map[string]uint64{incrementalProblemDiskNotFound: 1},
		state.ProblemCounts,
	)
}

// Snapshots of the same disk that are created at the same time have the same
// base, so all of them become heads of the incremental chain of the disk.
func TestCheckIncrementalSnapshotsTaskWithNbsMultipleHeads(t *testing.T) {
	ctx := test.NewContext()

	env, closeFunc := newRestoreTestEnv(t, ctx)
	defer closeFunc()

	disk := env.createDisk(
		t,
		ctx,
		newRestoreTestID(t, "disk"),
		restoreTestChunkCount,
		nil, // encryption
	)

	snapshotIDs := []string{
		newRestoreTestID(t, "snapshot1"),
		newRestoreTestID(t, "snapshot2"),
	}

	for _, snapshotID := range snapshotIDs {
		err := env.nbsClient.CreateCheckpoint(ctx, nbs_client.CheckpointParams{
			DiskID:       disk.DiskId,
			CheckpointID: snapshotID,
		})
		require.NoError(t, err)

		snapshotMeta, err := env.storage.CreateSnapshot(
			ctx,
			snapshot_storage.SnapshotMeta{
				ID:           snapshotID,
				Disk:         disk,
				CheckpointID: snapshotID,
				CreateTaskID: "create_" + snapshotID,
			},
		)
		require.NoError(t, err)
		require.Empty(t, snapshotMeta.BaseSnapshotID)
	}

	for _, snapshotID := range snapshotIDs {
		err := env.storage.SnapshotCreated(ctx, snapshotID, 0, 0, 0, nil)
		require.NoError(t, err)

		err = env.nbsClient.DeleteCheckpointData(ctx, disk.DiskId, snapshotID)
		require.NoError(t, err)
	}

	baseSnapshotID, _, err := env.storage.GetIncremental(ctx, disk)
	require.NoError(t, err)
	require.Contains(t, snapshotIDs, baseSnapshotID)

	state := runCheckIncrementalSnapshotsTask(t, ctx, env)
	require.EqualValues(t, 1, state.CheckedDiskCount)
	require.EqualValues(
		t,
		1,
		state.ProblemCounts[incrementalProblemMultipleHeads],
	)
	require.Zero(
		t,
		state.ProblemCounts[incrementalProblemBaseCheckpointNotFound],
	)
	require.Len(t, state.DisksWithProblems, 1)
	require.Equal(t, disk.DiskId, state.DisksWithProblems[0].Disk.DiskId)
	require.Equal(t, baseSnapshotID, state.DisksWithProblems[0].BaseSnapshotId)

	// Both checkpoints are still on the disk, so the next snapshot can be
	// created whichever head it is based on.
	checkpoints, err := env.nbsClient.GetCheckpoints(ctx, disk.DiskId)
	require.NoError(t, err)
	require.ElementsMatch(t, snapshotIDs, checkpoints)
}
