package dataplane

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

func newInheritanceScheduler(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	snapshotStorage snapshot_storage.Storage,
) (tasks.Scheduler, tasks_storage.Storage) {

	config := &tasks_config.TasksConfig{
		StorageFolder:                 proto.String(t.Name() + "/tasks"),
		RunnersCount:                  proto.Uint64(4),
		StalkingRunnersCount:          proto.Uint64(4),
		PollForTaskUpdatesPeriod:      proto.String("10ms"),
		PollForTasksPeriodMin:         proto.String("10ms"),
		PollForTasksPeriodMax:         proto.String("20ms"),
		PollForStallingTasksPeriodMin: proto.String("10ms"),
		PollForStallingTasksPeriodMax: proto.String("20ms"),
		TaskPingPeriod:                proto.String("100ms"),
		TaskStallingTimeout:           proto.String("1s"),
		TaskWaitingTimeout:            proto.String("100ms"),
		RegularSystemTasksEnabled:     proto.Bool(false),
	}
	registry := tasks.NewRegistry()
	metricsRegistry := metrics.NewEmptyRegistry()
	err := tasks_storage.CreateYDBTables(ctx, config, db, false)
	require.NoError(t, err)
	taskStorage, err := tasks_storage.NewStorage(config, metricsRegistry, db)
	require.NoError(t, err)
	scheduler, err := tasks.NewScheduler(
		ctx,
		registry,
		taskStorage,
		config,
		metricsRegistry,
	)
	require.NoError(t, err)

	err = registry.RegisterForExecution(
		"dataplane.EstablishSnapshotInheritance",
		func() tasks.Task {
			return &establishSnapshotInheritanceTask{
				storage:   snapshotStorage,
				batchSize: 2,
			}
		},
	)
	require.NoError(t, err)
	err = registry.RegisterForExecution(
		"dataplane.EstablishSnapshotInheritanceForAllSnapshots",
		func() tasks.Task {
			return &establishSnapshotInheritanceForAllSnapshotsTask{
				storage:       snapshotStorage,
				scheduler:     scheduler,
				inflightLimit: 4,
			}
		},
	)
	require.NoError(t, err)
	err = tasks.StartRunners(
		ctx,
		taskStorage,
		registry,
		metricsRegistry,
		config,
		"localhost",
	)
	require.NoError(t, err)

	return scheduler, taskStorage
}

func waitForInheritanceReadinessRetries(
	t *testing.T,
	ctx context.Context,
	taskStorage tasks_storage.Storage,
	taskID string,
	snapshotIDs []string,
) []string {

	wanted := make(map[string]bool)
	for _, snapshotID := range snapshotIDs {
		wanted[snapshotID] = true
	}

	// Initial creation uses generation zero, and the first worker lock uses
	// generation one. Pings do not advance it. A second running generation
	// proves that a worker already yielded while waiting for readiness.
	var childTaskIDs []string
	require.Eventually(t, func() bool {
		state, err := taskStorage.GetTask(ctx, taskID)
		if err != nil || tasks_storage.HasResult(state.Status) {
			return false
		}
		var scan protos.EstablishSnapshotInheritanceForAllSnapshotsTaskState
		if proto.Unmarshal(state.State, &scan) != nil {
			return false
		}
		var waiting []string
		for _, child := range scan.InflightSnapshots {
			if !wanted[child.SnapshotId] {
				continue
			}
			childState, err := taskStorage.GetTask(ctx, child.TaskId)
			if err == nil && childState.GenerationID >= 2 &&
				childState.Status == tasks_storage.TaskStatusRunning {
				waiting = append(waiting, child.TaskId)
			}
		}
		if len(waiting) != len(snapshotIDs) {
			return false
		}
		childTaskIDs = waiting
		return true
	}, 30*time.Second, 20*time.Millisecond)
	return childTaskIDs
}

func TestEstablishSnapshotInheritanceForAllSnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(newContext(), 2*time.Minute)
	defer cancel()

	storage, closeStorage := newStorage(t, ctx)
	defer closeStorage()
	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	clearParents := func(snapshotID string) {
		// Simulate snapshots written before inheritance tracking was enabled.
		_, err := db.ExecuteRW(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $snapshot_id as Utf8;

			delete from snapshot_parents where snapshot_id = $snapshot_id;
		`, db.AbsolutePath("dataplane_tasks_tests/"+t.Name())),
			persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		)
		require.NoError(t, err)
		parents, err := storage.GetSnapshotParents(ctx, snapshotID)
		require.NoError(t, err)
		require.Empty(t, parents)
	}

	expectedParents := make(map[string][]string)
	var creators []string
	var previousSnapshotID string
	var chunkCount uint32
	for i := 0; i < 10; i++ {
		index := i + 2
		if i == 0 {
			index = 0
		}
		snapshotID := fmt.Sprintf("snapshot-%02d", index)
		_, err = storage.CreateSnapshot(ctx, snapshot_storage.SnapshotMeta{
			ID:           snapshotID,
			CreateTaskID: "create-" + snapshotID,
		})
		require.NoError(t, err)
		if previousSnapshotID != "" {
			err = storage.ShallowCopySnapshot(ctx, previousSnapshotID, snapshotID, 0, nil)
			require.NoError(t, err)
		}
		expectedParents[snapshotID] = append([]string(nil), creators...)

		// Alternate snapshots with new data and copies that own no chunks.
		// Descendants must reference chunk creators, skipping pure copies.
		if i%2 == 0 {
			_, err = storage.WriteChunk(
				ctx,
				"create-"+snapshotID,
				snapshotID,
				dataplane_common.Chunk{Index: chunkCount, Data: []byte(snapshotID)},
				false,
			)
			require.NoError(t, err)
			creators = append(creators, snapshotID)
			chunkCount++
		}
		clearParents(snapshotID)
		err = storage.SnapshotCreated(ctx, snapshotID, uint64(chunkCount)*4096, 0, chunkCount, nil)
		require.NoError(t, err)
		previousSnapshotID = snapshotID
	}

	// These IDs sort into the first batch, so the all-snapshots task must
	// remain active until their creation finishes.
	creatingSnapshotIDs := []string{"snapshot-01", "snapshot-02"}
	for _, snapshotID := range creatingSnapshotIDs {
		_, err = storage.CreateSnapshot(ctx, snapshot_storage.SnapshotMeta{
			ID:           snapshotID,
			CreateTaskID: "create-" + snapshotID,
		})
		require.NoError(t, err)
		err = storage.ShallowCopySnapshot(ctx, previousSnapshotID, snapshotID, 0, nil)
		require.NoError(t, err)
		clearParents(snapshotID)
		expectedParents[snapshotID] = append([]string(nil), creators...)
	}

	runnerCtx, stopRunners := context.WithCancel(ctx)
	defer stopRunners()
	scheduler, taskStorage := newInheritanceScheduler(t, runnerCtx, db, storage)
	scheduleAll := func() (string, error) {
		return scheduler.ScheduleNonCancellableTask(
			headers.SetIncomingIdempotencyKey(
				ctx,
				"dataplane.EstablishSnapshotInheritanceForAllSnapshots",
			),
			"dataplane.EstablishSnapshotInheritanceForAllSnapshots",
			"",
			"",
			&empty.Empty{},
		)
	}
	taskID, err := scheduleAll()
	require.NoError(t, err)

	waitForInheritanceReadinessRetries(t, ctx, taskStorage, taskID, creatingSnapshotIDs)

	repeatedTaskID, err := scheduleAll()
	require.NoError(t, err)
	require.Equal(t, taskID, repeatedTaskID)

	_, err = scheduler.CancelTask(ctx, taskID)
	require.ErrorContains(t, err, "non-cancellable")
	for _, snapshotID := range creatingSnapshotIDs {
		meta, err := storage.GetSnapshotMeta(ctx, snapshotID)
		require.NoError(t, err)
		require.NotNil(t, meta)
		require.False(t, meta.Ready)
		parents, err := storage.GetSnapshotParents(ctx, snapshotID)
		require.NoError(t, err)
		require.Empty(t, parents, "inheritance must not be restored before readiness")
	}
	for _, snapshotID := range creatingSnapshotIDs {
		err = storage.SnapshotCreated(ctx, snapshotID, uint64(chunkCount)*4096, 0, chunkCount, nil)
		require.NoError(t, err)
	}

	_, err = scheduler.WaitTaskSync(ctx, taskID, time.Minute)
	require.NoError(t, err)
	repeatedTaskID, err = scheduleAll()
	require.NoError(t, err)
	require.Equal(t, taskID, repeatedTaskID)
	for snapshotID, expected := range expectedParents {
		parents, err := storage.GetSnapshotParents(ctx, snapshotID)
		require.NoError(t, err)
		require.ElementsMatch(t, expected, parents, snapshotID)
	}

	// Force-finishing the coordinator must leave its children running so that
	// they can finish restoring inheritance when their snapshots become ready.
	const pendingSnapshotID = "snapshot-12"
	_, err = storage.CreateSnapshot(ctx, snapshot_storage.SnapshotMeta{
		ID:           pendingSnapshotID,
		CreateTaskID: "create-" + pendingSnapshotID,
	})
	require.NoError(t, err)
	err = storage.ShallowCopySnapshot(ctx, previousSnapshotID, pendingSnapshotID, 0, nil)
	require.NoError(t, err)
	clearParents(pendingSnapshotID)

	forcedTaskID, err := scheduler.ScheduleNonCancellableTask(
		headers.SetIncomingIdempotencyKey(ctx, t.Name()+"_force_finish"),
		"dataplane.EstablishSnapshotInheritanceForAllSnapshots",
		"",
		"",
		&empty.Empty{},
	)
	require.NoError(t, err)
	childTaskIDs := waitForInheritanceReadinessRetries(
		t,
		ctx,
		taskStorage,
		forcedTaskID,
		[]string{pendingSnapshotID},
	)
	_, err = scheduler.CancelTask(ctx, forcedTaskID)
	require.ErrorContains(t, err, "non-cancellable")
	err = taskStorage.ForceFinishTask(ctx, forcedTaskID)
	require.NoError(t, err)
	_, err = scheduler.WaitTaskSync(ctx, forcedTaskID, time.Minute)
	require.NoError(t, err)
	for _, childTaskID := range childTaskIDs {
		child, err := taskStorage.GetTask(ctx, childTaskID)
		require.NoError(t, err)
		require.False(t, tasks_storage.HasResult(child.Status))
	}

	err = storage.SnapshotCreated(ctx, pendingSnapshotID, uint64(chunkCount)*4096, 0, chunkCount, nil)
	require.NoError(t, err)
	for _, childTaskID := range childTaskIDs {
		_, err = scheduler.WaitTaskSync(ctx, childTaskID, time.Minute)
		require.NoError(t, err)
	}
	parents, err := storage.GetSnapshotParents(ctx, pendingSnapshotID)
	require.NoError(t, err)
	require.ElementsMatch(t, creators, parents)
}
