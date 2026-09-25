package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/common"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

func TestStorageYDBMetricsDoNotConsumeQueueTurn(t *testing.T) {
	for _, reads := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprintf("metrics=%d", reads), func(t *testing.T) {
			ctx, s := newDelayedQueueTestStorage(t)

			ordinary := createDelayedQueueTestTask(t, ctx, s, "ordinary", time.Time{})
			due := createDelayedQueueTestTask(t, ctx, s, "due", time.Now().Add(-time.Minute))
			createDelayedQueueTestTask(t, ctx, s, "future", time.Now().Add(time.Hour))

			var selected []string
			for i := 0; i < 6; i++ {
				for j := 0; j < reads; j++ {
					list, err := s.ListTasksWithStatus(ctx, "ready_to_run")
					require.NoError(t, err)
					require.Len(t, list, 2)
				}

				list, err := s.ListTasksReadyToRun(ctx, 1, nil)
				require.NoError(t, err)
				require.Len(t, list, 1)
				selected = append(selected, list[0].ID)
			}

			require.Equal(t, []string{due, ordinary, due, ordinary, due, ordinary}, selected)
		})
	}
}

func TestStorageYDBEarlyCancellationKeepsHangingDeadline(t *testing.T) {
	ctx, s := newDelayedQueueTestStorage(t)
	s.hangingTaskTimeout = time.Hour

	now := time.Now().UTC().Truncate(time.Microsecond)
	requestedAt := now.Add(-20 * time.Minute)

	id, err := s.CreateTask(ctx, TaskState{
		IdempotencyKey: "cancel",
		TaskType:       "test",
		Status:         TaskStatusReadyToRun,
		CreatedAt:      now.Add(-time.Hour),
		ModifiedAt:     now.Add(-time.Hour),
		AvailableAt:    now.Add(time.Hour),
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)

	_, err = s.MarkForCancellation(ctx, id, requestedAt)
	require.NoError(t, err)

	_, err = s.MarkForCancellation(ctx, id, now)
	require.NoError(t, err)

	state, err := s.GetTask(ctx, id)
	require.NoError(t, err)
	require.WithinDuration(t, requestedAt, state.CancelRequestedAt, 0)
	require.True(t, state.FirstRunStartedAt.IsZero())

	list, err := s.ListHangingTasks(ctx, 100)
	require.NoError(t, err)
	require.Empty(t, list) // the default one-hour timeout has not elapsed

	s.hangingTaskTimeoutByType = map[string]time.Duration{"test": 10 * time.Minute}
	list, err = s.ListHangingTasks(ctx, 100)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, id, list[0].ID)

	for _, host := range []string{"first", "after-restart"} {
		state, err = s.LockTaskToCancel(
			ctx,
			TaskInfo{ID: id, GenerationID: state.GenerationID},
			now,
			host,
			"runner",
		)
		require.NoError(t, err)
		require.WithinDuration(t, requestedAt, state.CancelRequestedAt, 0)
		require.True(t, state.FirstRunStartedAt.IsZero())

		// A caller must not be able to reset the cancellation deadline.
		state.CancelRequestedAt = now.Add(time.Hour)
		state.ModifiedAt = now
		state, err = s.UpdateTask(ctx, state)
		require.NoError(t, err)
		require.WithinDuration(t, requestedAt, state.CancelRequestedAt, 0)

		list, err = s.ListHangingTasks(ctx, 100)
		require.NoError(t, err)
		require.Len(t, list, 1)
	}

	// Waiting on a dependency and being woken up must preserve the same clock.
	dependency := createDelayedQueueTestTask(t, ctx, s, "dependency", time.Time{})
	state.Dependencies.Add(dependency)

	_, err = s.UpdateTask(ctx, state)
	require.Error(t, err) // the runner is interrupted after entering WaitingToCancel

	state, err = s.GetTask(ctx, id)
	require.NoError(t, err)
	require.Equal(t, TaskStatusWaitingToCancel, state.Status)
	require.WithinDuration(t, requestedAt, state.CancelRequestedAt, 0)

	dep, err := s.GetTask(ctx, dependency)
	require.NoError(t, err)
	dep.Status = TaskStatusFinished
	dep.ModifiedAt = now

	_, err = s.UpdateTask(ctx, dep)
	require.NoError(t, err)

	state, err = s.GetTask(ctx, id)
	require.NoError(t, err)
	require.Equal(t, TaskStatusReadyToCancel, state.Status)
	require.WithinDuration(t, requestedAt, state.CancelRequestedAt, 0)
}

func TestStorageYDBCancellationTimestampFromUpdateAndLegacyWriter(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			ctx, s := newDelayedQueueTestStorage(t)
			now := time.Now().UTC().Truncate(time.Microsecond)
			id := createDelayedQueueTestTask(t, ctx, s, "cancel", now.Add(time.Hour))
			at := now.Add(-time.Minute)

			state, err := s.GetTask(ctx, id)
			require.NoError(t, err)
			state.Status = TaskStatusReadyToCancel
			state.ModifiedAt = at

			state, err = s.UpdateTask(ctx, state)
			require.NoError(t, err)
			require.WithinDuration(t, at, state.CancelRequestedAt, 0)

			if legacy {
				res, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
					--!syntax_v1
					pragma TablePathPrefix = "%v";
					declare $id as Utf8;
					UPDATE tasks SET cancel_requested_at = NULL WHERE id = $id;
				`, s.tablesPath),
					persistence.ValueParam("$id", persistence.UTF8Value(id)),
				)
				require.NoError(t, err)
				res.Close()
			}

			state, err = s.GetTask(ctx, id)
			require.NoError(t, err)

			state, err = s.LockTaskToCancel(
				ctx,
				TaskInfo{ID: id, GenerationID: state.GenerationID},
				now,
				"new-worker",
				"runner",
			)
			require.NoError(t, err)
			require.WithinDuration(t, at, state.CancelRequestedAt, 0)
			require.WithinDuration(t, now, state.ChangedStateAt, 0)

			state, err = s.GetTask(ctx, id)
			require.NoError(t, err)

			// A fallback that was not persisted would incorrectly return now.
			require.WithinDuration(t, at, state.CancelRequestedAt, 0)
		})
	}
}

func insertDelayedOrphan(t *testing.T, ctx context.Context, s *storageYDB, id string, at time.Time) {
	t.Helper()

	res, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;
		declare $at as Timestamp;
		UPSERT INTO ready_to_run_delayed (available_at, id, generation_id, task_type, zone_id)
		VALUES ($at, $id, 0u, 'test', '');
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(id)),
		persistence.ValueParam("$at", persistence.TimestampValue(at)),
	)
	require.NoError(t, err)
	res.Close()
}

func TestStorageYDBReconciliationIsBoundedAndResumable(t *testing.T) {
	ctx, s := newDelayedQueueTestStorage(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	for i := 1; i <= 3; i++ {
		createDelayedQueueTestTask(t, ctx, s, fmt.Sprint(i), now.Add(time.Duration(i)*time.Hour))
	}

	insertDelayedOrphan(t, ctx, s, "orphan", now.Add(4*time.Hour))

	cursor, err := s.ReconcileReadyToRunDelayed(ctx, 1, DelayedQueueCursor{})
	require.NoError(t, err)
	require.False(t, cursor.Done)
	require.WithinDuration(t, now.Add(time.Hour), cursor.After.AvailableAt, 0)
	require.Equal(t, uint64(4), delayedQueueRowCount(t, ctx, s))

	// Insertion after the saved upper bound belongs to the next pass.
	insertDelayedOrphan(t, ctx, s, "later", now.Add(5*time.Hour))

	saved, err := json.Marshal(cursor)
	require.NoError(t, err)

	var restartedCursor DelayedQueueCursor
	require.NoError(t, json.Unmarshal(saved, &restartedCursor))

	restarted, err := NewStorage(&tasks_config.TasksConfig{StorageFolder: &s.folder}, empty.NewRegistry(), s.db)
	require.NoError(t, err)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	unchanged, err := restarted.ReconcileReadyToRunDelayed(cancelled, 1, restartedCursor)
	require.Error(t, err)
	require.Equal(t, restartedCursor, unchanged)

	// Replaying the last confirmed page is also safe.
	_, err = restarted.ReconcileReadyToRunDelayed(
		ctx,
		1,
		DelayedQueueCursor{StorageFolder: s.folder, Upper: cursor.Upper},
	)
	require.NoError(t, err)

	for attempts := 0; !restartedCursor.Done; attempts++ {
		require.Less(t, attempts, 4)
		restartedCursor, err = restarted.ReconcileReadyToRunDelayed(ctx, 1, restartedCursor)
		require.NoError(t, err)
	}

	require.Equal(t, uint64(4), delayedQueueRowCount(t, ctx, s))

	require.NoError(t, reconcileDelayedQueue(ctx, s, 1))
	require.Equal(t, uint64(3), delayedQueueRowCount(t, ctx, s))
}

func TestStorageYDBReconciliationFoldersAreIndependent(t *testing.T) {
	ctx, current := newDelayedQueueTestStorage(t)

	legacy := *current
	legacy.folder = "unavailable-legacy"
	legacy.tablesPath += "/does-not-exist"

	compound := &compoundStorage{
		storageFolder:       current.folder,
		storage:             current,
		legacyStorageFolder: legacy.folder,
		legacyStorage:       &legacy,
	}

	insertDelayedOrphan(t, ctx, current, "orphan", time.Now().Add(time.Hour))

	_, err := compound.ReconcileReadyToRunDelayed(ctx, 1, DelayedQueueCursor{StorageFolder: legacy.folder})
	require.Error(t, err)

	cursor, err := compound.ReconcileReadyToRunDelayed(ctx, 1, DelayedQueueCursor{StorageFolder: current.folder})
	require.NoError(t, err)
	require.True(t, cursor.Done)
	require.Zero(t, delayedQueueRowCount(t, ctx, current))
}

type readyToRunStorageStub struct {
	Storage
	list func(context.Context) ([]TaskInfo, error)
}

func (s *readyToRunStorageStub) ListTasksReadyToRun(
	ctx context.Context,
	_ uint64,
	_ []string,
) ([]TaskInfo, error) {
	return s.list(ctx)
}

func TestCompoundStorageListsHealthyFolderWhenOtherReadRetries(t *testing.T) {
	for _, unavailableLegacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("unavailableLegacy=%v", unavailableLegacy), func(t *testing.T) {
			unavailable := &readyToRunStorageStub{
				list: func(ctx context.Context) ([]TaskInfo, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}
			healthy := &readyToRunStorageStub{
				list: func(context.Context) ([]TaskInfo, error) {
					return []TaskInfo{{ID: "healthy"}}, nil
				},
			}

			compound := &compoundStorage{
				legacyStorageFolder: "legacy",
				storageFolder:       "current",
				listTimeout:         50 * time.Millisecond,
			}
			wantFolder := "current"
			if unavailableLegacy {
				compound.legacyStorage = unavailable
				compound.storage = healthy
			} else {
				compound.legacyStorage = healthy
				compound.storage = unavailable
				wantFolder = "legacy"
			}

			ctx, cancel := context.WithTimeout(newContext(), time.Second)
			defer cancel()
			infos, err := compound.ListTasksReadyToRun(ctx, 1, nil)
			require.NoError(t, err)
			require.Equal(t, []TaskInfo{{ID: "healthy", StorageFolder: wantFolder}}, infos)
		})
	}
}

func TestCompoundStorageAcceptsEmptySuccessfulFolder(t *testing.T) {
	unavailable := &readyToRunStorageStub{
		list: func(ctx context.Context) ([]TaskInfo, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	healthy := &readyToRunStorageStub{
		list: func(context.Context) ([]TaskInfo, error) {
			return nil, nil
		},
	}
	compound := &compoundStorage{
		legacyStorageFolder: "legacy",
		legacyStorage:       healthy,
		storageFolder:       "current",
		storage:             unavailable,
		listTimeout:         50 * time.Millisecond,
	}

	infos, err := compound.ListTasksReadyToRun(newContext(), 1, nil)
	require.NoError(t, err)
	require.Empty(t, infos)
}
