package snapshots

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	tasks_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type staggeringStorageStub struct {
	storage.Storage
	state *storage.TaskState
}

func (s *staggeringStorageStub) GetTaskByIdempotencyKey(
	context.Context,
	string,
	string,
) (storage.TaskState, error) {

	if s.state == nil {
		return storage.TaskState{}, tasks_errors.NewEmptyNotFoundError()
	}

	return *s.state, nil
}

////////////////////////////////////////////////////////////////////////////////

type staggeringSchedulerStub struct {
	tasks.Scheduler
	create func(tasks.TaskScheduleTiming, proto.Message) (string, error)
}

func (s *staggeringSchedulerStub) ScheduleTask(
	_ context.Context,
	_, _ string,
	request proto.Message,
) (string, error) {

	return s.create(tasks.TaskScheduleTiming{}, request)
}

func (s *staggeringSchedulerStub) ScheduleTaskAt(
	_ context.Context,
	_, _ string,
	timing tasks.TaskScheduleTiming,
	request proto.Message,
) (string, error) {

	return s.create(timing, request)
}

////////////////////////////////////////////////////////////////////////////////

func TestSnapshotStaggeringConfig(t *testing.T) {
	for _, value := range []string{"0s", "5m"} {
		_, err := NewService(nil, nil, &config.SnapshotsConfig{
			CreateSnapshotStaggeringWindow: proto.String(value),
		})
		require.NoError(t, err)
	}

	for _, value := range []string{"", "wrong", "-1s"} {
		_, err := NewService(nil, nil, &config.SnapshotsConfig{
			CreateSnapshotStaggeringWindow: proto.String(value),
		})
		require.Error(t, err)
	}

	value, err := parseCreateSnapshotStaggeringWindow(
		(&config.SnapshotsConfig{}).GetCreateSnapshotStaggeringWindow(),
	)
	require.NoError(t, err)
	require.Equal(t, 5*time.Minute, value)
}

func TestSnapshotStaggeringHash(t *testing.T) {
	window := 5 * time.Minute
	var buckets [5]int

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("request-%d", i)
		offset := snapshotStartOffset(key, "snapshot", "zone", "disk", window)
		require.Equal(
			t,
			offset,
			snapshotStartOffset(key, "snapshot", "zone", "disk", window),
		)
		require.GreaterOrEqual(t, offset, time.Duration(0))
		require.Less(t, offset, window)

		buckets[int(offset/time.Minute)]++
	}

	for _, count := range buckets {
		require.InDelta(t, 2000, count, 300)
	}

	require.Zero(t, snapshotStartOffset("a", "b", "c", "d", 0))
	require.NotEqual(
		t,
		snapshotStartOffset("ab", "c", "d", "e", window),
		snapshotStartOffset("a", "bc", "d", "e", window),
	)
}

func TestSnapshotStaggeringServiceIdempotency(t *testing.T) {
	for _, window := range []string{"0s", "5m"} {
		for _, race := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/race=%v", window, race), func(t *testing.T) {
				store := &staggeringStorageStub{}
				calls := 0

				scheduler := &staggeringSchedulerStub{}
				scheduler.create = func(
					timing tasks.TaskScheduleTiming,
					req proto.Message,
				) (string, error) {

					calls++

					data, err := proto.Marshal(req)
					require.NoError(t, err)

					store.state = &storage.TaskState{
						ID:          "snapshot-task",
						TaskType:    "snapshots.CreateSnapshotFromDisk",
						Request:     data,
						ReceivedAt:  timing.ReceivedAt,
						AvailableAt: timing.NotBefore,
					}

					if race {
						// The concurrent winner persisted a different internal decision.
						saved := proto.Clone(req).(*protos.CreateSnapshotFromDiskRequest)
						saved.UseS3 = !saved.UseS3

						store.state.Request, err = proto.Marshal(saved)
						require.NoError(t, err)

						return "", fmt.Errorf("different request")
					}

					return store.state.ID, nil
				}

				cfg := &config.SnapshotsConfig{
					CreateSnapshotStaggeringWindow: proto.String(window),
					UseS3Percentage:                proto.Uint32(0),
				}
				svc, err := NewService(scheduler, store, cfg)
				require.NoError(t, err)

				// Persist the first request and check its initial schedule.
				ctx := headers.SetIncomingIdempotencyKey(context.Background(), "key")
				req := &disk_manager.CreateSnapshotRequest{
					Src: &disk_manager.DiskId{
						ZoneId: "zone",
						DiskId: "disk",
					},
					SnapshotId: "snapshot",
					FolderId:   "folder",
				}

				id, err := svc.CreateSnapshot(ctx, req)
				require.NoError(t, err)

				deadline := store.state.AvailableAt
				if window == "0s" {
					require.True(t, deadline.IsZero())
				} else {
					require.Equal(
						t,
						snapshotStartOffset("key", "snapshot", "zone", "disk", 5*time.Minute),
						deadline.Sub(store.state.ReceivedAt),
					)
				}

				// A rollout changed internal decisions and disabled staggering.
				cfg.UseS3Percentage = proto.Uint32(100)
				cfg.CreateSnapshotStaggeringWindow = proto.String("0s")

				svc, err = NewService(scheduler, store, cfg)
				require.NoError(t, err)

				repeatedID, err := svc.CreateSnapshot(ctx, req)
				require.NoError(t, err)
				require.Equal(t, id, repeatedID)
				require.True(t, deadline.Equal(store.state.AvailableAt))
				require.Equal(t, 1, calls)

				// Reusing the key for another snapshot must still be rejected.
				changed := proto.Clone(req).(*disk_manager.CreateSnapshotRequest)
				changed.SnapshotId = "another-snapshot"

				_, err = svc.CreateSnapshot(ctx, changed)
				require.Error(t, err)
			})
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

type unstartedSnapshotContext struct {
	tasks.ExecutionContext
}

func (*unstartedSnapshotContext) IsUnstartedDelayedTask() bool {
	return true
}

func TestSnapshotStaggeringCancelWithoutNBS(t *testing.T) {
	// All clients are nil: touching any of them would fail the test.
	task := &createSnapshotFromDiskTask{}
	require.NoError(
		t,
		task.Cancel(context.Background(), &unstartedSnapshotContext{}),
	)
}
