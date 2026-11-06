package dataplane

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type establishSnapshotInheritanceForAllSnapshotsTask struct {
	scheduler     tasks.Scheduler
	storage       storage.Storage
	inflightLimit uint64
	state         *protos.EstablishSnapshotInheritanceForAllSnapshotsTaskState
}

func (
	t *establishSnapshotInheritanceForAllSnapshotsTask,
) Save() ([]byte, error) {

	return proto.Marshal(t.state)
}

func (t *establishSnapshotInheritanceForAllSnapshotsTask) Load(
	_, state []byte,
) error {

	t.state = &protos.EstablishSnapshotInheritanceForAllSnapshotsTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *establishSnapshotInheritanceForAllSnapshotsTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.state.Completed {
		return nil
	}

	if t.inflightLimit == 0 {
		return errors.NewNonRetriableErrorf(
			"snapshot inheritance inflight limit must be positive",
		)
	}

	if t.state.StartedAt == nil {
		t.state.StartedAt = ptypes.TimestampNow()
		err := execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	startedAt, err := ptypes.Timestamp(t.state.StartedAt)
	if err != nil {
		return errors.NewNonRetriableError(err)
	}

	for {
		if len(t.state.InflightSnapshots) == 0 {
			snapshotIDs, err := t.storage.ListSnapshotsBefore(
				ctx,
				startedAt,
				t.state.LastSnapshotId,
				t.inflightLimit,
			)
			if err != nil {
				return err
			}

			if len(snapshotIDs) == 0 {
				t.state.Completed = true
				return execCtx.SaveState(ctx)
			}

			for _, snapshotID := range snapshotIDs {
				t.state.InflightSnapshots = append(
					t.state.InflightSnapshots,
					&protos.SnapshotInheritanceTask{SnapshotId: snapshotID},
				)
			}

			// Save the cursor together with the complete batch before starting
			// children, so retries cannot skip snapshots or lose running tasks.
			t.state.LastSnapshotId = snapshotIDs[len(snapshotIDs)-1]
			err = execCtx.SaveState(ctx)
			if err != nil {
				return err
			}
		}

		err = t.scheduleInflightSnapshots(ctx, execCtx)
		if err != nil {
			return err
		}

		taskIDs := make([]string, 0, len(t.state.InflightSnapshots))
		for _, snapshot := range t.state.InflightSnapshots {
			taskIDs = append(taskIDs, snapshot.TaskId)
		}

		finishedTaskIDs, err := t.scheduler.WaitAnyTasks(ctx, taskIDs)
		if err != nil {
			return err
		}

		finishedTasks := make(map[string]bool, len(finishedTaskIDs))
		for _, taskID := range finishedTaskIDs {
			finishedTasks[taskID] = true
		}

		inflightSnapshots := t.state.InflightSnapshots
		t.state.InflightSnapshots = nil
		for _, snapshot := range inflightSnapshots {
			if !finishedTasks[snapshot.TaskId] {
				t.state.InflightSnapshots = append(
					t.state.InflightSnapshots,
					snapshot,
				)
			}
		}

		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}
}

func (t *establishSnapshotInheritanceForAllSnapshotsTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// Scheduled with ScheduleNonCancellableTask; the scheduler rejects cancellation.
	return nil
}

func (t *establishSnapshotInheritanceForAllSnapshotsTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (
	t *establishSnapshotInheritanceForAllSnapshotsTask,
) GetResponse() proto.Message {

	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (
	t *establishSnapshotInheritanceForAllSnapshotsTask,
) scheduleInflightSnapshots(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	for _, snapshot := range t.state.InflightSnapshots {
		if len(snapshot.TaskId) != 0 {
			continue
		}

		taskID, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(
				ctx,
				fmt.Sprintf(
					"%s_establish_snapshot_inheritance_%s",
					execCtx.GetTaskID(),
					snapshot.SnapshotId,
				),
			),
			"dataplane.EstablishSnapshotInheritance",
			"",
			&protos.EstablishSnapshotInheritanceRequest{
				SnapshotId: snapshot.SnapshotId,
			},
		)
		if err != nil {
			return err
		}

		snapshot.TaskId = taskID
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}
