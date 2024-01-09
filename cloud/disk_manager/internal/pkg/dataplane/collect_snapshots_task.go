package dataplane

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type collectSnapshotsTask struct {
	scheduler                       tasks.Scheduler
	storage                         storage.Storage
	snapshotCollectionTimeout       time.Duration
	snapshotCollectionInflightLimit int
	state                           *protos.CollectSnapshotsTaskState
}

func (t *collectSnapshotsTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *collectSnapshotsTask) Load(_, state []byte) error {
	t.state = &protos.CollectSnapshotsTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *collectSnapshotsTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()

	snapshotIDs := make(map[string]bool)
	for _, snapshot := range t.state.Snapshots {
		snapshotIDs[snapshot.SnapshotId] = true
	}

	var taskIDs []string
	taskIDToSnapshot := make(map[string]*storage_protos.DeletingSnapshotKey)

	for {
		deletingBefore := time.Now().Add(-t.snapshotCollectionTimeout)

		var snapshotsToScheduleDeletionFor []*storage_protos.DeletingSnapshotKey
		if len(taskIDs) == 0 {
			snapshotsToScheduleDeletionFor = t.state.Snapshots
		}

		inflightLimit := t.snapshotCollectionInflightLimit

		if len(t.state.Snapshots) < inflightLimit {
			snapshots, err := t.storage.GetSnapshotsToDelete(
				ctx,
				deletingBefore,
				inflightLimit,
			)
			if err != nil {
				return err
			}

			for _, snapshot := range snapshots {
				if !snapshotIDs[snapshot.SnapshotId] {
					snapshotIDs[snapshot.SnapshotId] = true
					t.state.Snapshots = append(t.state.Snapshots, snapshot)

					snapshotsToScheduleDeletionFor = append(
						snapshotsToScheduleDeletionFor,
						snapshot,
					)
				}
			}

			err = execCtx.SaveState(ctx)
			if err != nil {
				return err
			}
		}

		for _, snapshot := range snapshotsToScheduleDeletionFor {
			idempotencyKey := fmt.Sprintf(
				"%v_%v",
				selfTaskID,
				snapshot.SnapshotId,
			)

			taskID, err := t.scheduler.ScheduleTask(
				headers.SetIncomingIdempotencyKey(ctx, idempotencyKey),
				"dataplane.DeleteSnapshotData",
				"",
				&protos.DeleteSnapshotDataRequest{
					SnapshotId: snapshot.SnapshotId,
				},
				"",
				"",
			)
			if err != nil {
				return err
			}

			taskIDToSnapshot[taskID] = snapshot
			taskIDs = append(taskIDs, taskID)
		}

		if len(taskIDs) == 0 {
			// Nothing to collect.
			return errors.NewInterruptExecutionError()
		}

		finishedTaskIDs, err := t.scheduler.WaitAnyTasks(ctx, taskIDs)
		if err != nil {
			return err
		}

		deletedSnapshotIDs := make(map[string]bool)
		var deletedSnapshots []*storage_protos.DeletingSnapshotKey

		for _, taskID := range finishedTaskIDs {
			snapshot := taskIDToSnapshot[taskID]
			delete(taskIDToSnapshot, taskID)

			deletedSnapshotIDs[snapshot.SnapshotId] = true
			deletedSnapshots = append(deletedSnapshots, snapshot)
		}

		taskIDs = nil
		for taskID := range taskIDToSnapshot {
			taskIDs = append(taskIDs, taskID)
		}

		err = t.storage.ClearDeletingSnapshots(ctx, deletedSnapshots)
		if err != nil {
			return err
		}

		snapshots := t.state.Snapshots
		t.state.Snapshots = nil
		snapshotIDs = make(map[string]bool)

		for _, snapshot := range snapshots {
			if !deletedSnapshotIDs[snapshot.SnapshotId] {
				snapshotIDs[snapshot.SnapshotId] = true
				t.state.Snapshots = append(t.state.Snapshots, snapshot)
			}
		}

		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}
}

func (t *collectSnapshotsTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *collectSnapshotsTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *collectSnapshotsTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
