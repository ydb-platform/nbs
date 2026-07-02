package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	snapshot_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage"
	storage_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type collectFilesystemSnapshotsTask struct {
	scheduler                       tasks.Scheduler
	storage                         snapshot_storage.Storage
	snapshotCollectionTimeout       time.Duration
	snapshotCollectionInflightLimit int
	state                           *snapshot_protos.CollectFilesystemSnapshotsTaskState
}

func (t *collectFilesystemSnapshotsTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *collectFilesystemSnapshotsTask) Load(_, state []byte) error {
	t.state = &snapshot_protos.CollectFilesystemSnapshotsTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *collectFilesystemSnapshotsTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()

	snapshotIDs := make(map[string]bool)
	for _, snapshot := range t.state.Snapshots {
		snapshotIDs[snapshot.SnapshotId] = true
	}

	var taskIDs []string
	taskIDToSnapshot := make(map[string]*storage_protos.DeletingFilesystemSnapshotKey)

	for {
		deletingBefore := time.Now().Add(-t.snapshotCollectionTimeout)

		var snapshotsToScheduleDeletionFor []*storage_protos.DeletingFilesystemSnapshotKey
		if len(taskIDs) == 0 {
			snapshotsToScheduleDeletionFor = t.state.Snapshots
		}

		inflightLimit := t.snapshotCollectionInflightLimit

		if len(t.state.Snapshots) < inflightLimit {
			snapshots, err := t.storage.GetFilesystemSnapshotsToDelete(
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
				"dataplane.DeleteFilesystemSnapshotData",
				"",
				&snapshot_protos.DeleteFilesystemSnapshotDataRequest{
					SnapshotId: snapshot.SnapshotId,
				},
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
		var deletedSnapshots []*storage_protos.DeletingFilesystemSnapshotKey

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

		err = t.storage.ClearDeletingFilesystemSnapshots(ctx, deletedSnapshots)
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

func (t *collectFilesystemSnapshotsTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *collectFilesystemSnapshotsTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *collectFilesystemSnapshotsTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
