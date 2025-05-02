package dataplane

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type migrateSnapshotDatabaseTask struct {
	srcStorage storage.Storage
	dstStorage storage.Storage
	config     *config.DataplaneConfig
	scheduler  tasks.Scheduler
}

func (m migrateSnapshotDatabaseTask) Save() ([]byte, error) {
	return []byte{}, nil
}

func (m migrateSnapshotDatabaseTask) Load(request []byte, state []byte) error {
	return nil
}

func (m migrateSnapshotDatabaseTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	for {
		// Infinite loop to synchronize state between src and dst storages.
		// The task implies manual forceful finish, after all snapshots are migrated,
		// because before the task can be finished, we need to ensure no new snapshots
		// are created by disabling snapshot creation tasks.
		// Disabling snapshot creation is error-prone, thus we should perform
		// it manually by disabling respective tasks in config.
		inflightSnapshotsCount := m.config.GetMigrationInflightTransferringSnapshotsCount()
		srcSnapshots, err := m.srcStorage.ListAllSnapshots(ctx)
		if err != nil {
			return err
		}

		dstSnapshots, err := m.dstStorage.ListAllSnapshots(ctx)
		if err != nil {
			return err
		}

		var snapshotIDs = common.NewChannelWithCancellation[string](srcSnapshots.Size())

		for snapshotId, _ := range srcSnapshots.Vals() {
			if dstSnapshots.Has(snapshotId) {
				continue
			}

			_, err := snapshotIDs.Send(ctx, snapshotId)
			if err != nil {
				return err
			}
		}

		var inflightTaskIDs []string

		for !snapshotIDs.Empty() {
			snapshotId, ok, err := snapshotIDs.Receive(ctx)
			if !ok {
				break
			}
			if err != nil {
				return err
			}

			taskID, err := m.scheduler.ScheduleTask(
				headers.SetIncomingIdempotencyKey(
					ctx,
					"dataplane.MigrateSnapshotTask_"+snapshotId+"_"+execCtx.GetTaskID(),
				),
				"dataplane.MigrateSnapshotTask",
				"",
				&dataplane_protos.MigrateSnapshotRequest{
					SrcSnapshotId: snapshotId,
				},
			)
			if err != nil {
				return err
			}

			err = execCtx.AddTaskDependency(ctx, taskID)
			if err != nil {
				return err
			}

			inflightTaskIDs = append(inflightTaskIDs, taskID)
			inflightTasksLimitReached := len(inflightTaskIDs) == int(inflightSnapshotsCount)
			inflightTasksLimitReached = inflightTasksLimitReached || snapshotIDs.Empty()
			if inflightTasksLimitReached {
				finishedTaskIDs, err := m.scheduler.WaitAnyTasks(ctx, inflightTaskIDs)
				if err != nil {
					return err
				}

				newInflightTaskIds := make([]string, len(inflightTaskIDs))
				for _, inflightTaskID := range inflightTaskIDs {
					if common.Find(finishedTaskIDs, inflightTaskID) {

					} else {
						newInflightTaskIds = append(newInflightTaskIds, inflightTaskID)
					}
				}

				inflightTaskIDs = newInflightTaskIds
			}

		}
	}

	//goland:noinspection GoUnreachableCode
	return nil
}

func (m migrateSnapshotDatabaseTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return nil
}

func (m migrateSnapshotDatabaseTask) GetMetadata(ctx context.Context) (proto.Message, error) {
	return nil, nil
}

func (m migrateSnapshotDatabaseTask) GetResponse() proto.Message {
	return nil
}
