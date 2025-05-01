package dataplane

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type migrateSnapshotDatabaseTask struct {
	srcStorage                         storage.Storage
	dstStorage                         storage.Storage
	config                             *config.DataplaneConfig
	scheduler                          tasks.Scheduler
	useS3                              bool
	inflightTransferringSnapshotsCount int
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
		srcSnapshots, err := m.srcStorage.ListAllSnapshots(ctx)
		if err != nil {
			return err
		}

		dstSnapshots, err := m.dstStorage.ListAllSnapshots(ctx)
		if err != nil {
			return err
		}

		var snapshotIDs = make([]string, 0)

		for snapshotId, _ := range srcSnapshots {
			if _, ok := dstSnapshots[snapshotId]; ok {
				continue
			}

			snapshotIDs = append(snapshotIDs, snapshotId)
		}

		var taskIDs []string

		for _, snapshotId := range snapshotIDs {
			taskID, err := m.scheduler.ScheduleTask(
				headers.SetIncomingIdempotencyKey(
					ctx,
					"dataplane.MigrateSnapshotTask_"+snapshotId+"_"+execCtx.GetTaskId(),
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

			taskIDs = append(taskIDs, taskID)
		}

		for len(taskIDs) > 0 {
			// On each iteration we copy a batch of inflight tasks
			// and then delete finished ones from the list.
			inflightTaskIDs := make([]string, m.inflightTransferringSnapshotsCount)
			for i := 0; i < m.inflightTransferringSnapshotsCount && i < len(taskIDs); i++ {
				inflightTaskIDs[i] = taskIDs[i]
			}

			finishedTaskIDs, err := m.scheduler.WaitAnyTasks(ctx, inflightTaskIDs)
			if err != nil {
				return err
			}

			for i := 0; i < m.inflightTransferringSnapshotsCount && i < len(taskIDs); i++ {
				for _, finishedTaskID := range finishedTaskIDs {
					if finishedTaskID != taskIDs[i] {
						continue
					}

					// Remove finished task from the list
					taskIDs[i] = taskIDs[len(taskIDs)-1]
					taskIDs = taskIDs[:len(taskIDs)-1]
				}
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
