package dataplane

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type migrateSnapshotDatabaseTask struct {
	registry   metrics.Registry
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

func (m migrateSnapshotDatabaseTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	subregistry := m.registry.WithTags(map[string]string{
		"id": execCtx.GetTaskID(),
	})
	for {
		// Infinite loop to synchronize state between src and dst storages.
		// The task implies manual forceful finish, after all snapshots are migrated,
		// because before the task can be finished, we need to ensure no new snapshots
		// are created by disabling snapshot creation tasks.
		// Disabling snapshot creation is error-prone, thus we should perform
		// it manually by disabling respective tasks in config.
		inflightSnapshotsCount := m.config.GetMigrationInflightTransferringSnapshotsCount()
		taskTimeout, err := time.ParseDuration(m.config.GetMigrationSnapshotCollectionTimeout())
		if err != nil {
			return err
		}

		srcSnapshots, err := m.srcStorage.ListAllSnapshots(ctx)
		if err != nil {
			return err
		}

		dstSnapshots, err := m.dstStorage.ListAllSnapshots(ctx)
		if err != nil {
			return err
		}

		snapshotsToProcess := srcSnapshots.Subtract(dstSnapshots)
		snapshotsToProcessCount := snapshotsToProcess.Size()
		subregistry.Gauge(
			"snapshots/migratingCount",
		).Set(float64(snapshotsToProcessCount))
		var inflightTaskIDs []string

		for snapshotId := range snapshotsToProcess.Vals() {
			idempotencyKey := headers.SetIncomingIdempotencyKey(
				ctx,
				fmt.Sprintf(
					"dataplane.MigrateSnapshotTask_%s_%s",
					snapshotId,
					execCtx.GetTaskID(),
				),
			)
			taskID, err := m.scheduler.ScheduleTask(
				idempotencyKey,
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
			if inflightTasksLimitReached || snapshotsToProcessCount == 1 {
				finishedTaskIDs, err := m.scheduler.WaitAnyTasksWithTimeout(
					ctx,
					inflightTaskIDs,
					taskTimeout,
				)
				if err != nil {
					return err
				}

				newInflightTaskIds := make([]string, len(inflightTaskIDs))
				for _, inflightTaskID := range inflightTaskIDs {
					if !common.Find(finishedTaskIDs, inflightTaskID) {
						newInflightTaskIds = append(
							newInflightTaskIds,
							inflightTaskID,
						)
					}
				}

				inflightTaskIDs = newInflightTaskIds
			}
			snapshotsToProcessCount--
		}
	}

	//goland:noinspection GoUnreachableCode
	return nil
}

func (m migrateSnapshotDatabaseTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return nil
}

func (m migrateSnapshotDatabaseTask) GetMetadata(ctx context.Context) (proto.Message, error) {
	return &empty.Empty{}, nil
}

func (m migrateSnapshotDatabaseTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
