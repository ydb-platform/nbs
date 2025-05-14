package dataplane

import (
	"context"
	"fmt"

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
	state      *dataplane_protos.MigrateSnapshotDatabaseTaskState
}

func (m migrateSnapshotDatabaseTask) Save() ([]byte, error) {
	return proto.Marshal(m.state)
}

func (m migrateSnapshotDatabaseTask) Load(request []byte, state []byte) error {
	m.state = &dataplane_protos.MigrateSnapshotDatabaseTaskState{}
	return proto.Unmarshal(state, m.state)
}

func (m migrateSnapshotDatabaseTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// WARNING: we expect the destination database to be empty and not
	// attached to any other disk-manager instance as a main storage
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
		cfg := m.config
		inflightSnapshotsLimit := int(cfg.GetMigratingSnapshotsInflightLimit())

		srcSnapshots, err := m.srcStorage.ListSnapshots(ctx)
		if err != nil {
			return err
		}

		dstSnapshots, err := m.dstStorage.ListSnapshots(ctx)
		if err != nil {
			return err
		}

		snapshotsToMigrate := srcSnapshots.Subtract(dstSnapshots)
		snapshotsToProcessCount := snapshotsToMigrate.Size()
		subregistry.Gauge(
			"snapshots/migratingCount",
		).Set(float64(snapshotsToProcessCount))

		for snapshotID := range snapshotsToMigrate.Vals() {
			taskID, err := m.scheduleMigrateSnapshotTask(
				ctx,
				execCtx,
				snapshotID,
			)
			if err != nil {
				return err
			}

			m.state.InflightTaskIDs = append(m.state.InflightTaskIDs, taskID)
			err = execCtx.SaveState(ctx)
			if err != nil {
				return err
			}

			tasksCount := len(m.state.InflightTaskIDs)
			inflightTasksLimitReached := tasksCount >= inflightSnapshotsLimit
			if !inflightTasksLimitReached && snapshotsToProcessCount != 1 {
				snapshotsToProcessCount--
				continue
			}

			finishedTaskIDs, err := m.scheduler.WaitAnyTasks(
				ctx,
				m.state.InflightTaskIDs,
			)
			if err != nil {
				return err
			}

			unfinishedTaskIDs := make([]string, tasksCount)
			for _, inflightTaskID := range m.state.InflightTaskIDs {
				if !common.Find(finishedTaskIDs, inflightTaskID) {
					unfinishedTaskIDs = append(
						unfinishedTaskIDs,
						inflightTaskID,
					)
				}
			}

			m.state.InflightTaskIDs = unfinishedTaskIDs
			err = execCtx.SaveState(ctx)
			if err != nil {
				return err
			}

			snapshotsToProcessCount--
		}
	}

	return nil
}

func (m migrateSnapshotDatabaseTask) scheduleMigrateSnapshotTask(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	snapshotID string,
) (string, error) {

	idempotencyKey := headers.SetIncomingIdempotencyKey(
		ctx,
		fmt.Sprintf(
			"%s_migrate_snapshot_%s",
			snapshotID,
			execCtx.GetTaskID(),
		),
	)
	return m.scheduler.ScheduleTask(
		idempotencyKey,
		"dataplane.MigrateSnapshotTask",
		"",
		&dataplane_protos.MigrateSnapshotRequest{
			SrcSnapshotId: snapshotID,
		},
	)
}

func (m migrateSnapshotDatabaseTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (m migrateSnapshotDatabaseTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (m migrateSnapshotDatabaseTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
