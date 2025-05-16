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
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type snapshotToTasksMapping struct {
	snapshotToTask map[string]string
	taskToSnapshot map[string]string
}

func newSnapshotToTasksMapping() *snapshotToTasksMapping {
	return &snapshotToTasksMapping{
		snapshotToTask: make(map[string]string),
		taskToSnapshot: make(map[string]string),
	}
}

func (m *snapshotToTasksMapping) add(
	snapshotID string,
	taskID string,
) {
	m.snapshotToTask[snapshotID] = taskID
	m.taskToSnapshot[taskID] = snapshotID
}

func (m *snapshotToTasksMapping) remove(taskIDs []string) {
	for _, taskID := range taskIDs {
		snapshotID, ok := m.taskToSnapshot[taskID]
		if !ok {
			continue
		}

		delete(m.snapshotToTask, snapshotID)
		delete(m.taskToSnapshot, taskID)
	}
}

func (m *snapshotToTasksMapping) hasSnapshots(snapshotID string) bool {
	_, ok := m.snapshotToTask[snapshotID]
	return ok
}

func (m *snapshotToTasksMapping) taskIDs() []string {
	taskIDs := make([]string, 0, len(m.taskToSnapshot))
	for taskID := range m.taskToSnapshot {
		taskIDs = append(taskIDs, taskID)
	}

	return taskIDs
}

func (m *snapshotToTasksMapping) snapshotIDs() []string {
	snapshotIDs := make([]string, 0, len(m.snapshotToTask))
	for snapshotID := range m.snapshotToTask {
		snapshotIDs = append(snapshotIDs, snapshotID)
	}

	return snapshotIDs
}

////////////////////////////////////////////////////////////////////////////////

type migrateSnapshotDatabaseTask struct {
	registry   metrics.Registry
	srcStorage storage.Storage
	dstStorage storage.Storage
	config     *config.DataplaneConfig
	scheduler  tasks.Scheduler
	state      *dataplane_protos.MigrateSnapshotDatabaseTaskState
}

func (m *migrateSnapshotDatabaseTask) Save() ([]byte, error) {
	return proto.Marshal(m.state)
}

func (m *migrateSnapshotDatabaseTask) Load(request []byte, state []byte) error {
	m.state = &dataplane_protos.MigrateSnapshotDatabaseTaskState{}
	return proto.Unmarshal(state, m.state)
}

func (m *migrateSnapshotDatabaseTask) Run(
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
		err = m.migrateSnapshots(ctx, execCtx, snapshotsToMigrate)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *migrateSnapshotDatabaseTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (m *migrateSnapshotDatabaseTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (m *migrateSnapshotDatabaseTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (m *migrateSnapshotDatabaseTask) migrateSnapshots(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	snapshotsToMigrate tasks_storage.StringSet,
) error {

	mapping := newSnapshotToTasksMapping()

	for {
		err := m.updateInflightSnapshots(ctx, execCtx, snapshotsToMigrate)
		if err != nil {
			return err
		}

		if len(m.state.InflightSnapshots) == 0 {
			return nil
		}

		err = m.scheduleInflightSnapshotsAndSaveThemIntoMapping(ctx, execCtx, mapping)
		if err != nil {
			return err
		}

		finishedTaskIDs, err := m.scheduler.WaitAnyTasks(
			ctx,
			mapping.taskIDs(),
		)
		if err != nil {
			return err
		}

		mapping.remove(finishedTaskIDs)
		m.state.InflightSnapshots = mapping.snapshotIDs()
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}
}

func (m *migrateSnapshotDatabaseTask) updateInflightSnapshots(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	snapshotsToMigrate tasks_storage.StringSet,
) error {

	cfg := m.config
	snapshotsInflightLimit := int(cfg.GetMigratingSnapshotsInflightLimit())

	// Persistently save all inflight snapshots to the state
	for snapshotID := range snapshotsToMigrate.Vals() {
		if common.Find(m.state.InflightSnapshots, snapshotID) {
			snapshotsToMigrate.Remove(snapshotID)
			continue
		}

		if len(m.state.InflightSnapshots) >= snapshotsInflightLimit {
			break
		}

		m.state.InflightSnapshots = append(
			m.state.InflightSnapshots,
			snapshotID,
		)
		snapshotsToMigrate.Remove(snapshotID)
	}

	return execCtx.SaveState(ctx)
}

func (m *migrateSnapshotDatabaseTask) scheduleInflightSnapshotsAndSaveThemIntoMapping(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	mapping *snapshotToTasksMapping,
) error {

	for _, snapshotID := range m.state.InflightSnapshots {
		if mapping.hasSnapshots(snapshotID) {
			continue
		}

		// We expect that if snapshot is inflight
		// and the task was restarted and the inflight snapshot creation
		// was not finished before each scheduling iteration,
		// the task would still be present in the database.
		// (finished tasks are removed from the database).
		//
		// It is the same as the following statement:
		// We expect the tasks not to hang between its start
		// and scheduling of snapshots for longer than tasks collection interval.
		//
		// This assumption seems to be reasonable, because
		// endedTasksExpirationTimeout is set to 96h by default, which is by
		// several orders of magnitude greater than the time between task start
		// and scheduling of snapshot migration task.
		// This expectation allows us to avoid storing mappings in the database.
		taskID, err := m.scheduleMigrateSnapshotTask(
			ctx,
			execCtx,
			snapshotID,
		)
		if err != nil {
			return err
		}

		mapping.add(snapshotID, taskID)
	}

	return nil
}

func (m *migrateSnapshotDatabaseTask) scheduleMigrateSnapshotTask(
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
