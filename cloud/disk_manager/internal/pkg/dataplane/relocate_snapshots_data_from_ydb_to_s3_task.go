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
	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type relocateSnapshotsDataFromYDBToS3Task struct {
	config    *config.DataplaneConfig
	registry  metrics.Registry
	storage   storage.Storage
	scheduler tasks.Scheduler
	request   *dataplane_protos.RelocateSnapshotsDataFromYDBToS3Request
	state     *dataplane_protos.RelocateSnapshotsDataFromYDBToS3TaskState
}

func (t *relocateSnapshotsDataFromYDBToS3Task) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *relocateSnapshotsDataFromYDBToS3Task) Load(request, state []byte) error {
	t.request = &dataplane_protos.RelocateSnapshotsDataFromYDBToS3Request{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &dataplane_protos.RelocateSnapshotsDataFromYDBToS3TaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *relocateSnapshotsDataFromYDBToS3Task) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	subregistry := t.registry.WithTags(map[string]string{
		"id": execCtx.GetTaskID(),
	})
	for {
		// Infinite loop: stop via tasks cancel after load is drained.
		snapshots, err := t.storage.ListSnapshots(ctx)
		if err != nil {
			return err
		}

		subregistry.Gauge("snapshots/relocateToS3Candidates").Set(
			float64(snapshots.Size()),
		)
		err = t.relocateSnapshots(ctx, execCtx, snapshots)
		if err != nil {
			return err
		}
	}
}

func (t *relocateSnapshotsDataFromYDBToS3Task) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *relocateSnapshotsDataFromYDBToS3Task) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *relocateSnapshotsDataFromYDBToS3Task) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *relocateSnapshotsDataFromYDBToS3Task) relocateSnapshots(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	snapshots tasks_common.StringSet,
) error {

	mapping := newSnapshotToTasksMapping()

	for {
		err := t.updateInflightSnapshots(ctx, execCtx, snapshots)
		if err != nil {
			return err
		}

		if len(t.state.InflightSnapshots) == 0 {
			return nil
		}

		err = t.scheduleInflightSnapshotsAndSaveThemIntoMapping(
			ctx,
			execCtx,
			mapping,
		)
		if err != nil {
			return err
		}

		finishedTaskIDs, err := t.scheduler.WaitAnyTasks(
			ctx,
			mapping.taskIDs(),
		)
		if err != nil {
			return err
		}

		mapping.remove(finishedTaskIDs)
		t.state.InflightSnapshots = mapping.snapshotIDs()
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}
}

func (t *relocateSnapshotsDataFromYDBToS3Task) updateInflightSnapshots(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	snapshots tasks_common.StringSet,
) error {

	inflightLimit := int(t.request.GetInflightLimit())
	if inflightLimit == 0 {
		inflightLimit = int(t.config.GetRelocateSnapshotsToS3InflightLimit())
	}
	if inflightLimit == 0 {
		inflightLimit = 1
	}

	for snapshotID := range snapshots.Vals() {
		if common.Find(t.state.InflightSnapshots, snapshotID) {
			snapshots.Remove(snapshotID)
			continue
		}

		if len(t.state.InflightSnapshots) >= inflightLimit {
			break
		}

		meta, err := t.storage.GetSnapshotMeta(ctx, snapshotID)
		if err != nil {
			return err
		}
		if meta == nil || !meta.Ready {
			snapshots.Remove(snapshotID)
			continue
		}

		t.state.InflightSnapshots = append(
			t.state.InflightSnapshots,
			snapshotID,
		)
		snapshots.Remove(snapshotID)
	}

	return execCtx.SaveState(ctx)
}

func (t *relocateSnapshotsDataFromYDBToS3Task) scheduleInflightSnapshotsAndSaveThemIntoMapping(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	mapping *snapshotToTasksMapping,
) error {

	for _, snapshotID := range t.state.InflightSnapshots {
		if mapping.hasSnapshots(snapshotID) {
			continue
		}

		taskID, err := t.scheduleRelocateSnapshotChunksToS3Task(
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

func (t *relocateSnapshotsDataFromYDBToS3Task) scheduleRelocateSnapshotChunksToS3Task(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	snapshotID string,
) (string, error) {

	idempotencyKey := headers.SetIncomingIdempotencyKey(
		ctx,
		fmt.Sprintf(
			"%s_relocate_snapshot_chunks_to_s3_%s",
			snapshotID,
			execCtx.GetTaskID(),
		),
	)
	return t.scheduler.ScheduleTask(
		idempotencyKey,
		"dataplane.RelocateSnapshotChunksToS3Task",
		"",
		&dataplane_protos.RelocateSnapshotChunksToS3Request{
			SnapshotId:  snapshotID,
			WorkerCount: t.request.GetWorkerCount(),
		},
	)
}
