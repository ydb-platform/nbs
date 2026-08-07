package dataplane

import (
	"context"
	"fmt"
	"sync/atomic"

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

type relocateAllSnapshotsDataFromYDBToS3Task struct {
	config    *config.DataplaneConfig
	registry  metrics.Registry
	storage   storage.Storage
	scheduler tasks.Scheduler
	request   *dataplane_protos.RelocateAllSnapshotsDataFromYDBToS3Request
	state     *dataplane_protos.RelocateAllSnapshotsDataFromYDBToS3TaskState
}

func (t *relocateAllSnapshotsDataFromYDBToS3Task) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *relocateAllSnapshotsDataFromYDBToS3Task) Load(request, state []byte) error {
	t.request = &dataplane_protos.RelocateAllSnapshotsDataFromYDBToS3Request{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &dataplane_protos.RelocateAllSnapshotsDataFromYDBToS3TaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *relocateAllSnapshotsDataFromYDBToS3Task) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	subregistry := t.registry.WithTags(map[string]string{
		"id": execCtx.GetTaskID(),
	})
	for {
		// Infinite loop: stop via tasks cancel after load is drained.
		err := t.relocatePass(ctx, execCtx, subregistry)
		if err != nil {
			return err
		}
	}
}

func (t *relocateAllSnapshotsDataFromYDBToS3Task) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *relocateAllSnapshotsDataFromYDBToS3Task) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *relocateAllSnapshotsDataFromYDBToS3Task) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *relocateAllSnapshotsDataFromYDBToS3Task) relocatePass(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	subregistry metrics.Registry,
) error {

	queueSize := int(t.config.GetRelocateSnapshotsScanQueueSize())
	if queueSize == 0 {
		queueSize = 500
	}

	scanCtx, cancelScan := context.WithCancel(ctx)
	defer cancelScan()

	ids, scanErrors := t.storage.StreamReadySnapshotIDs(scanCtx)
	candidates := make(chan string, queueSize)

	var queueDepth int64
	var scannedCount int64
	var skippedCount int64
	var scheduledCount int64

	scanDone := make(chan error, 1)
	go func() {
		defer close(candidates)

		for id := range ids {
			atomic.AddInt64(&scannedCount, 1)
			subregistry.Gauge("snapshots/relocateToS3Scanned").Set(
				float64(atomic.LoadInt64(&scannedCount)),
			)

			select {
			case candidates <- id:
				depth := atomic.AddInt64(&queueDepth, 1)
				subregistry.Gauge("snapshots/relocateToS3Queue").Set(
					float64(depth),
				)
			case <-scanCtx.Done():
				scanDone <- scanCtx.Err()
				return
			}
		}

		var terminal error
		if err, ok := <-scanErrors; ok {
			terminal = err
		}
		scanDone <- terminal
	}()

	mapping := newSnapshotToTasksMapping()
	scanExhausted := false

	for {
		err := t.updateInflightFromScan(
			ctx,
			execCtx,
			candidates,
			&queueDepth,
			&skippedCount,
			&scheduledCount,
			subregistry,
			&scanExhausted,
		)
		if err != nil {
			cancelScan()
			<-scanDone
			return err
		}

		if len(t.state.InflightSnapshots) == 0 {
			if scanExhausted {
				return <-scanDone
			}
			continue
		}

		err = t.scheduleInflightSnapshotsAndSaveThemIntoMapping(
			ctx,
			execCtx,
			mapping,
		)
		if err != nil {
			cancelScan()
			<-scanDone
			return err
		}

		finishedTaskIDs, err := t.scheduler.WaitAnyTasks(
			ctx,
			mapping.taskIDs(),
		)
		if err != nil {
			cancelScan()
			<-scanDone
			return err
		}

		mapping.remove(finishedTaskIDs)
		t.state.InflightSnapshots = mapping.snapshotIDs()
		err = execCtx.SaveState(ctx)
		if err != nil {
			cancelScan()
			<-scanDone
			return err
		}
	}
}

func (t *relocateAllSnapshotsDataFromYDBToS3Task) updateInflightFromScan(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	candidates <-chan string,
	queueDepth *int64,
	skippedCount *int64,
	scheduledCount *int64,
	subregistry metrics.Registry,
	scanExhausted *bool,
) error {

	inflightLimit := int(t.config.GetRelocatingSnapshotsToS3InflightLimit())
	if inflightLimit == 0 {
		inflightLimit = 1
	}

	for len(t.state.InflightSnapshots) < inflightLimit {
		var snapshotID string
		var ok bool

		if len(t.state.InflightSnapshots) == 0 {
			snapshotID, ok = <-candidates
			if !ok {
				*scanExhausted = true
				return nil
			}
		} else {
			select {
			case snapshotID, ok = <-candidates:
				if !ok {
					*scanExhausted = true
					return nil
				}
			default:
				return execCtx.SaveState(ctx)
			}
		}

		depth := atomic.AddInt64(queueDepth, -1)
		if depth < 0 {
			depth = 0
			atomic.StoreInt64(queueDepth, 0)
		}
		subregistry.Gauge("snapshots/relocateToS3Queue").Set(float64(depth))

		if common.Find(t.state.InflightSnapshots, snapshotID) {
			continue
		}

		needs, err := t.storage.SnapshotNeedsRelocateToS3(
			ctx,
			snapshotID,
			t.request.KeepYdbData,
		)
		if err != nil {
			return err
		}
		if !needs {
			atomic.AddInt64(skippedCount, 1)
			subregistry.Gauge("snapshots/relocateToS3Skipped").Set(
				float64(atomic.LoadInt64(skippedCount)),
			)
			continue
		}

		t.state.InflightSnapshots = append(
			t.state.InflightSnapshots,
			snapshotID,
		)
		atomic.AddInt64(scheduledCount, 1)
		subregistry.Gauge("snapshots/relocateToS3Scheduled").Set(
			float64(atomic.LoadInt64(scheduledCount)),
		)
	}

	return execCtx.SaveState(ctx)
}

func (t *relocateAllSnapshotsDataFromYDBToS3Task) scheduleInflightSnapshotsAndSaveThemIntoMapping(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	mapping *snapshotToTasksMapping,
) error {

	for _, snapshotID := range t.state.InflightSnapshots {
		if mapping.hasSnapshots(snapshotID) {
			continue
		}

		taskID, err := t.scheduleRelocateSnapshotDataFromYDBToS3Task(
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

func (t *relocateAllSnapshotsDataFromYDBToS3Task) scheduleRelocateSnapshotDataFromYDBToS3Task(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	snapshotID string,
) (string, error) {

	idempotencyKey := headers.SetIncomingIdempotencyKey(
		ctx,
		fmt.Sprintf(
			"%s_relocate_snapshot_data_from_ydb_to_s3_%s",
			snapshotID,
			execCtx.GetTaskID(),
		),
	)
	return t.scheduler.ScheduleTask(
		idempotencyKey,
		"dataplane.RelocateSnapshotDataFromYDBToS3Task",
		"",
		&dataplane_protos.RelocateSnapshotDataFromYDBToS3Request{
			SnapshotId:  snapshotID,
			KeepYdbData: t.request.KeepYdbData,
		},
	)
}
