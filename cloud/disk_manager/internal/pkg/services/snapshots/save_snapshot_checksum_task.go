package snapshots

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

// Computes the checksum of the snapshot and saves it to the database, see
// validateSnapshotChecksumTask for the task that uses the saved checksum.
// The checksum that is already saved is overwritten.
type saveSnapshotChecksumTask struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	request   *protos.SaveSnapshotChecksumRequest
	state     *protos.SaveSnapshotChecksumTaskState
}

func (t *saveSnapshotChecksumTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *saveSnapshotChecksumTask) Load(request, state []byte) error {
	t.request = &protos.SaveSnapshotChecksumRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.SaveSnapshotChecksumTaskState{}
	return proto.Unmarshal(state, t.state)
}

// Run is started from the beginning when the dataplane task is finished, so
// each step should be idempotent.
func (t *saveSnapshotChecksumTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.SnapshotId

	meta, err := t.storage.GetSnapshotMeta(ctx, snapshotID)
	if err != nil {
		return err
	}

	if meta == nil || !meta.Ready {
		return errors.NewNonRetriableErrorf(
			"snapshot %v is absent or not ready",
			snapshotID,
		)
	}

	// The idempotency key depends only on the id of this task, so the
	// dataplane task is scheduled once, no matter how many times Run is
	// started.
	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			ctx,
			execCtx.GetTaskID()+"_compute_checksum",
		),
		"dataplane.ComputeSnapshotChecksum",
		"",
		&dataplane_protos.ComputeSnapshotChecksumRequest{
			SnapshotId: snapshotID,
		},
	)
	if err != nil {
		return err
	}

	t.state.ComputeChecksumTaskId = taskID

	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	response, err := t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	typedResponse, ok :=
		response.(*dataplane_protos.ComputeSnapshotChecksumResponse)
	if !ok {
		return errors.NewNonRetriableErrorf(
			"invalid dataplane.ComputeSnapshotChecksum response type %T",
			response,
		)
	}

	t.state.Checksum = typedResponse.Checksum

	// Fails if the snapshot is not ready anymore: the checksum of a snapshot
	// that is being deleted is of no use.
	return t.storage.SnapshotChecksumCalculated(
		ctx,
		snapshotID,
		typedResponse.Checksum,
	)
}

func (t *saveSnapshotChecksumTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.state.ComputeChecksumTaskId == "" {
		return nil
	}

	_, err := t.scheduler.CancelTask(ctx, t.state.ComputeChecksumTaskId)
	if err != nil {
		return err
	}

	return t.scheduler.WaitTaskEnded(ctx, t.state.ComputeChecksumTaskId)
}

func (t *saveSnapshotChecksumTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *saveSnapshotChecksumTask) GetResponse() proto.Message {
	return &protos.SaveSnapshotChecksumResponse{
		Checksum: t.state.Checksum,
	}
}
