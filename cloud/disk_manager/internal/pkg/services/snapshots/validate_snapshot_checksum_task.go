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

// Computes the checksum of the snapshot once again and compares it with the
// one saved by saveSnapshotChecksumTask. Fails if they differ: the data of
// the snapshot has changed since its checksum was saved. Changes nothing.
type validateSnapshotChecksumTask struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	request   *protos.ValidateSnapshotChecksumRequest
	state     *protos.ValidateSnapshotChecksumTaskState
}

func (t *validateSnapshotChecksumTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *validateSnapshotChecksumTask) Load(request, state []byte) error {
	t.request = &protos.ValidateSnapshotChecksumRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.ValidateSnapshotChecksumTaskState{}
	return proto.Unmarshal(state, t.state)
}

// Run is started from the beginning when the dataplane task is finished, so
// each step should be idempotent.
func (t *validateSnapshotChecksumTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.SnapshotId

	// Fail fast: computing the checksum is expensive and there is no point in
	// it if there is nothing to compare the result with.
	_, err := t.getSavedChecksum(ctx)
	if err != nil {
		return err
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

	// The saved checksum is read once again: the snapshot might have been
	// deleted or its checksum might have been saved anew in the meantime.
	savedChecksum, err := t.getSavedChecksum(ctx)
	if err != nil {
		return err
	}

	if savedChecksum != typedResponse.Checksum {
		return errors.NewNonRetriableErrorf(
			"checksum mismatch for snapshot %v: saved %v, actual %v",
			snapshotID,
			savedChecksum,
			typedResponse.Checksum,
		)
	}

	return nil
}

func (t *validateSnapshotChecksumTask) Cancel(
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

func (t *validateSnapshotChecksumTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *validateSnapshotChecksumTask) GetResponse() proto.Message {
	return &protos.ValidateSnapshotChecksumResponse{
		Checksum: t.state.Checksum,
	}
}

////////////////////////////////////////////////////////////////////////////////

// Returns the checksum saved for the snapshot. Fails if there is nothing to
// validate: the snapshot is not ready or its checksum is not saved.
func (t *validateSnapshotChecksumTask) getSavedChecksum(
	ctx context.Context,
) (uint32, error) {

	snapshotID := t.request.SnapshotId

	meta, err := t.storage.GetSnapshotMeta(ctx, snapshotID)
	if err != nil {
		return 0, err
	}

	if meta == nil || !meta.Ready {
		return 0, errors.NewNonRetriableErrorf(
			"snapshot %v is absent or not ready",
			snapshotID,
		)
	}

	if !meta.ChecksumCalculated {
		return 0, errors.NewNonRetriableErrorf(
			"checksum of snapshot %v is not saved",
			snapshotID,
		)
	}

	return meta.Checksum, nil
}
