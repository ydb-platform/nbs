package images

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

// Computes the checksum of the image once again and compares it with the one
// saved by saveImageChecksumTask. Fails if they differ: the data of the image
// has changed since its checksum was saved. Changes nothing.
type validateImageChecksumTask struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	request   *protos.ValidateImageChecksumRequest
	state     *protos.ValidateImageChecksumTaskState
}

func (t *validateImageChecksumTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *validateImageChecksumTask) Load(request, state []byte) error {
	t.request = &protos.ValidateImageChecksumRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.ValidateImageChecksumTaskState{}
	return proto.Unmarshal(state, t.state)
}

// Run is started from the beginning when the dataplane task is finished, so
// each step should be idempotent.
func (t *validateImageChecksumTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	imageID := t.request.ImageId

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
			// Data of the image is stored as a snapshot with the same id.
			SnapshotId: imageID,
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

	// The saved checksum is read once again: the image might have been
	// deleted or its checksum might have been saved anew in the meantime.
	savedChecksum, err := t.getSavedChecksum(ctx)
	if err != nil {
		return err
	}

	if savedChecksum != typedResponse.Checksum {
		return errors.NewNonRetriableErrorf(
			"checksum mismatch for image %v: saved %v, actual %v",
			imageID,
			savedChecksum,
			typedResponse.Checksum,
		)
	}

	return nil
}

func (t *validateImageChecksumTask) Cancel(
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

func (t *validateImageChecksumTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *validateImageChecksumTask) GetResponse() proto.Message {
	return &protos.ValidateImageChecksumResponse{
		Checksum: t.state.Checksum,
	}
}

////////////////////////////////////////////////////////////////////////////////

// Returns the checksum saved for the image. Fails if there is nothing to
// validate: the image is not ready or its checksum is not saved.
func (t *validateImageChecksumTask) getSavedChecksum(
	ctx context.Context,
) (uint32, error) {

	imageID := t.request.ImageId

	meta, err := t.storage.GetImageMeta(ctx, imageID)
	if err != nil {
		return 0, err
	}

	if meta == nil || !meta.Ready {
		return 0, errors.NewNonRetriableErrorf(
			"image %v is absent or not ready",
			imageID,
		)
	}

	if !meta.ChecksumCalculated {
		return 0, errors.NewNonRetriableErrorf(
			"checksum of image %v is not saved",
			imageID,
		)
	}

	return meta.Checksum, nil
}
