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

// Computes the checksum of the image and saves it to the database, see
// validateImageChecksumTask for the task that uses the saved checksum.
// The checksum that is already saved is overwritten.
type saveImageChecksumTask struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	request   *protos.SaveImageChecksumRequest
	state     *protos.SaveImageChecksumTaskState
}

func (t *saveImageChecksumTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *saveImageChecksumTask) Load(request, state []byte) error {
	t.request = &protos.SaveImageChecksumRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.SaveImageChecksumTaskState{}
	return proto.Unmarshal(state, t.state)
}

// Run is started from the beginning when the dataplane task is finished, so
// each step should be idempotent.
func (t *saveImageChecksumTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	imageID := t.request.ImageId

	meta, err := t.storage.GetImageMeta(ctx, imageID)
	if err != nil {
		return err
	}

	if meta == nil || !meta.Ready {
		return errors.NewNonRetriableErrorf(
			"image %v is absent or not ready",
			imageID,
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

	// Fails if the image is not ready anymore: the checksum of an image that
	// is being deleted is of no use.
	return t.storage.ImageChecksumCalculated(
		ctx,
		imageID,
		typedResponse.Checksum,
	)
}

func (t *saveImageChecksumTask) Cancel(
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

func (t *saveImageChecksumTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *saveImageChecksumTask) GetResponse() proto.Message {
	return &protos.SaveImageChecksumResponse{
		Checksum: t.state.Checksum,
	}
}
