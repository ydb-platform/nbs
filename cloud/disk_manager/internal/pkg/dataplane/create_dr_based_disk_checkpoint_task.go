package dataplane

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type createDRBasedDiskCheckpointTask struct {
	nbsFactory               nbs_client.Factory
	checkpointIterationLimit int32
	request                  *protos.CreateDRBasedDiskCheckpointRequest
	state                    *protos.CreateDRBasedDiskCheckpointTaskState
}

func (t *createDRBasedDiskCheckpointTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createDRBasedDiskCheckpointTask) Load(request, state []byte) error {
	t.request = &protos.CreateDRBasedDiskCheckpointRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateDRBasedDiskCheckpointTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createDRBasedDiskCheckpointTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.state.CheckpointIteration >= t.checkpointIterationLimit {
		return errors.NewNonRetriableErrorf(
			"Too many failed checkpoint iterations: %v",
			t.state.CheckpointIteration)
	}

	disk := t.request.Disk

	nbsClient, err := t.nbsFactory.GetClient(ctx, disk.ZoneId)
	if err != nil {
		return err
	}

	err = t.updateCheckpoints(ctx, nbsClient)
	if err != nil {
		return err
	}

	// Retry with the same iteration if checkpoint is not ready yet.
	// Retry with new iteration if checkpoint is broken.
	err = nbsClient.EnsureCheckpointReady(
		ctx,
		disk.DiskId,
		t.getCurrentCheckpointID(),
	)
	if err != nil {
		if errors.Is(err, errors.NewEmptyRetriableError()) {
			t.state.CheckpointIteration++
			saveStateErr := execCtx.SaveState(ctx)
			if saveStateErr != nil {
				return saveStateErr
			}
		}

		return err
	}

	t.state.CheckpointId = t.getCurrentCheckpointID()
	return execCtx.SaveState(ctx)
}

func (t *createDRBasedDiskCheckpointTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	nbsClient, err := t.nbsFactory.GetClient(ctx, t.request.Disk.ZoneId)
	if err != nil {
		return nil
	}

	return t.deleteCheckpoints(ctx, nbsClient)
}

func (t *createDRBasedDiskCheckpointTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.CreateDRBasedDiskCheckpointMetadata{
		CheckpointId: t.state.CheckpointId,
	}, nil
}

func (t *createDRBasedDiskCheckpointTask) GetResponse() proto.Message {
	return &protos.CreateDRBasedDiskCheckpointResponse{
		CheckpointId: t.state.CheckpointId,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *createDRBasedDiskCheckpointTask) makeCheckpointID(
	iteration int32,
) string {

	return fmt.Sprintf("%v_%v", t.request.CheckpointIdPrefix, iteration)
}

func (t *createDRBasedDiskCheckpointTask) getCurrentCheckpointID() string {
	return t.makeCheckpointID(t.state.CheckpointIteration)
}

func (t *createDRBasedDiskCheckpointTask) getCheckpointIDs() (string, string) {
	previousCheckpointID := ""
	if t.state.CheckpointIteration > 0 {
		previousCheckpointID = t.makeCheckpointID(
			t.state.CheckpointIteration - 1,
		)
	}

	currentCheckpointID := t.getCurrentCheckpointID()
	return previousCheckpointID, currentCheckpointID
}

func (t *createDRBasedDiskCheckpointTask) updateCheckpoints(
	ctx context.Context,
	nbsClient nbs_client.Client,
) error {

	previousCheckpointID, currentCheckpointID := t.getCheckpointIDs()

	if previousCheckpointID != "" {
		err := nbsClient.DeleteCheckpoint(
			ctx,
			t.request.Disk.DiskId,
			previousCheckpointID,
		)
		if err != nil {
			return err
		}
	}

	return nbsClient.CreateCheckpoint(
		ctx,
		nbs_client.CheckpointParams{
			DiskID:       t.request.Disk.DiskId,
			CheckpointID: currentCheckpointID,
		},
	)
}

func (t *createDRBasedDiskCheckpointTask) deleteCheckpoints(
	ctx context.Context,
	nbsClient nbs_client.Client,
) error {

	previousCheckpointID, currentCheckpointID := t.getCheckpointIDs()

	if previousCheckpointID != "" {
		err := nbsClient.DeleteCheckpoint(
			ctx,
			t.request.Disk.DiskId,
			previousCheckpointID,
		)
		if err != nil {
			return err
		}
	}

	return nbsClient.DeleteCheckpoint(
		ctx,
		t.request.Disk.DiskId,
		currentCheckpointID,
	)
}
