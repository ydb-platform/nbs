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

type createShadowDiskBasedCheckpointTask struct {
	nbsFactory nbs_client.Factory
	request    *protos.CreateShadowDiskBasedCheckpointRequest
	state      *protos.CreateShadowDiskBasedCheckpointTaskState
}

func (t *createShadowDiskBasedCheckpointTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createShadowDiskBasedCheckpointTask) Load(request, state []byte) error {
	t.request = &protos.CreateShadowDiskBasedCheckpointRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateShadowDiskBasedCheckpointTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createShadowDiskBasedCheckpointTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	disk := t.request.Disk

	nbsClient, err := t.nbsFactory.GetClient(ctx, disk.ZoneId)
	if err != nil {
		return err
	}

	if t.state.CheckpointId != "" {
		// Nothing to do.
		return nil
	}

	err = t.updateCheckpoints(ctx, nbsClient)
	if err != nil {
		return err
	}

	// Proceed creating snapshot if checkpoint is ready.
	// Retry with the same iteration if checkpoint in not ready yet.
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

func (t *createShadowDiskBasedCheckpointTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	nbsClient, err := t.nbsFactory.GetClient(ctx, t.request.Disk.ZoneId)
	if err != nil {
		return nil
	}

	return t.cleanupCheckpoints(ctx, nbsClient)
}

func (t *createShadowDiskBasedCheckpointTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.CreateShadowDiskBasedCheckpointMetadata{
		CheckpointId: t.state.CheckpointId,
	}, nil
}

func (t *createShadowDiskBasedCheckpointTask) GetResponse() proto.Message {
	return &protos.CreateShadowDiskBasedCheckpointResponse{
		CheckpointId: t.state.CheckpointId,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *createShadowDiskBasedCheckpointTask) makeCheckpointID(index int) string {
	return fmt.Sprintf("%v_%v", t.request.CheckpointIdPrefix, index)
}

func (t *createShadowDiskBasedCheckpointTask) getCurrentCheckpointID() string {
	return t.makeCheckpointID(int(t.state.CheckpointIteration))
}

func (t *createShadowDiskBasedCheckpointTask) getCheckpointIDs() (
	previousCheckpointID string,
	currentCheckpointID string,
) {

	if t.state.CheckpointIteration > 0 {
		previousCheckpointID = t.makeCheckpointID(
			int(t.state.CheckpointIteration) - 1,
		)
	}
	currentCheckpointID = t.getCurrentCheckpointID()
	return
}

func (t *createShadowDiskBasedCheckpointTask) updateCheckpoints(
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

func (t *createShadowDiskBasedCheckpointTask) cleanupCheckpoints(
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
