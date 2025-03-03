package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type createShadowDiskBasedCheckpointTask struct {
	nbsFactory nbs_client.Factory
	request *protos.CreateShadowDiskBasedCheckpointRequest
	state   *protos.CreateShadowDiskBasedCheckpointTaskState
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

	disk := t.request.SrcDisk

	nbsClient, err := t.nbsFactory.GetClient(ctx, disk.ZoneId)
	if err != nil {
		return err
	}

	if t.state.CheckpointID != "" {
		// Nothing to do.
		return nil
	}

	err = t.updateCheckpoint(ctx, nbsClient)
	if err != nil {
		return err
	}

	err = t.handleCheckpointStatus(
		ctx,
		execCtx,
		nbsClient,
		disk.DiskId,
		t.getCurrentCheckpointID(),
	)
	if err != nil {
		return err
	}

	t.state.CheckpointID = t.getCurrentCheckpointID()
	return execCtx.SaveState(ctx)
}

func (t *createShadowDiskBasedCheckpointTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	nbsClient, err := t.nbsFactory.GetClient(ctx, t.request.SrcDisk.ZoneId)
	if err != nil {
		return nil
	}

	return t.cleanupCheckpoints(ctx, nbsClient)
}

func (t *createShadowDiskBasedCheckpointTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *createShadowDiskBasedCheckpointTask) GetResponse() proto.Message {
	return &protos.CreateShadowDiskBasedCheckpointResponse{
		CheckpointId: t.state.CheckpointId,
	}
}

////////////////////////////////////////////////////////////////////////////////

// Proceed creating snapshot if checkpoint is ready.
// Retry with the same iteration if checkpoint in not ready yet.
// Retry with new iteration if checkpoint is broken.
func (t *createSnapshotFromDiskTask) handleCheckpointStatus(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	nbsClient nbs.Client,
	diskID string,
	checkpointID string,
) error {

	err := nbsClient.EnsureCheckpointReady(ctx, diskID, checkpointID)
	if errors.Is(err, errors.NewEmptyRetriableError()) {
		t.state.CheckpointIteration++
		saveStateErr := execCtx.SaveState(ctx)
		if saveStateErr != nil {
			return saveStateErr
		}
	}

	return err
}

////////////////////////////////////////////////////////////////////////////////

func (t *createSnapshotFromDiskTask) makeCheckpointID(index int) string {
	return fmt.Sprintf("%v_%v", t.request.CheckpointIdPrefix, index)
}

func (t *createSnapshotFromDiskTask) getCurrentCheckpointID() string {
	return t.makeCheckpointID(int(t.state.CheckpointIteration))
}

func (t *createSnapshotFromDiskTask) deletePreviousCheckpoint(
	ctx context.Context,
	nbsClient nbs.Client,
) error {

	if t.state.CheckpointIteration == 0 {
		// No previous checkpoint, nothing to do.
		return nil
	}

	checkpointID := t.makeCheckpointID(
		int(t.state.CheckpointIteration) - 1,
	)

	return nbsClient.DeleteCheckpoint(
		ctx,
		t.request.SrcDisk.DiskId,
		checkpointID,
	)
}

func (t *createSnapshotFromDiskTask) updateCheckpoint(
	ctx context.Context,
	nbsClient nbs.Client,
) error {

	err := t.deletePreviousCheckpoint(ctx, nbsClient)
	if err != nil {
		return err
	}

	return nbsClient.CreateCheckpoint(
		ctx,
		nbs.CheckpointParams{
			DiskID:       t.request.SrcDisk.DiskId,
			CheckpointID: t.getCurrentCheckpointID(),
		},
	)
}

func (t *createSnapshotFromDiskTask) cleanupCheckpoints(
	ctx context.Context,
	nbsClient nbs.Client,
) error {

	err := t.deletePreviousCheckpoint(ctx, nbsClient)
	if err != nil {
		return err
	}

	return nbsClient.DeleteCheckpoint(
		ctx,
		t.request.SrcDisk.DiskId,
		t.getCurrentCheckpointID(),
	)
}
