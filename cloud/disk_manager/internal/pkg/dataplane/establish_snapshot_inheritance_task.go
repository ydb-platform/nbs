package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type establishSnapshotInheritanceTask struct {
	storage   storage.Storage
	batchSize int
	request   *protos.EstablishSnapshotInheritanceRequest
	state     *protos.EstablishSnapshotInheritanceTaskState
}

func (t *establishSnapshotInheritanceTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *establishSnapshotInheritanceTask) Load(request, state []byte) error {
	t.request = &protos.EstablishSnapshotInheritanceRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.EstablishSnapshotInheritanceTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *establishSnapshotInheritanceTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.SnapshotId
	if len(snapshotID) == 0 {
		return errors.NewNonRetriableErrorf("snapshot ID must not be empty")
	}

	if t.state.InheritanceEstablished {
		return t.storage.UnlockSnapshot(ctx, snapshotID, execCtx.GetTaskID())
	}

	if t.batchSize <= 0 {
		return errors.NewNonRetriableErrorf(
			"snapshot inheritance batch size must be positive",
		)
	}

	locked, err := t.storage.LockSnapshot(ctx, snapshotID, execCtx.GetTaskID())
	if err != nil || !locked {
		// Missing and deleting targets need no backfill.
		return err
	}

	meta, err := t.storage.GetSnapshotMeta(ctx, snapshotID)
	if err != nil {
		return err
	}

	if meta == nil || !meta.Ready {
		// A lock prevents deletion, but does not stop chunk-map writes during
		// creation. Wait for readiness without blocking creation cancellation.
		err = t.storage.UnlockSnapshot(ctx, snapshotID, execCtx.GetTaskID())
		if err != nil {
			return err
		}

		return errors.NewInterruptExecutionError()
	}

	err = t.establishInheritance(ctx, execCtx, meta.ChunkCount)
	if err != nil {
		return err
	}

	// Save completion before unlocking: a retry after unlock must not scan a
	// snapshot whose deletion may already have started.
	t.state.InheritanceEstablished = true
	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	return t.storage.UnlockSnapshot(ctx, snapshotID, execCtx.GetTaskID())
}

func (t *establishSnapshotInheritanceTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.storage.UnlockSnapshot(
		ctx,
		t.request.SnapshotId,
		execCtx.GetTaskID(),
	)
}

func (t *establishSnapshotInheritanceTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *establishSnapshotInheritanceTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *establishSnapshotInheritanceTask) saveParents(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	parents map[string]struct{},
	milestone uint32,
) error {

	if len(parents) != 0 {
		err := t.storage.AddSnapshotParents(ctx, t.request.SnapshotId, parents)
		if err != nil {
			return err
		}
	}

	// Parent rows must be durable before advancing the scan cursor. Replaying
	// a batch after an interrupted save is safe because parent writes are
	// upserts.
	t.state.MilestoneChunkIndex = milestone
	return execCtx.SaveState(ctx)
}

func (t *establishSnapshotInheritanceTask) establishInheritance(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	chunkCount uint32,
) error {

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, entriesErrors := t.storage.ReadChunkMap(
		readCtx,
		t.request.SnapshotId,
		t.state.MilestoneChunkIndex,
	)

	parents := make(map[string]struct{})
	processed := 0
	milestone := t.state.MilestoneChunkIndex

	for entry := range entries {
		err := validateChunkMapEntry(entry, t.request.SnapshotId, chunkCount)
		if err != nil {
			return err
		}

		if len(entry.ChunkID) != 0 {
			parentID := storage.GetSnapshotIDFromChunkID(entry.ChunkID)
			if len(parentID) == 0 {
				return errors.NewNonRetriableErrorf(
					"invalid chunk ID %q",
					entry.ChunkID,
				)
			}

			if parentID != t.request.SnapshotId {
				parents[parentID] = struct{}{}
			}
		}

		milestone = entry.ChunkIndex + 1
		processed++
		if processed >= t.batchSize {
			err = t.saveParents(ctx, execCtx, parents, milestone)
			if err != nil {
				return err
			}

			parents = make(map[string]struct{})
			processed = 0
		}
	}

	err := <-entriesErrors
	if err != nil {
		return err
	}

	if processed != 0 {
		return t.saveParents(ctx, execCtx, parents, milestone)
	}

	return nil
}
