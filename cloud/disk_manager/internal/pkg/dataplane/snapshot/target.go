package snapshot

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/accounting"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
)

////////////////////////////////////////////////////////////////////////////////

type snapshotTarget struct {
	uniqueID         string
	snapshotID       string
	storage          storage.Storage
	ignoreZeroChunks bool
	useS3            bool
}

func (t *snapshotTarget) Write(
	ctx context.Context,
	chunk common.Chunk,
) error {

	if t.ignoreZeroChunks && chunk.Zero {
		return nil
	}

	_, err := t.storage.WriteChunk(
		ctx,
		t.uniqueID,
		t.snapshotID,
		chunk,
		t.useS3,
	)
	if err != nil {
		return err
	}

	accounting.OnSnapshotWrite(t.snapshotID, len(chunk.Data))
	return nil
}

func (t *snapshotTarget) Close(ctx context.Context) {
}

////////////////////////////////////////////////////////////////////////////////

func NewSnapshotTarget(
	uniqueID string,
	snapshotID string,
	storage storage.Storage,
	ignoreZeroChunks bool,
	useS3 bool,
) common.Target {

	return &snapshotTarget{
		uniqueID:         uniqueID,
		snapshotID:       snapshotID,
		storage:          storage,
		ignoreZeroChunks: ignoreZeroChunks,
		useS3:            useS3,
	}
}
