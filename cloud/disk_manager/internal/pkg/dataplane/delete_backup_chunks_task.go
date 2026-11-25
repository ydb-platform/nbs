package dataplane

import (
	"context"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

const backupChunkDeleteRequestSize = 100

// Deletes the follower objects of chunks deleted from chunk_blobs. A chunk is
// queued by its last unref; chunk IDs are never reused, so deleting the object
// later cannot hit a live chunk.
type deleteBackupChunksTask struct {
	storage       storage.Storage
	backupS3      *backup.S3
	batchSize     int
	inflightLimit int
	registry      metrics.Registry
}

func (t *deleteBackupChunksTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *deleteBackupChunksTask) Load(_, _ []byte) error {
	return nil
}

func (t *deleteBackupChunksTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	for {
		chunkIDs, err := t.storage.GetBackupChunksToDelete(ctx, t.batchSize)
		if err != nil {
			return err
		}

		if len(chunkIDs) == 0 {
			return errors.NewInterruptExecutionError()
		}

		err = t.deleteChunks(ctx, chunkIDs)
		if err != nil {
			return err
		}
	}
}

func (t *deleteBackupChunksTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *deleteBackupChunksTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *deleteBackupChunksTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

// Deleting a missing object succeeds, so a repeated batch is safe.
func (t *deleteBackupChunksTask) deleteChunks(
	ctx context.Context,
	chunkIDs []string,
) error {

	if t.inflightLimit < 1 {
		return errors.NewNonRetriableErrorf("DeleteBackupChunksInflightLimit must be positive")
	}

	var deleted []string
	var mu sync.Mutex
	var group errgroup.Group
	group.SetLimit(t.inflightLimit)
	for i := 0; i < len(chunkIDs); i += backupChunkDeleteRequestSize {
		end := min(i+backupChunkDeleteRequestSize, len(chunkIDs))
		batch := chunkIDs[i:end]
		group.Go(func() error {
			ids, err := t.backupS3.DeleteChunks(ctx, batch)
			t.registry.Counter("backup/deletedChunks").Add(int64(len(ids)))

			mu.Lock()
			deleted = append(deleted, ids...)
			mu.Unlock()
			return err
		})
	}

	err := group.Wait()
	if len(deleted) == 0 {
		return err
	}
	// Acknowledge successful deletes even if another key or request failed.
	if err := t.storage.BackupChunksDeleted(ctx, deleted); err != nil {
		return err
	}
	t.registry.Counter("backup/dequeuedChunks").Add(int64(len(deleted)))
	return err
}
