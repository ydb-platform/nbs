package dataplane

import (
	"context"

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

		deleted, deleteErr := t.deleteChunks(ctx, chunkIDs)
		if len(deleted) != 0 {
			err = t.storage.BackupChunksDeleted(ctx, deleted)
			if err != nil {
				return err
			}
			t.registry.Counter("backup/dequeuedChunks").Add(int64(len(deleted)))
		}
		if deleteErr != nil {
			return deleteErr
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
) ([]string, error) {

	if t.inflightLimit < 1 {
		return nil, errors.NewNonRetriableErrorf("DeleteBackupChunksInflightLimit must be positive")
	}

	// An S3 error in one request must not cancel other requests in this batch.
	var group errgroup.Group
	group.SetLimit(t.inflightLimit)
	requestCount := (len(chunkIDs) + backupChunkDeleteRequestSize - 1) / backupChunkDeleteRequestSize
	results := make([][]string, requestCount)
	for i := range results {
		i := i
		first := i * backupChunkDeleteRequestSize
		end := min(first+backupChunkDeleteRequestSize, len(chunkIDs))
		requestIDs := chunkIDs[first:end]
		group.Go(func() error {
			deleted, err := t.backupS3.DeleteChunks(ctx, requestIDs)
			t.registry.Counter("backup/deletedChunks").Add(int64(len(deleted)))
			results[i] = deleted
			return err
		})
	}

	err := group.Wait()
	deleted := make([]string, 0, len(chunkIDs))
	for _, ids := range results {
		deleted = append(deleted, ids...)
	}
	return deleted, err
}
