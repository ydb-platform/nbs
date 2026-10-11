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

		err = t.storage.BackupChunksDeleted(ctx, chunkIDs)
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

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(t.inflightLimit)

	for _, chunkID := range chunkIDs {
		chunkID := chunkID
		group.Go(func() error {
			err := t.backupS3.DeleteChunk(groupCtx, chunkID)
			if err != nil {
				return err
			}

			t.registry.Counter("backup/deletedChunks").Inc()
			return nil
		})
	}

	return group.Wait()
}
