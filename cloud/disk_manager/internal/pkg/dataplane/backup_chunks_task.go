package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

type backupChunksTask struct {
	storage     storage.Storage
	followerS3  *backup.FollowerS3
	batchSize   int
	workerCount int
	registry    metrics.Registry
	state       *protos.BackupChunksTaskState
}

func (t *backupChunksTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *backupChunksTask) Load(_, state []byte) error {
	t.state = &protos.BackupChunksTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupChunksTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	for {
		entries, err := t.storage.GetBackupChunkQueue(ctx, t.batchSize)
		if err != nil {
			return err
		}

		if len(entries) == 0 {
			return errors.NewInterruptExecutionError()
		}

		err = t.copyChunks(ctx, entries)
		if err != nil {
			return err
		}

		err = t.storage.ChunksBackupCompleted(ctx, entries)
		if err != nil {
			return err
		}
	}
}

func (t *backupChunksTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *backupChunksTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *backupChunksTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *backupChunksTask) copyChunk(
	ctx context.Context,
	entry storage.BackupChunkQueueEntry,
) error {

	object, err := t.storage.ReadChunkBlob(ctx, entry.ChunkID)
	if err != nil {
		if errors.Is(err, errors.NewEmptyNonRetriableError()) &&
			errors.IsSilent(err) {

			logging.Warn(
				ctx,
				"Chunk %v of snapshot %v is gone, skipping it",
				entry.ChunkID,
				entry.SnapshotID,
			)
			return nil
		}

		return err
	}

	err = t.followerS3.PutObject(ctx, backup.ChunkKey(entry.ChunkID), object)
	if err != nil {
		return err
	}

	t.registry.Counter("backup/copiedChunks").Inc()
	t.registry.Counter("backup/copiedBytes").Add(int64(len(object.Data)))
	return nil
}

func (t *backupChunksTask) copyChunks(
	ctx context.Context,
	entries []storage.BackupChunkQueueEntry,
) error {

	group, groupCtx := errgroup.WithContext(ctx)
	queue := make(chan storage.BackupChunkQueueEntry)

	group.Go(func() error {
		defer close(queue)

		for _, entry := range entries {
			select {
			case queue <- entry:
			case <-groupCtx.Done():
				return groupCtx.Err()
			}
		}

		return nil
	})

	for i := 0; i < t.workerCount; i++ {
		group.Go(func() error {
			for entry := range queue {
				err := t.copyChunk(groupCtx, entry)
				if err != nil {
					return err
				}
			}

			return nil
		})
	}

	return group.Wait()
}
