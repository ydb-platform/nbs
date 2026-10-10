package dataplane

import (
	"context"
	"math/rand"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

type chunkCopyLimiter interface {
	Wait(ctx context.Context, bytes int) error
}

////////////////////////////////////////////////////////////////////////////////

// Copies queued chunks to the follower until the queue is empty or, if
// lifetime is not zero, until it has run that long. The dispatcher keeps as
// many of these running as the queue needs.
type backupChunksTask struct {
	storage   storage.Storage
	backupS3  *backup.S3
	limiter   chunkCopyLimiter
	batchSize int
	ioDepth   int
	lifetime  time.Duration
	registry  metrics.Registry
	state     *protos.BackupChunksTaskState
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

	deadline := time.Now().Add(t.lifetime)

	for {
		// A random start keeps the batches of concurrent workers apart.
		entries, err := t.storage.GetQueuedChunksToBackup(
			ctx,
			rand.Uint64(),
			t.batchSize,
		)
		if err != nil {
			return err
		}

		if len(entries) == 0 {
			return nil
		}

		copied, err := t.copyChunks(ctx, entries)
		if len(copied) == 0 {
			return err
		}

		err = t.storage.ChunksBackupCompleted(ctx, copied)
		if err != nil {
			return err
		}

		if t.lifetime > 0 && time.Now().After(deadline) {
			return nil
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

	chunkBlob, err := t.storage.ReadChunkBlob(
		ctx,
		entry.ChunkID,
		entry.StoredInS3,
	)
	if err != nil {
		return err
	}

	waitStart := time.Now()
	err = t.limiter.Wait(ctx, len(chunkBlob.Data))
	if err != nil {
		return err
	}
	t.registry.Counter("backup/bandwidthWaitMs").Add(
		time.Since(waitStart).Milliseconds(),
	)

	err = t.backupS3.PutObject(
		ctx,
		backup.ChunkKey(entry.ChunkID),
		entry.EncryptedDEK,
		chunks.NewS3Object(chunkBlob),
	)
	if err != nil {
		return err
	}

	t.registry.Counter("backup/copiedChunks").Inc()
	t.registry.Counter("backup/copiedBytes").Add(int64(len(chunkBlob.Data)))
	return nil
}

// Returns the copied chunks and the first error of the ones that failed.
func (t *backupChunksTask) copyChunks(
	ctx context.Context,
	entries []storage.BackupChunkQueueEntry,
) ([]storage.BackupChunkQueueEntry, error) {

	group, groupCtx := errgroup.WithContext(ctx)
	queue := make(chan storage.BackupChunkQueueEntry)
	copiedEntries := make(chan storage.BackupChunkQueueEntry, len(entries))
	copyErrors := make(chan error, len(entries))

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

	for i := 0; i < t.ioDepth; i++ {
		group.Go(func() error {
			for entry := range queue {
				if groupCtx.Err() != nil {
					return groupCtx.Err()
				}

				err := t.copyChunk(groupCtx, entry)
				if err != nil {
					logging.Warn(
						groupCtx,
						"Failed to copy chunk %v of snapshot %v, will retry: %v",
						entry.ChunkID,
						entry.SnapshotID,
						err,
					)
					copyErrors <- err
					continue
				}

				copiedEntries <- entry
			}

			return nil
		})
	}

	err := group.Wait()
	if err != nil {
		return nil, err
	}

	close(copiedEntries)
	close(copyErrors)

	var copied []storage.BackupChunkQueueEntry
	for entry := range copiedEntries {
		copied = append(copied, entry)
	}

	return copied, <-copyErrors
}
