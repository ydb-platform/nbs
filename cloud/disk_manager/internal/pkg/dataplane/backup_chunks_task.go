package dataplane

import (
	"context"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/bucket"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

// How often copied chunks are marked when fewer than a batch are waiting.
const backupChunksCompleteInterval = time.Second

////////////////////////////////////////////////////////////////////////////////

// Copies the queued chunks of one shard_id range to the follower and ends.
// The dispatcher gives a range to one task at a time, so no other task copies
// these chunks.
//
// The copy is a pipeline: a fetcher reads the next batch of the range while
// the copiers work on the current one, and a completer marks copied chunks in
// batches. There is no pause between batches.
type backupChunksTask struct {
	storage  storage.Storage
	backupS3 *backup.S3
	registry metrics.Registry
	request  *protos.BackupChunksRequest

	// Chunks read from the queue at a time.
	batchSize int
	// Chunks copied at the same time.
	ioDepth int
}

func (t *backupChunksTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *backupChunksTask) Load(request, _ []byte) error {
	t.request = &protos.BackupChunksRequest{}
	return proto.Unmarshal(request, t.request)
}

func (t *backupChunksTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	workers := t.registry.Gauge("backup/workers")
	workers.Add(1)
	defer workers.Add(-1)

	// An item is the queue entries of one chunk: the chunk is copied once for
	// all snapshots that queued it.
	toCopy := make(chan []storage.BackupChunkQueueEntry, t.batchSize)
	copied := make(chan []storage.BackupChunkQueueEntry, t.batchSize)

	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		defer close(toCopy)
		return t.fetch(groupCtx, toCopy)
	})

	var copiers sync.WaitGroup
	for i := 0; i < t.ioDepth; i++ {
		copiers.Add(1)
		group.Go(func() error {
			defer copiers.Done()
			return t.copy(groupCtx, toCopy, copied)
		})
	}

	group.Go(func() error {
		copiers.Wait()
		close(copied)
		return nil
	})

	// The completer runs on the parent context: when the copy stops early, the
	// chunks already copied are still marked and not copied again.
	group.Go(func() error {
		return t.complete(ctx, copied)
	})

	err := group.Wait()

	// The task copies its range for hours, so transient errors of the
	// follower or YDB, such as an S3 500, add up past the retry limit and end
	// it as failed while the copy is fine. They are retried without a limit;
	// the counter shows how often the copy restarts.
	var retriable *errors.RetriableError
	if errors.As(err, &retriable) && !retriable.IgnoreRetryLimit {
		t.registry.Counter("backup/copyRestarts").Inc()
		retriable.IgnoreRetryLimit = true
		return retriable
	}

	return err
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

// Reads the range batch by batch, each after the last entry of the previous
// one, and passes the chunks to the copiers. Entries of one chunk queued by
// several snapshots come one after another in key order; they are passed as
// one item, also across batches.
func (t *backupChunksTask) fetch(
	ctx context.Context,
	toCopy chan<- []storage.BackupChunkQueueEntry,
) error {

	send := func(entries []storage.BackupChunkQueueEntry) error {
		select {
		case toCopy <- entries:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	var after *storage.BackupChunkQueueEntry
	// The entries of the last chunk read so far: the next batch may have more.
	var pending []storage.BackupChunkQueueEntry
	for {
		readStart := time.Now()
		entries, err := t.storage.GetQueuedChunksToBackup(
			ctx,
			t.request.FirstShardId,
			t.request.LastShardId,
			after,
			t.batchSize,
		)
		t.registry.Timer("backup/queueReadTime").RecordDuration(
			time.Since(readStart),
		)
		if err != nil {
			return err
		}

		if len(entries) == 0 {
			break
		}
		after = &entries[len(entries)-1]

		groups := groupByChunk(append(pending, entries...))
		for _, group := range groups[:len(groups)-1] {
			err = send(group)
			if err != nil {
				return err
			}
		}
		pending = groups[len(groups)-1]
	}

	if len(pending) == 0 {
		return nil
	}
	return send(pending)
}

// Splits entries into runs of one chunk, keeping their order.
func groupByChunk(
	entries []storage.BackupChunkQueueEntry,
) [][]storage.BackupChunkQueueEntry {

	var groups [][]storage.BackupChunkQueueEntry
	start := 0
	for i := 1; i <= len(entries); i++ {
		if i == len(entries) || entries[i].ChunkID != entries[start].ChunkID {
			// Capped, so that appending to a group never overwrites the next.
			groups = append(groups, entries[start:i:i])
			start = i
		}
	}
	return groups
}

// A chunk that cannot be read now, such as a 500 from the chunk storage on
// its object, stays in the queue and the rest of the range goes on: the next
// task of the range reads it again, until it is read. Any other retriable
// error, such as the follower or YDB being unavailable, stops the task: the
// framework runs it again later. Any other error belongs to the chunk, such
// as missing or corrupt data: the chunk leaves the queue as failed, which
// fails the backup of its snapshot, and the rest of the range goes on.
func (t *backupChunksTask) copy(
	ctx context.Context,
	toCopy <-chan []storage.BackupChunkQueueEntry,
	copied chan<- []storage.BackupChunkQueueEntry,
) error {

	for entries := range toCopy {
		if ctx.Err() != nil {
			return nil
		}

		// The object is the same for every snapshot of the chunk.
		chunkBlob, err := t.readChunk(ctx, entries[0])
		if errors.Is(err, errors.NewEmptyRetriableError()) {
			logging.Warn(
				ctx,
				"Failed to read chunk %v, it stays in the queue: %v",
				entries[0].ChunkID,
				err,
			)
			continue
		}

		if err == nil {
			err = t.putChunk(ctx, entries[0], chunkBlob)
		}

		if errors.Is(err, errors.NewEmptyRetriableError()) {
			return err
		}

		if err != nil {
			err = t.chunkFailed(ctx, entries, err)
			if err != nil {
				return err
			}
			continue
		}

		select {
		case copied <- entries:
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}

func (t *backupChunksTask) chunkFailed(
	ctx context.Context,
	entries []storage.BackupChunkQueueEntry,
	copyErr error,
) error {

	t.registry.Counter("backup/failedChunks").Inc()

	for _, entry := range entries {
		logging.Warn(
			ctx,
			"Failed to copy chunk %v of snapshot %v, the backup fails: %v",
			entry.ChunkID,
			entry.SnapshotID,
			copyErr,
		)

		err := t.storage.BackupChunkFailed(ctx, entry, copyErr.Error())
		if err != nil {
			return err
		}
	}

	return nil
}

// Marks copied chunks in batches of batchSize, or every
// backupChunksCompleteInterval when fewer are waiting. Ends when the copiers
// are done and copied is closed.
func (t *backupChunksTask) complete(
	ctx context.Context,
	copied <-chan []storage.BackupChunkQueueEntry,
) error {

	var batch []storage.BackupChunkQueueEntry
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		err := t.storage.ChunksBackupCompleted(ctx, batch)
		if err != nil {
			return err
		}

		batch = nil
		return nil
	}

	ticker := time.NewTicker(backupChunksCompleteInterval)
	defer ticker.Stop()

	for {
		select {
		case entries, ok := <-copied:
			if !ok {
				return flush()
			}

			batch = append(batch, entries...)
			if len(batch) >= t.batchSize {
				err := flush()
				if err != nil {
					return err
				}
			}
		case <-ticker.C:
			err := flush()
			if err != nil {
				return err
			}
		}
	}
}

func (t *backupChunksTask) readChunk(
	ctx context.Context,
	entry storage.BackupChunkQueueEntry,
) (chunks.ChunkBlob, error) {

	readStart := time.Now()
	chunkBlob, err := t.storage.ReadChunkBlob(
		ctx,
		entry.ChunkID,
		entry.StoredInS3,
	)
	t.registry.Timer("backup/chunkReadTime").RecordDuration(
		time.Since(readStart),
	)
	return chunkBlob, err
}

func (t *backupChunksTask) putChunk(
	ctx context.Context,
	entry storage.BackupChunkQueueEntry,
	chunkBlob chunks.ChunkBlob,
) error {

	// The follower client waits for the host's bandwidth itself.
	err := t.backupS3.PutObject(
		ctx,
		bucket.ChunkKey(entry.ChunkID),
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
