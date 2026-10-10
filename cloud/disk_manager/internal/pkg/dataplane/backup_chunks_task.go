package dataplane

import (
	"context"
	"sync"
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

	failures := &backupChunkCopyFailures{limit: t.batchSize}
	toCopy := make(chan storage.BackupChunkQueueEntry, t.batchSize)
	copied := make(chan storage.BackupChunkQueueEntry, t.batchSize)

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
			return t.copy(groupCtx, failures, toCopy, copied)
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
	if err != nil {
		return err
	}

	// Failed chunks stay queued. A retriable error makes the framework run
	// the task again; otherwise the task fails and the dispatcher schedules
	// a new one for the range.
	return failures.first()
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
// one, and passes the chunks to the copiers.
func (t *backupChunksTask) fetch(
	ctx context.Context,
	toCopy chan<- storage.BackupChunkQueueEntry,
) error {

	var after *storage.BackupChunkQueueEntry
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
			return nil
		}

		for _, entry := range entries {
			select {
			case toCopy <- entry:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		after = &entries[len(entries)-1]
	}
}

// Stops the task when a batch worth of chunks failed in a row: the follower
// or the chunk storage is likely down.
func (t *backupChunksTask) copy(
	ctx context.Context,
	failures *backupChunkCopyFailures,
	toCopy <-chan storage.BackupChunkQueueEntry,
	copied chan<- storage.BackupChunkQueueEntry,
) error {

	for entry := range toCopy {
		if ctx.Err() != nil {
			return nil
		}

		err := t.copyChunk(ctx, entry)
		if err != nil {
			logging.Warn(
				ctx,
				"Failed to copy chunk %v of snapshot %v, will retry: %v",
				entry.ChunkID,
				entry.SnapshotID,
				err,
			)
			if failures.add(err) {
				return err
			}
			continue
		}

		failures.reset()

		select {
		case copied <- entry:
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}

// Marks copied chunks in batches of batchSize, or every
// backupChunksCompleteInterval when fewer are waiting. Ends when the copiers
// are done and copied is closed.
func (t *backupChunksTask) complete(
	ctx context.Context,
	copied <-chan storage.BackupChunkQueueEntry,
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
		case entry, ok := <-copied:
			if !ok {
				return flush()
			}

			batch = append(batch, entry)
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

func (t *backupChunksTask) copyChunk(
	ctx context.Context,
	entry storage.BackupChunkQueueEntry,
) error {

	readStart := time.Now()
	chunkBlob, err := t.storage.ReadChunkBlob(
		ctx,
		entry.ChunkID,
		entry.StoredInS3,
	)
	t.registry.Timer("backup/chunkReadTime").RecordDuration(
		time.Since(readStart),
	)
	if err != nil {
		return err
	}

	// The follower client waits for the host's bandwidth itself.
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

////////////////////////////////////////////////////////////////////////////////

type backupChunkCopyFailures struct {
	mutex sync.Mutex
	// Failures in a row that stop the task.
	limit int
	inRow int
	err   error
}

// Returns true when limit chunks failed in a row.
func (f *backupChunkCopyFailures) add(err error) bool {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.err == nil {
		f.err = err
	}
	f.inRow++
	return f.inRow >= f.limit
}

func (f *backupChunkCopyFailures) reset() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.inRow = 0
}

func (f *backupChunkCopyFailures) first() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.err
}
