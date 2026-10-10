package dataplane

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
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

// Copies queued chunks to the follower until the queue is empty or it has
// taken maxChunks. The dispatcher keeps as many of these running as the queue
// needs.
//
// The copy is a pipeline: a fetcher reads the next batch while the copiers
// work on the current one, and a completer marks copied chunks in batches.
// There is no pause between batches.
type backupChunksTask struct {
	storage  storage.Storage
	backupS3 *backup.S3
	registry metrics.Registry

	// Chunks taken from the queue at a time.
	batchSize int
	// Chunks copied at the same time.
	ioDepth int
	// The task ends after taking this many chunks; 0 = no limit.
	maxChunks int
}

func (t *backupChunksTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *backupChunksTask) Load(_, _ []byte) error {
	return nil
}

func (t *backupChunksTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	workers := t.registry.Gauge("backup/workers")
	workers.Add(1)
	defer workers.Add(-1)

	taken := newTakenBackupChunks()
	toCopy := make(chan storage.BackupChunkQueueEntry, t.batchSize)
	copied := make(chan storage.BackupChunkQueueEntry, t.batchSize)

	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		defer close(toCopy)
		return t.fetch(groupCtx, taken, toCopy)
	})

	var copiers sync.WaitGroup
	for i := 0; i < t.ioDepth; i++ {
		copiers.Add(1)
		group.Go(func() error {
			defer copiers.Done()
			t.copy(groupCtx, taken, toCopy, copied)
			return nil
		})
	}

	group.Go(func() error {
		copiers.Wait()
		close(copied)
		return nil
	})

	group.Go(func() error {
		return t.complete(groupCtx, taken, copied)
	})

	err := group.Wait()
	if err != nil {
		return err
	}

	// Failed chunks stay queued; the task is retried and takes them again.
	return taken.firstError()
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

// Reads batches from random places of the queue and passes the chunks this
// task has not taken yet to the copiers. Ends when the queue is empty, when
// only this task's failed chunks are left or after maxChunks.
func (t *backupChunksTask) fetch(
	ctx context.Context,
	taken *takenBackupChunks,
	toCopy chan<- storage.BackupChunkQueueEntry,
) error {

	sent := 0
	for t.maxChunks == 0 || sent < t.maxChunks {
		readStart := time.Now()
		// A random start keeps the batches of concurrent workers apart.
		entries, err := t.storage.GetQueuedChunksToBackup(
			ctx,
			rand.Uint64(),
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

		fresh := taken.take(entries)
		if len(fresh) == 0 {
			// Everything read is this task's: being copied, waiting to be
			// marked or failed.
			if !taken.hasPending() {
				return nil
			}

			select {
			case <-taken.progress:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		for _, entry := range fresh {
			select {
			case toCopy <- entry:
				sent++
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
}

func (t *backupChunksTask) copy(
	ctx context.Context,
	taken *takenBackupChunks,
	toCopy <-chan storage.BackupChunkQueueEntry,
	copied chan<- storage.BackupChunkQueueEntry,
) {

	for entry := range toCopy {
		if ctx.Err() != nil {
			return
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
			taken.failed(entry, err)
			continue
		}

		select {
		case copied <- entry:
		case <-ctx.Done():
			return
		}
	}
}

// Marks copied chunks in batches of batchSize, or every
// backupChunksCompleteInterval when fewer are waiting.
func (t *backupChunksTask) complete(
	ctx context.Context,
	taken *takenBackupChunks,
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

		taken.completed(batch)
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
		case <-ctx.Done():
			return ctx.Err()
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

type backupChunkKey struct {
	snapshotID string
	chunkID    string
}

// Queue entries this task has taken and not marked copied yet. A prefetched
// batch may return them again; they are skipped instead of copied twice.
type takenBackupChunks struct {
	mutex sync.Mutex
	// Taken entries; true once the copy of the entry failed.
	entries map[backupChunkKey]bool
	pending int
	err     error
	// Signalled when an entry stops being pending.
	progress chan struct{}
}

func newTakenBackupChunks() *takenBackupChunks {
	return &takenBackupChunks{
		entries:  make(map[backupChunkKey]bool),
		progress: make(chan struct{}, 1),
	}
}

func backupChunkKeyOf(entry storage.BackupChunkQueueEntry) backupChunkKey {
	return backupChunkKey{
		snapshotID: entry.SnapshotID,
		chunkID:    entry.ChunkID,
	}
}

// Returns the entries not taken before and takes them.
func (c *takenBackupChunks) take(
	entries []storage.BackupChunkQueueEntry,
) []storage.BackupChunkQueueEntry {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	var fresh []storage.BackupChunkQueueEntry
	for _, entry := range entries {
		key := backupChunkKeyOf(entry)
		if _, ok := c.entries[key]; ok {
			continue
		}

		c.entries[key] = false
		c.pending++
		fresh = append(fresh, entry)
	}

	return fresh
}

func (c *takenBackupChunks) completed(
	entries []storage.BackupChunkQueueEntry,
) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, entry := range entries {
		delete(c.entries, backupChunkKeyOf(entry))
		c.pending--
	}
	c.signal()
}

// The entry stays taken: this task does not retry it, the next one will.
func (c *takenBackupChunks) failed(
	entry storage.BackupChunkQueueEntry,
	err error,
) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.entries[backupChunkKeyOf(entry)] = true
	c.pending--
	if c.err == nil {
		c.err = err
	}
	c.signal()
}

func (c *takenBackupChunks) hasPending() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.pending > 0
}

func (c *takenBackupChunks) firstError() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.err
}

func (c *takenBackupChunks) signal() {
	select {
	case c.progress <- struct{}{}:
	default:
	}
}
