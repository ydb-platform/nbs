package export

import (
	"context"
	"io"
	"sync/atomic"

	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

// Must be equal to chunkSize used by dataplane tasks that create snapshots
// (see internal/pkg/dataplane/consts.go). ReadChunk verifies the checksum of
// the whole chunk, so a mismatch results in an error, not in corrupted output.
const chunkSize = 4 * 1024 * 1024

// Progress is logged after each logProgressChunkCount processed chunks.
const logProgressChunkCount = 1024

// DefaultStreamReadWorkerCount controls parallel chunk reads for non-seekable
// stream exports. Stdout still receives chunks in strict index order.
const DefaultStreamReadWorkerCount = 16

const streamReadAheadMultiplier = 4

////////////////////////////////////////////////////////////////////////////////

// Destination receives the exported snapshot data, e.g. *os.File.
// It should not contain any data before the export: zero chunks are skipped,
// not written, so the destination is expected to read back as zeroes there.
type Destination interface {
	io.WriterAt
	Truncate(size int64) error
}

type Stats struct {
	// Snapshot virtual size in bytes.
	Size uint64
	// Number of chunks read from the storage.
	DataChunkCount uint32
	// Number of zero chunks emitted or skipped during export.
	ZeroChunkCount uint32
}

////////////////////////////////////////////////////////////////////////////////

func checkSnapshotReadyForExport(
	ctx context.Context,
	snapshotStorage storage.Storage,
	snapshotID string,
) (storage.SnapshotMeta, error) {

	meta, err := snapshotStorage.CheckSnapshotReady(ctx, snapshotID)
	if err != nil {
		return storage.SnapshotMeta{}, err
	}

	if meta.Encryption.GetMode() != types.EncryptionMode_NO_ENCRYPTION {
		logging.Warn(
			ctx,
			"snapshot %v is encrypted, the exported data is encrypted as well",
			snapshotID,
		)
	}

	logging.Info(
		ctx,
		"exporting snapshot %v: size %v bytes, %v chunks",
		snapshotID,
		meta.Size,
		meta.ChunkCount,
	)

	return meta, nil
}

////////////////////////////////////////////////////////////////////////////////

// Export reads all chunks of the snapshot (or image) snapshotID from
// snapshotStorage, verifies their checksums and writes them to dst at
// chunkIndex*chunkSize offsets. In the end dst is truncated to the snapshot
// virtual size. The result is a raw disk image. Zero chunks may be absent
// from the chunk map; they are left as holes and materialized by truncation.
// Incremental snapshots don't need any special handling: unchanged chunks
// are shallow copies of the base snapshot chunks.
func Export(
	ctx context.Context,
	snapshotStorage storage.Storage,
	snapshotID string,
	dst Destination,
	workerCount int,
) (Stats, error) {

	if workerCount <= 0 {
		return Stats{}, task_errors.NewNonRetriableErrorf(
			"workerCount must be positive, got %v",
			workerCount,
		)
	}

	meta, err := checkSnapshotReadyForExport(ctx, snapshotStorage, snapshotID)
	if err != nil {
		return Stats{}, err
	}

	var dataChunkCount, zeroChunkCount, processedChunkCount atomic.Uint32

	errGroup, groupCtx := errgroup.WithContext(ctx)

	entries, entriesErrors := snapshotStorage.ReadChunkMap(
		groupCtx,
		snapshotID,
		0, // milestoneChunkIndex
	)

	for i := 0; i < workerCount; i++ {
		errGroup.Go(func() error {
			data := make([]byte, chunkSize)

			for entry := range entries {
				if len(entry.ChunkID) == 0 {
					// Zero chunks have no data and are skipped.
					zeroChunkCount.Add(1)
				} else {
					err := exportChunk(
						groupCtx,
						snapshotStorage,
						entry,
						data,
						dst,
					)
					if err != nil {
						return err
					}

					dataChunkCount.Add(1)
				}

				processed := processedChunkCount.Add(1)
				if processed%logProgressChunkCount == 0 {
					logging.Info(
						ctx,
						"exported %v/%v chunks",
						processed,
						meta.ChunkCount,
					)
				}
			}

			return nil
		})
	}

	err = errGroup.Wait()
	if err != nil {
		return Stats{}, err
	}

	err = <-entriesErrors
	if err != nil {
		return Stats{}, err
	}

	err = dst.Truncate(int64(meta.Size))
	if err != nil {
		return Stats{}, err
	}

	return Stats{
		Size:           meta.Size,
		DataChunkCount: dataChunkCount.Load(),
		ZeroChunkCount: zeroChunkCount.Load(),
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

// ExportToWriter reads the snapshot (or image) snapshotID and writes it to dst as
// a sequential raw disk image stream. Unlike Export, it can write to non-seekable
// destinations such as stdout, so zero chunks are written explicitly,
// including zero chunks that are absent from the chunk map.
func ExportToWriter(
	ctx context.Context,
	snapshotStorage storage.Storage,
	snapshotID string,
	dst io.Writer,
) (Stats, error) {

	return ExportToWriterWithReadWorkers(
		ctx,
		snapshotStorage,
		snapshotID,
		dst,
		DefaultStreamReadWorkerCount,
	)
}

// ExportToWriterWithReadWorkers is ExportToWriter with configurable parallel
// chunk readers. It reads data chunks concurrently, but writes the raw stream to
// dst in chunk index order.
func ExportToWriterWithReadWorkers(
	ctx context.Context,
	snapshotStorage storage.Storage,
	snapshotID string,
	dst io.Writer,
	readWorkerCount int,
) (Stats, error) {

	if readWorkerCount <= 0 {
		return Stats{}, task_errors.NewNonRetriableErrorf(
			"readWorkerCount must be positive, got %v",
			readWorkerCount,
		)
	}

	meta, err := checkSnapshotReadyForExport(ctx, snapshotStorage, snapshotID)
	if err != nil {
		return Stats{}, err
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entriesByIndex, hasEntry, err := readStreamChunkMap(
		readCtx,
		snapshotStorage,
		snapshotID,
		meta.ChunkCount,
	)
	if err != nil {
		return Stats{}, err
	}

	type streamReadResult struct {
		chunkIndex uint32
		data       []byte
		err        error
	}

	readAheadChunkCount := readWorkerCount * streamReadAheadMultiplier
	if readAheadChunkCount < readWorkerCount {
		readAheadChunkCount = readWorkerCount
	}

	jobs := make(chan storage.ChunkMapEntry)
	results := make(chan streamReadResult, readAheadChunkCount)

	errGroup, groupCtx := errgroup.WithContext(readCtx)
	for i := 0; i < readWorkerCount; i++ {
		errGroup.Go(func() error {
			for {
				var entry storage.ChunkMapEntry
				var ok bool

				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case entry, ok = <-jobs:
					if !ok {
						return nil
					}
				}

				data := make([]byte, chunkSize)
				chunk := dataplane_common.Chunk{
					ID:         entry.ChunkID,
					Index:      entry.ChunkIndex,
					StoredInS3: entry.StoredInS3,
					Data:       data,
				}

				err := snapshotStorage.ReadChunk(groupCtx, &chunk)
				if err != nil {
					data = nil
				}

				select {
				case results <- streamReadResult{
					chunkIndex: entry.ChunkIndex,
					data:       data,
					err:        err,
				}:
				case <-groupCtx.Done():
					return groupCtx.Err()
				}
			}
		})
	}

	jobsClosed := false
	workersDone := false
	defer func() {
		if !jobsClosed {
			close(jobs)
		}
		cancel()
		if !workersDone {
			_ = errGroup.Wait()
		}
	}()

	var dataChunkCount, zeroChunkCount, nextChunkIndex uint32
	var scheduledChunkIndex uint32
	inFlightReads := 0
	readyChunks := make(map[uint32][]byte)
	zeroes := make([]byte, chunkSize)

	scheduleReads := func() error {
		for scheduledChunkIndex < meta.ChunkCount && inFlightReads < readAheadChunkCount {
			entry := entriesByIndex[scheduledChunkIndex]
			if hasEntry[scheduledChunkIndex] && len(entry.ChunkID) != 0 {
				select {
				case jobs <- entry:
					inFlightReads++
				case <-readCtx.Done():
					return readCtx.Err()
				}
			}

			scheduledChunkIndex++
		}

		return nil
	}

	writeZeroChunk := func(chunkIndex uint32) error {
		zeroChunkCount++
		return writeStreamChunk(dst, zeroes, chunkIndex, meta.Size)
	}

	for nextChunkIndex < meta.ChunkCount {
		if err := scheduleReads(); err != nil {
			return Stats{}, err
		}

		entry := entriesByIndex[nextChunkIndex]
		if !hasEntry[nextChunkIndex] || len(entry.ChunkID) == 0 {
			err = writeZeroChunk(nextChunkIndex)
			if err != nil {
				return Stats{}, err
			}

			nextChunkIndex++
			logExportProgress(ctx, nextChunkIndex, meta.ChunkCount)
			continue
		}

		data, ready := readyChunks[nextChunkIndex]
		for !ready {
			select {
			case result := <-results:
				inFlightReads--
				if result.err != nil {
					return Stats{}, result.err
				}

				if result.chunkIndex == nextChunkIndex {
					data = result.data
					ready = true
				} else {
					readyChunks[result.chunkIndex] = result.data
				}
			case <-readCtx.Done():
				return Stats{}, readCtx.Err()
			}

			if err := scheduleReads(); err != nil {
				return Stats{}, err
			}
		}

		delete(readyChunks, nextChunkIndex)
		err = writeStreamChunk(dst, data, nextChunkIndex, meta.Size)
		if err != nil {
			return Stats{}, err
		}

		dataChunkCount++
		nextChunkIndex++
		logExportProgress(ctx, nextChunkIndex, meta.ChunkCount)
	}

	close(jobs)
	jobsClosed = true

	err = errGroup.Wait()
	workersDone = true
	if err != nil {
		return Stats{}, err
	}

	return Stats{
		Size:           meta.Size,
		DataChunkCount: dataChunkCount,
		ZeroChunkCount: zeroChunkCount,
	}, nil
}

func readStreamChunkMap(
	ctx context.Context,
	snapshotStorage storage.Storage,
	snapshotID string,
	chunkCount uint32,
) ([]storage.ChunkMapEntry, []bool, error) {

	entries, entriesErrors := snapshotStorage.ReadChunkMap(
		ctx,
		snapshotID,
		0, // milestoneChunkIndex
	)

	entriesByIndex := make([]storage.ChunkMapEntry, int(chunkCount))
	hasEntry := make([]bool, int(chunkCount))

	var lastEntryIndex uint32
	hasLastEntry := false

	for entry := range entries {
		if hasLastEntry && entry.ChunkIndex <= lastEntryIndex {
			return nil, nil, task_errors.NewNonRetriableErrorf(
				"chunk map is not ordered: got chunk index %v after %v chunks",
				entry.ChunkIndex,
				lastEntryIndex+1,
			)
		}
		if entry.ChunkIndex >= chunkCount {
			return nil, nil, task_errors.NewNonRetriableErrorf(
				"chunk index %v is outside snapshot chunk count %v",
				entry.ChunkIndex,
				chunkCount,
			)
		}

		entriesByIndex[entry.ChunkIndex] = entry
		hasEntry[entry.ChunkIndex] = true
		lastEntryIndex = entry.ChunkIndex
		hasLastEntry = true
	}

	err := <-entriesErrors
	if err != nil {
		return nil, nil, err
	}

	return entriesByIndex, hasEntry, nil
}

func logExportProgress(ctx context.Context, processedChunkCount uint32, chunkCount uint32) {
	if processedChunkCount%logProgressChunkCount == 0 {
		logging.Info(
			ctx,
			"exported %v/%v chunks",
			processedChunkCount,
			chunkCount,
		)
	}
}

////////////////////////////////////////////////////////////////////////////////

func exportChunk(
	ctx context.Context,
	snapshotStorage storage.Storage,
	entry storage.ChunkMapEntry,
	data []byte,
	dst Destination,
) error {

	chunk := dataplane_common.Chunk{
		ID:         entry.ChunkID,
		Index:      entry.ChunkIndex,
		StoredInS3: entry.StoredInS3,
		Data:       data,
	}

	err := snapshotStorage.ReadChunk(ctx, &chunk)
	if err != nil {
		return err
	}

	_, err = dst.WriteAt(data, int64(entry.ChunkIndex)*chunkSize)
	return err
}

func writeStreamChunk(
	dst io.Writer,
	data []byte,
	chunkIndex uint32,
	size uint64,
) error {

	offset := uint64(chunkIndex) * chunkSize
	if offset >= size {
		return task_errors.NewNonRetriableErrorf(
			"chunk index %v is outside snapshot size %v",
			chunkIndex,
			size,
		)
	}

	length := uint64(len(data))
	if offset+length > size {
		length = size - offset
	}

	return writeAll(dst, data[:int(length)])
}

func writeAll(dst io.Writer, data []byte) error {
	for len(data) != 0 {
		n, err := dst.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}

	return nil
}
