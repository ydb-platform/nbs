package export

import (
	"context"
	"errors"
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
	// Number of bytes exported to the destination.
	Size uint64
	// Number of chunks read from the storage.
	DataChunkCount uint32
	// Number of zero chunks emitted or skipped during export.
	ZeroChunkCount uint32
}

////////////////////////////////////////////////////////////////////////////////

// ValidatePartition validates a 1-based partition number.
func ValidatePartition(partition uint32, partitionCount uint32) error {
	if partitionCount == 0 {
		return task_errors.NewNonRetriableErrorf(
			"partitionCount must be positive, got %v",
			partitionCount,
		)
	}
	if partition == 0 || partition > partitionCount {
		return task_errors.NewNonRetriableErrorf(
			"partition must be in range [1, %v], got %v",
			partitionCount,
			partition,
		)
	}

	return nil
}

func partitionChunkRange(
	chunkCount uint32,
	partition uint32,
	partitionCount uint32,
) (uint32, uint32, error) {

	if err := ValidatePartition(partition, partitionCount); err != nil {
		return 0, 0, err
	}

	partitionIndex := uint64(partition - 1)
	count := uint64(partitionCount)
	chunks := uint64(chunkCount)
	chunksPerPartition := chunks / count
	partitionsWithExtraChunk := chunks % count

	startChunkIndex := partitionIndex * chunksPerPartition
	if partitionIndex < partitionsWithExtraChunk {
		startChunkIndex += partitionIndex
	} else {
		startChunkIndex += partitionsWithExtraChunk
	}

	partitionChunkCount := chunksPerPartition
	if partitionIndex < partitionsWithExtraChunk {
		partitionChunkCount++
	}

	endChunkIndex := startChunkIndex + partitionChunkCount
	return uint32(startChunkIndex), uint32(endChunkIndex), nil
}

func partitionSize(
	snapshotSize uint64,
	startChunkIndex uint32,
	endChunkIndex uint32,
) (uint64, error) {

	if startChunkIndex > endChunkIndex {
		return 0, task_errors.NewNonRetriableErrorf(
			"invalid partition chunk range [%v, %v)",
			startChunkIndex,
			endChunkIndex,
		)
	}
	if startChunkIndex == endChunkIndex {
		return 0, nil
	}

	startOffset := uint64(startChunkIndex) * uint64(chunkSize)
	if startOffset >= snapshotSize {
		return 0, task_errors.NewNonRetriableErrorf(
			"partition starts at chunk %v outside snapshot size %v",
			startChunkIndex,
			snapshotSize,
		)
	}

	endOffset := uint64(endChunkIndex) * uint64(chunkSize)
	if endOffset > snapshotSize {
		endOffset = snapshotSize
	}

	return endOffset - startOffset, nil
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

	return ExportPartitionToWriterWithReadWorkers(
		ctx,
		snapshotStorage,
		snapshotID,
		dst,
		1, // partition
		1, // partitionCount
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

	return ExportPartitionToWriterWithReadWorkers(
		ctx,
		snapshotStorage,
		snapshotID,
		dst,
		1, // partition
		1, // partitionCount
		readWorkerCount,
	)
}

// ExportPartitionToWriter writes one 1-based partition of the snapshot raw
// stream. Partitions are contiguous, chunk-aligned ranges; concatenating them
// in ascending order reconstructs the complete raw snapshot.
func ExportPartitionToWriter(
	ctx context.Context,
	snapshotStorage storage.Storage,
	snapshotID string,
	dst io.Writer,
	partition uint32,
	partitionCount uint32,
) (Stats, error) {

	return ExportPartitionToWriterWithReadWorkers(
		ctx,
		snapshotStorage,
		snapshotID,
		dst,
		partition,
		partitionCount,
		DefaultStreamReadWorkerCount,
	)
}

// ExportPartitionToWriterWithReadWorkers is ExportPartitionToWriter with
// configurable parallel chunk readers. Data chunks are read concurrently but
// written to dst in their original order within the selected partition.
func ExportPartitionToWriterWithReadWorkers(
	ctx context.Context,
	snapshotStorage storage.Storage,
	snapshotID string,
	dst io.Writer,
	partition uint32,
	partitionCount uint32,
	readWorkerCount int,
) (Stats, error) {

	if readWorkerCount <= 0 {
		return Stats{}, task_errors.NewNonRetriableErrorf(
			"readWorkerCount must be positive, got %v",
			readWorkerCount,
		)
	}
	if err := ValidatePartition(partition, partitionCount); err != nil {
		return Stats{}, err
	}

	meta, err := checkSnapshotReadyForExport(ctx, snapshotStorage, snapshotID)
	if err != nil {
		return Stats{}, err
	}

	startChunkIndex, endChunkIndex, err := partitionChunkRange(
		meta.ChunkCount,
		partition,
		partitionCount,
	)
	if err != nil {
		return Stats{}, err
	}

	size, err := partitionSize(meta.Size, startChunkIndex, endChunkIndex)
	if err != nil {
		return Stats{}, err
	}

	logging.Info(
		ctx,
		"exporting partition %v/%v of snapshot %v: chunks [%v, %v), size %v bytes",
		partition,
		partitionCount,
		snapshotID,
		startChunkIndex,
		endChunkIndex,
		size,
	)

	partitionChunkCount := endChunkIndex - startChunkIndex
	if partitionChunkCount == 0 {
		return Stats{Size: size}, nil
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entriesByIndex, hasEntry, err := readStreamChunkMap(
		readCtx,
		snapshotStorage,
		snapshotID,
		startChunkIndex,
		endChunkIndex,
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

	var dataChunkCount, zeroChunkCount uint32
	nextChunkIndex := startChunkIndex
	scheduledChunkIndex := startChunkIndex
	inFlightReads := 0
	readyChunks := make(map[uint32][]byte)
	zeroes := make([]byte, chunkSize)

	scheduleReads := func() error {
		for scheduledChunkIndex < endChunkIndex &&
			inFlightReads < readAheadChunkCount {

			localIndex := scheduledChunkIndex - startChunkIndex
			entry := entriesByIndex[localIndex]
			if hasEntry[localIndex] && len(entry.ChunkID) != 0 {
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

	for nextChunkIndex < endChunkIndex {
		if err := scheduleReads(); err != nil {
			return Stats{}, err
		}

		localIndex := nextChunkIndex - startChunkIndex
		entry := entriesByIndex[localIndex]
		if !hasEntry[localIndex] || len(entry.ChunkID) == 0 {
			err = writeZeroChunk(nextChunkIndex)
			if err != nil {
				return Stats{}, err
			}

			nextChunkIndex++
			logExportProgress(
				ctx,
				nextChunkIndex-startChunkIndex,
				partitionChunkCount,
			)
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
		logExportProgress(
			ctx,
			nextChunkIndex-startChunkIndex,
			partitionChunkCount,
		)
	}

	close(jobs)
	jobsClosed = true

	err = errGroup.Wait()
	workersDone = true
	if err != nil {
		return Stats{}, err
	}

	return Stats{
		Size:           size,
		DataChunkCount: dataChunkCount,
		ZeroChunkCount: zeroChunkCount,
	}, nil
}

func readStreamChunkMap(
	ctx context.Context,
	snapshotStorage storage.Storage,
	snapshotID string,
	startChunkIndex uint32,
	endChunkIndex uint32,
	chunkCount uint32,
) ([]storage.ChunkMapEntry, []bool, error) {

	if startChunkIndex > endChunkIndex || endChunkIndex > chunkCount {
		return nil, nil, task_errors.NewNonRetriableErrorf(
			"invalid chunk range [%v, %v) for snapshot chunk count %v",
			startChunkIndex,
			endChunkIndex,
			chunkCount,
		)
	}

	mapCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, entriesErrors := snapshotStorage.ReadChunkMap(
		mapCtx,
		snapshotID,
		startChunkIndex,
	)

	partitionChunkCount := endChunkIndex - startChunkIndex
	entriesByIndex := make([]storage.ChunkMapEntry, int(partitionChunkCount))
	hasEntry := make([]bool, int(partitionChunkCount))

	var lastEntryIndex uint32
	hasLastEntry := false
	stoppedAtEnd := false

	for entry := range entries {
		if hasLastEntry && entry.ChunkIndex <= lastEntryIndex {
			return nil, nil, task_errors.NewNonRetriableErrorf(
				"chunk map is not ordered: got chunk index %v after %v chunks",
				entry.ChunkIndex,
				lastEntryIndex+1,
			)
		}
		if entry.ChunkIndex < startChunkIndex {
			return nil, nil, task_errors.NewNonRetriableErrorf(
				"chunk index %v is before requested chunk range [%v, %v)",
				entry.ChunkIndex,
				startChunkIndex,
				endChunkIndex,
			)
		}
		if entry.ChunkIndex >= chunkCount {
			return nil, nil, task_errors.NewNonRetriableErrorf(
				"chunk index %v is outside snapshot chunk count %v",
				entry.ChunkIndex,
				chunkCount,
			)
		}

		lastEntryIndex = entry.ChunkIndex
		hasLastEntry = true

		if entry.ChunkIndex >= endChunkIndex {
			stoppedAtEnd = true
			cancel()
			break
		}

		localIndex := entry.ChunkIndex - startChunkIndex
		entriesByIndex[localIndex] = entry
		hasEntry[localIndex] = true
	}

	if stoppedAtEnd {
		for range entries {
		}
	}

	err := <-entriesErrors
	if err != nil {
		if !stoppedAtEnd || ctx.Err() != nil || !errors.Is(err, context.Canceled) {
			return nil, nil, err
		}
	}
	if err := ctx.Err(); err != nil {
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
