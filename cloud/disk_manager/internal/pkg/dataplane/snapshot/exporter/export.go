package exporter

import (
	"context"
	"errors"
	"io"

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

type Stats struct {
	// Number of bytes exported to the writer.
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

// ExportPartitionToWriterWithReadWorkers writes one 1-based partition of the
// snapshot raw stream. Partitions are contiguous, chunk-aligned ranges;
// concatenating them in ascending order reconstructs the complete raw snapshot.
// Data chunks are read concurrently but written to dst in their original order
// within the selected partition.
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
