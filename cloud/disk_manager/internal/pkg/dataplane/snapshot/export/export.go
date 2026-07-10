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

	meta, err := checkSnapshotReadyForExport(ctx, snapshotStorage, snapshotID)
	if err != nil {
		return Stats{}, err
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, entriesErrors := snapshotStorage.ReadChunkMap(
		readCtx,
		snapshotID,
		0, // milestoneChunkIndex
	)

	data := make([]byte, chunkSize)
	zeroes := make([]byte, chunkSize)
	var dataChunkCount, zeroChunkCount, nextChunkIndex uint32

	writeZeroChunk := func(chunkIndex uint32) error {
		zeroChunkCount++
		return writeStreamChunk(dst, zeroes, chunkIndex, meta.Size)
	}

	for entry := range entries {
		if entry.ChunkIndex < nextChunkIndex {
			return Stats{}, task_errors.NewNonRetriableErrorf(
				"chunk map is not ordered: got chunk index %v after %v chunks",
				entry.ChunkIndex,
				nextChunkIndex,
			)
		}
		if entry.ChunkIndex >= meta.ChunkCount {
			return Stats{}, task_errors.NewNonRetriableErrorf(
				"chunk index %v is outside snapshot chunk count %v",
				entry.ChunkIndex,
				meta.ChunkCount,
			)
		}

		for nextChunkIndex < entry.ChunkIndex {
			err = writeZeroChunk(nextChunkIndex)
			if err != nil {
				return Stats{}, err
			}
			nextChunkIndex++
			logExportProgress(ctx, nextChunkIndex, meta.ChunkCount)
		}

		if len(entry.ChunkID) == 0 {
			err = writeZeroChunk(entry.ChunkIndex)
		} else {
			chunk := dataplane_common.Chunk{
				ID:         entry.ChunkID,
				Index:      entry.ChunkIndex,
				StoredInS3: entry.StoredInS3,
				Data:       data,
			}

			err = snapshotStorage.ReadChunk(readCtx, &chunk)
			if err == nil {
				dataChunkCount++
				err = writeStreamChunk(dst, data, entry.ChunkIndex, meta.Size)
			}
		}
		if err != nil {
			return Stats{}, err
		}

		nextChunkIndex++
		logExportProgress(ctx, nextChunkIndex, meta.ChunkCount)
	}

	err = <-entriesErrors
	if err != nil {
		return Stats{}, err
	}

	for nextChunkIndex < meta.ChunkCount {
		err = writeZeroChunk(nextChunkIndex)
		if err != nil {
			return Stats{}, err
		}
		nextChunkIndex++
		logExportProgress(ctx, nextChunkIndex, meta.ChunkCount)
	}

	return Stats{
		Size:           meta.Size,
		DataChunkCount: dataChunkCount,
		ZeroChunkCount: zeroChunkCount,
	}, nil
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
