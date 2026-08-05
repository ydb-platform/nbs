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
	// Snapshot virtual size in bytes, the destination is truncated to it.
	Size uint64
	// Number of chunks read from the storage.
	DataChunkCount uint32
	// Number of zero chunks (they are never stored, only listed in chunk map).
	ZeroChunkCount uint32
}

////////////////////////////////////////////////////////////////////////////////

// Export reads all chunks of the snapshot (or image) snapshotID from
// snapshotStorage, verifies their checksums and writes them to dst at
// chunkIndex*chunkSize offsets. In the end dst is truncated to the snapshot
// virtual size. The result is a raw disk image. Incremental snapshots don't
// need any special handling: their chunk map is complete, unchanged chunks
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

	meta, err := snapshotStorage.CheckSnapshotReady(ctx, snapshotID)
	if err != nil {
		return Stats{}, err
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
