package exporter

import (
	"context"
	"errors"
	"io"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

// Progress is logged after each logProgressChunkCount processed chunks.
const logProgressChunkCount = 1024

// DefaultStreamReadWorkerCount controls parallel chunk reads for non-seekable
// stream exports. Stdout still receives chunks in strict index order.
const DefaultStreamReadWorkerCount = 16

const readAheadMultiplier = 4

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

// ExportSnapshot writes the snapshot raw stream. Data chunks are read
// concurrently but written to dst in their original order.
//
// Chunk map reading, entry caching and zero chunk detection are delegated to
// snapshot.NewSnapshotSource, the same source implementation that dataplane
// transfer tasks use.
func ExportSnapshot(
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

	if meta.ChunkCount == 0 {
		return Stats{Size: meta.Size}, nil
	}

	return newSnapshotExporter(
		snapshotStorage,
		snapshotID,
		meta,
		readWorkerCount,
	).export(ctx, dst)
}

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
			"snapshot %v has encryption metadata; exported data is the stored ciphertext, not decrypted plaintext",
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

// One position of the raw stream: either a zero chunk or a data chunk that a
// background reader fills in.
type pendingChunk struct {
	chunkIndex uint32
	zero       bool
	data       []byte
	err        error
	// Closed when the read completes; nil for chunks absent from the map.
	ready chan struct{}
}

// snapshotExporter streams snapshot chunks to a sequential writer. The
// scheduler sends chunks to pendingChunks in stream order, data chunks are
// read concurrently by at most cap(readSlots) readers, and writeChunks writes
// everything to the destination strictly in order.
type snapshotExporter struct {
	source dataplane_common.Source
	meta   storage.SnapshotMeta

	errGroup *errgroup.Group

	// Chunks in stream order; the capacity bounds the read-ahead.
	pendingChunks chan *pendingChunk
	// Semaphore limiting concurrent chunk reads.
	readSlots chan struct{}
	// Written chunk indices, returned to the source inflight queue.
	processedChunkIndices chan uint32

	chunkIndices       <-chan uint32
	chunkIndicesErrors <-chan error
}

func newSnapshotExporter(
	snapshotStorage storage.Storage,
	snapshotID string,
	meta storage.SnapshotMeta,
	readWorkerCount int,
) *snapshotExporter {

	readAheadChunkCount := readWorkerCount * readAheadMultiplier
	return &snapshotExporter{
		source: snapshot.NewSnapshotSource(snapshotID, snapshotStorage),
		meta:   meta,

		pendingChunks:         make(chan *pendingChunk, readAheadChunkCount),
		readSlots:             make(chan struct{}, readWorkerCount),
		processedChunkIndices: make(chan uint32, readAheadChunkCount),
	}
}

func (e *snapshotExporter) export(
	ctx context.Context,
	dst io.Writer,
) (Stats, error) {

	exportCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	defer e.source.Close(ctx)
	// Closing the channel stops the inflight queue goroutine of the source.
	defer close(e.processedChunkIndices)

	e.chunkIndices, _, e.chunkIndicesErrors = e.source.ChunkIndices(
		exportCtx,
		dataplane_common.Milestone{},
		e.processedChunkIndices,
		common.ChannelWithCancellation{}, // holeChunkIndices
	)

	var groupCtx context.Context
	e.errGroup, groupCtx = errgroup.WithContext(exportCtx)
	e.errGroup.Go(func() error {
		return e.scheduleChunks(groupCtx)
	})

	stats, err := e.writeChunks(groupCtx, dst)
	if err != nil {
		cancel()
		// Prefer the causal pipeline error over the cancellation it triggered.
		waitErr := e.errGroup.Wait()
		if waitErr != nil && errors.Is(err, context.Canceled) {
			return Stats{}, waitErr
		}

		return Stats{}, err
	}

	err = e.errGroup.Wait()
	if err != nil {
		return Stats{}, err
	}

	return stats, nil
}

// scheduleChunks turns the sorted chunk index stream of the source into
// pendingChunks. Indices absent from the chunk map are zero chunks: every gap
// between consecutive indices is a range of holes.
func (e *snapshotExporter) scheduleChunks(ctx context.Context) error {
	defer close(e.pendingChunks)

	nextChunkIndex := uint32(0)
	hasPrevChunkIndex := false
	var prevChunkIndex uint32

	for {
		var chunkIndex uint32
		var ok bool

		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunkIndex, ok = <-e.chunkIndices:
		}
		if !ok {
			break
		}

		if hasPrevChunkIndex && chunkIndex <= prevChunkIndex {
			return task_errors.NewNonRetriableErrorf(
				"chunk map is not ordered: got chunk index %v after %v",
				chunkIndex,
				prevChunkIndex,
			)
		}
		if chunkIndex >= e.meta.ChunkCount {
			return task_errors.NewNonRetriableErrorf(
				"chunk index %v is outside snapshot chunk count %v",
				chunkIndex,
				e.meta.ChunkCount,
			)
		}
		prevChunkIndex = chunkIndex
		hasPrevChunkIndex = true

		err := e.scheduleZeroChunks(ctx, nextChunkIndex, chunkIndex)
		if err != nil {
			return err
		}

		err = e.scheduleDataChunk(ctx, chunkIndex)
		if err != nil {
			return err
		}

		nextChunkIndex = chunkIndex + 1
	}

	err := <-e.chunkIndicesErrors
	if err != nil {
		return err
	}

	// The rest of the snapshot is not present in the chunk map.
	return e.scheduleZeroChunks(ctx, nextChunkIndex, e.meta.ChunkCount)
}

func (e *snapshotExporter) scheduleZeroChunks(
	ctx context.Context,
	fromChunkIndex uint32,
	toChunkIndex uint32,
) error {

	for chunkIndex := fromChunkIndex; chunkIndex < toChunkIndex; chunkIndex++ {
		select {
		case e.pendingChunks <- &pendingChunk{chunkIndex: chunkIndex, zero: true}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (e *snapshotExporter) scheduleDataChunk(
	ctx context.Context,
	chunkIndex uint32,
) error {

	select {
	case e.readSlots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	pending := &pendingChunk{
		chunkIndex: chunkIndex,
		ready:      make(chan struct{}),
	}

	e.errGroup.Go(func() error {
		defer func() { <-e.readSlots }()
		defer close(pending.ready)

		chunk := dataplane_common.Chunk{
			Index: chunkIndex,
			Data:  make([]byte, dataplane_common.ChunkSize),
		}
		err := e.source.Read(ctx, &chunk)
		if err != nil {
			pending.err = err
			return err
		}

		pending.zero = chunk.Zero
		pending.data = chunk.Data
		return nil
	})

	select {
	case e.pendingChunks <- pending:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (e *snapshotExporter) writeChunks(
	ctx context.Context,
	dst io.Writer,
) (Stats, error) {

	var dataChunkCount, zeroChunkCount uint32
	var processedChunkCount uint32
	zeroes := make([]byte, dataplane_common.ChunkSize)

	for pending := range e.pendingChunks {
		if pending.ready != nil {
			select {
			case <-pending.ready:
			case <-ctx.Done():
				return Stats{}, ctx.Err()
			}
			if pending.err != nil {
				return Stats{}, pending.err
			}
		}

		data := pending.data
		if pending.zero {
			data = zeroes
			zeroChunkCount++
		} else {
			dataChunkCount++
		}

		err := writeStreamChunk(dst, data, pending.chunkIndex, e.meta.Size)
		if err != nil {
			return Stats{}, err
		}

		processedChunkCount++
		logExportProgress(ctx, processedChunkCount, e.meta.ChunkCount)

		if pending.ready != nil {
			select {
			case e.processedChunkIndices <- pending.chunkIndex:
			case <-ctx.Done():
				return Stats{}, ctx.Err()
			}
		}
	}

	return Stats{
		Size:           e.meta.Size,
		DataChunkCount: dataChunkCount,
		ZeroChunkCount: zeroChunkCount,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

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

func writeStreamChunk(
	dst io.Writer,
	data []byte,
	chunkIndex uint32,
	size uint64,
) error {

	offset := uint64(chunkIndex) * dataplane_common.ChunkSize
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
