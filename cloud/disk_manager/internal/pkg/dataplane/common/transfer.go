package common

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type Milestone struct {
	ChunkIndex            uint32
	TransferredChunkCount uint32

	// CurrentBatchBitmap is used only in batched transfer mode. It holds one bit
	// per chunk of the batch that starts at ChunkIndex; a set bit means the chunk
	// is already written. It lets a rescheduled task redo only the not-yet-written
	// chunks of the current batch. Empty in streaming mode.
	CurrentBatchBitmap []byte
}

type baseSource interface {
	// Should be called once.
	//
	// Takes processedChunkIndices channel containing indices of chunks which
	// have been transferred, it is needed for in-flight limiting and for
	// computing last valid state to restore from.
	// Takes holeChunkIndices channel containing indices of chunks (holes) that
	// should not be processed (they will be excluded from chunkIndices
	// channel). These indices should be sorted.
	//
	// Returns channel that produces indices of chunks to read. These indices
	// should be sorted.
	// Also returns optional channel (duplicatedChunkIndices), the content of
	// which is exact copy of chunkIndices channel.
	ChunkIndices(
		ctx context.Context,
		milestone Milestone,
		processedChunkIndices <-chan uint32,
		holeChunkIndices common.ChannelWithCancellation,
	) (
		chunkIndices <-chan uint32,
		duplicatedChunkIndices common.ChannelWithCancellation,
		errors <-chan error,
	)

	// Returns milestone to restore from.
	Milestone() Milestone

	Close(ctx context.Context)
}

type Source interface {
	baseSource

	Read(ctx context.Context, chunk *Chunk) error
	// Returns total chunk count in the source.
	ChunkCount(ctx context.Context) (uint32, error)
	// Returns data size to read. Might be less than ChunkCount * chunkSize.
	// Used to calculate the estimated transfer time.
	EstimatedBytesToRead(ctx context.Context) (uint64, error)
}

type ShallowSource interface {
	baseSource

	ShallowCopy(ctx context.Context, chunkIndex uint32) error
}

type Target interface {
	Write(ctx context.Context, chunk Chunk) error
	Close(ctx context.Context)
}

////////////////////////////////////////////////////////////////////////////////

type SaveProgress = func(context.Context, Milestone) error

////////////////////////////////////////////////////////////////////////////////

type Transferer struct {
	ReaderCount         uint32
	WriterCount         uint32
	ChunksInflightLimit uint32
	ChunkSize           int

	// BatchSize enables batched transfer when greater than zero: chunks are
	// processed in batches of this many chunk indices, only not-yet-written
	// chunks are (re)tried, and per-batch progress is persisted via
	// Milestone.CurrentBatchBitmap so a rescheduled task redoes only the failed
	// chunks of the current batch. Zero keeps the streaming behavior.
	// Batched mode currently applies only when ShallowSource is nil.
	BatchSize uint32

	ShallowCopyWorkerCount   uint32
	ShallowCopyInflightLimit uint32
	ShallowSource            ShallowSource
}

// Transfers data from |source| to |target|.
// Takes |saveProgress| function which is periodically called in order to
// save execution state persistently.
// Returns amount of chunks that have been transferred.
func (t Transferer) Transfer(
	ctx context.Context,
	source Source,
	target Target,
	milestone Milestone,
	saveProgress SaveProgress,
) (uint32, error) {

	if t.BatchSize > 0 && t.ShallowSource == nil {
		return t.transferBatched(ctx, source, target, milestone, saveProgress)
	}

	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	transferredChunkIndices := make(chan uint32, t.ChunksInflightLimit)
	defer close(transferredChunkIndices)

	chunkIndices, holeChunkIndices, chunkIndicesErrors := source.ChunkIndices(
		ctx,
		milestone,
		transferredChunkIndices,
		common.ChannelWithCancellation{}, // holeChunkIndices
	)

	var shallowCopyChunkIndicesErrors <-chan error
	var shallowCopierErrors <-chan error

	if t.ShallowSource != nil {
		common.Assert(!holeChunkIndices.Empty(), "should not be empty")

		copiedChunkIndices := make(chan uint32, t.ShallowCopyInflightLimit)
		defer close(copiedChunkIndices)

		shallowCopyChunkIndicesErrors, shallowCopierErrors = t.goShallowCopy(
			ctx,
			milestone,
			copiedChunkIndices,
			holeChunkIndices,
		)
	}

	chunks := make(chan *Chunk, t.ChunksInflightLimit)
	chunkPool := sync.Pool{
		New: func() interface{} {
			return &Chunk{
				Data: make([]byte, t.ChunkSize),
			}
		},
	}

	readerErrors := t.goRead(ctx, source, chunkIndices, chunks, &chunkPool)

	transferredChunkCount := milestone.TransferredChunkCount
	writerErrors := t.goWrite(
		ctx,
		target,
		chunks,
		&chunkPool,
		transferredChunkIndices,
		&transferredChunkCount,
	)

	var shallowCopyDoneSuccessfully atomic.Bool

	waitSaver, saverError := common.ProgressSaver(
		ctx,
		func(ctx context.Context) error {
			milestone := source.Milestone()

			if t.ShallowSource != nil && !shallowCopyDoneSuccessfully.Load() {
				shallowCopyMilestone := t.ShallowSource.Milestone()
				if shallowCopyMilestone.ChunkIndex < milestone.ChunkIndex {
					milestone.ChunkIndex = shallowCopyMilestone.ChunkIndex
					// TODO: milestone.TransferredChunkCount should be computed
					// properly.
				}
			}

			logging.Info(ctx, "Saving progress with milestone %+v", milestone)
			return saveProgress(ctx, milestone)
		},
	)
	defer waitSaver()

	var resultErr error
	chunkIndicesDone := false
	readersDone := uint32(0)
	writersDone := uint32(0)
	chunksClosed := false
	shallowCopyChunkIndicesDone := false
	shallowCopiersDone := uint32(0)

	for {
		var err error

		select {
		case err = <-chunkIndicesErrors:
			chunkIndicesDone = true
		case err = <-readerErrors:
			readersDone++
		case err = <-writerErrors:
			writersDone++
		case err = <-shallowCopyChunkIndicesErrors:
			shallowCopyChunkIndicesDone = true
		case err = <-shallowCopierErrors:
			shallowCopiersDone++
		}

		if err != nil {
			panicErr := &errors.PanicError{}
			if errors.As(err, &panicErr) {
				panicErr.Reraise()
			}
		}

		if resultErr == nil {
			if err == nil {
				select {
				case err = <-saverError:
				default:
				}
			}

			if err != nil {
				resultErr = err
				cancel()
			}
		}

		if chunkIndicesDone && readersDone == t.ReaderCount {
			if !chunksClosed {
				// It's safe to close chunks since we know that all readers are
				// finished their work.
				close(chunks)
				chunksClosed = true
			}

			allDone := writersDone == t.WriterCount
			if t.ShallowSource != nil {
				shallowCopyDone := shallowCopyChunkIndicesDone
				shallowCopyDone =
					shallowCopyDone && shallowCopiersDone == t.ShallowCopyWorkerCount
				if shallowCopyDone && resultErr == nil {
					shallowCopyDoneSuccessfully.Store(true)
				}

				allDone = allDone && shallowCopyDone
			}

			if allDone {
				if resultErr == nil {
					resultErr = waitSaver()
				}

				if resultErr != nil {
					return 0, resultErr
				}

				return transferredChunkCount, nil
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t Transferer) goShallowCopy(
	ctx context.Context,
	milestone Milestone,
	copiedChunkIndices chan uint32,
	holeChunkIndices common.ChannelWithCancellation,
) (<-chan error, <-chan error) {

	chunkIndices, _, chunkIndicesErrors := t.ShallowSource.ChunkIndices(
		ctx,
		milestone,
		copiedChunkIndices,
		holeChunkIndices,
	)

	copierErrors := make(chan error, t.ShallowCopyWorkerCount)

	for i := uint32(0); i < t.ShallowCopyWorkerCount; i++ {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					copierErrors <- errors.NewPanicError(r)
				}
			}()

			for {
				select {
				case chunkIndex, more := <-chunkIndices:
					if !more {
						copierErrors <- nil
						return
					}

					err := t.ShallowSource.ShallowCopy(ctx, chunkIndex)
					if err != nil {
						copierErrors <- err
						return
					}

					select {
					case copiedChunkIndices <- chunkIndex:
					case <-ctx.Done():
						copierErrors <- ctx.Err()
						return
					}
				case <-ctx.Done():
					copierErrors <- ctx.Err()
					return
				}
			}
		}()
	}

	return chunkIndicesErrors, copierErrors
}

func (t Transferer) goRead(
	ctx context.Context,
	source Source,
	chunkIndices <-chan uint32,
	chunks chan *Chunk,
	chunkPool *sync.Pool,
) <-chan error {

	readerErrors := make(chan error, t.ReaderCount)

	for i := uint32(0); i < t.ReaderCount; i++ {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					readerErrors <- errors.NewPanicError(r)
				}
			}()

			for {
				select {
				case chunkIndex, more := <-chunkIndices:
					if !more {
						readerErrors <- nil
						return
					}

					chunk := chunkPool.Get().(*Chunk)
					chunk.Index = chunkIndex
					chunk.Zero = false

					err := source.Read(ctx, chunk)
					if err != nil {
						readerErrors <- err
						return
					}

					if !chunk.Zero {
						chunk.Zero = chunk.CheckDataIsAllZeroes()
					}

					select {
					case chunks <- chunk:
					case <-ctx.Done():
						readerErrors <- ctx.Err()
						return
					}
				case <-ctx.Done():
					readerErrors <- ctx.Err()
					return
				}
			}
		}()
	}

	return readerErrors
}

func (t Transferer) goWrite(
	ctx context.Context,
	target Target,
	chunks <-chan *Chunk,
	chunkPool *sync.Pool,
	transferredChunkIndices chan uint32,
	transferredChunkCount *uint32,
) <-chan error {

	writerErrors := make(chan error, t.WriterCount)

	for i := uint32(0); i < t.WriterCount; i++ {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					writerErrors <- errors.NewPanicError(r)
				}
			}()

			for {
				select {
				case chunk, more := <-chunks:
					if !more {
						writerErrors <- nil
						return
					}

					chunkIndex := chunk.Index

					err := target.Write(ctx, *chunk)
					chunkPool.Put(chunk)
					if err != nil {
						writerErrors <- err
						return
					}

					atomic.AddUint32(transferredChunkCount, 1)

					select {
					case transferredChunkIndices <- chunkIndex:
					case <-ctx.Done():
						writerErrors <- ctx.Err()
						return
					}
				case <-ctx.Done():
					writerErrors <- ctx.Err()
					return
				}
			}
		}()
	}

	return writerErrors
}

////////////////////////////////////////////////////////////////////////////////
// Batched transfer.
//
// Streaming Transfer cancels the whole worker pool on the first write error and
// restarts from a coarse milestone, so a degraded target (for example S3 that
// serves only a fraction of requests) makes almost no progress and amplifies
// errors. Batched mode instead processes chunks in fixed windows of BatchSize
// chunk indices. Within a window it writes every not-yet-written chunk once and
// records the successes in a bitmap, then:
//   - advances to the next window once the whole window is written;
//   - on a non-retriable error, fails immediately (as before);
//   - otherwise, if some chunks are still unwritten, persists the bitmap and
//     returns a retriable error. The task framework reschedules the task, which
//     resumes the same window and redoes only the not-yet-written chunks.
// Already-written chunks are never redone, so the transfer makes monotonic
// progress even against a heavily degraded target.

func (t Transferer) transferBatched(
	ctx context.Context,
	source Source,
	target Target,
	milestone Milestone,
	saveProgress SaveProgress,
) (uint32, error) {

	chunkCount, err := source.ChunkCount(ctx)
	if err != nil {
		return 0, err
	}

	processedChunkIndices := make(chan uint32, t.ChunksInflightLimit)
	defer close(processedChunkIndices)

	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	chunkIndices, _, chunkIndicesErrors := source.ChunkIndices(
		ctx,
		milestone,
		processedChunkIndices,
		common.ChannelWithCancellation{}, // holeChunkIndices
	)

	chunkPool := &sync.Pool{
		New: func() interface{} {
			return &Chunk{Data: make([]byte, t.ChunkSize)}
		},
	}

	transferredChunkCount := milestone.TransferredChunkCount
	batchStart := milestone.ChunkIndex
	bitmap := newBatchBitmap(t.BatchSize, milestone.CurrentBatchBitmap)

	// pending holds the indices of the current window that still need writing.
	var pending []uint32

	// writeCurrentBatch runs one pass over pending, updating the bitmap and
	// transferredChunkCount. It returns whether the whole window is written.
	writeCurrentBatch := func() (bool, error) {
		if len(pending) == 0 {
			return true, nil
		}
		return t.runBatchPass(
			ctx,
			source,
			target,
			chunkPool,
			pending,
			batchStart,
			bitmap,
			&transferredChunkCount,
		)
	}

	rescheduleForUnwritten := func() error {
		if err := saveProgress(ctx, Milestone{
			ChunkIndex:            batchStart,
			TransferredChunkCount: transferredChunkCount,
			CurrentBatchBitmap:    bitmap.clone(),
		}); err != nil {
			return err
		}

		logging.Warn(
			ctx,
			"Batch at chunk %v is not fully written, rescheduling",
			batchStart,
		)
		return errors.NewRetriableErrorf(
			"batch at chunk %v is not fully written",
			batchStart,
		)
	}

	// finishCurrentBatch processes the current window and, on success, advances
	// to the next one.
	finishCurrentBatch := func() error {
		hadWork := len(pending) > 0

		done, err := writeCurrentBatch()
		if err != nil {
			return err
		}
		if !done {
			return rescheduleForUnwritten()
		}

		batchStart += t.BatchSize
		bitmap.reset()
		pending = pending[:0]

		if !hadWork {
			// Empty window (for example a gap between allocated regions): nothing
			// was written, so skip the progress save. A crash just re-scans this
			// range, which writes nothing.
			return nil
		}
		return saveProgress(ctx, Milestone{
			ChunkIndex:            batchStart,
			TransferredChunkCount: transferredChunkCount,
		})
	}

	indicesDrained := false
	generationChecked := false
	for !indicesDrained || !generationChecked {
		select {
		case chunkIndex, more := <-chunkIndices:
			if !more {
				indicesDrained = true
				chunkIndices = nil
				continue
			}

			for chunkIndex >= batchStart+t.BatchSize {
				if err := finishCurrentBatch(); err != nil {
					return 0, err
				}
			}

			if !bitmap.isSet(chunkIndex - batchStart) {
				pending = append(pending, chunkIndex)
			}

			// Keep the source generator unblocked: it limits the number of
			// in-flight indices via this channel.
			select {
			case processedChunkIndices <- chunkIndex:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		case err := <-chunkIndicesErrors:
			if err != nil {
				return 0, err
			}
			generationChecked = true
			chunkIndicesErrors = nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	// Process the last (partial) window.
	done, err := writeCurrentBatch()
	if err != nil {
		return 0, err
	}
	if !done {
		return 0, rescheduleForUnwritten()
	}

	if err := saveProgress(ctx, Milestone{
		ChunkIndex:            chunkCount,
		TransferredChunkCount: transferredChunkCount,
	}); err != nil {
		return 0, err
	}

	return transferredChunkCount, nil
}

// runBatchPass writes every index in pending once, using WriterCount workers.
// Successful writes are recorded in bitmap and counted in transferredChunkCount.
// It returns whether all pending indices are now written. A non-retriable error
// stops the pass and is returned; retriable failures just leave the chunk
// unwritten (to be retried after reschedule).
func (t Transferer) runBatchPass(
	ctx context.Context,
	source Source,
	target Target,
	chunkPool *sync.Pool,
	pending []uint32,
	batchStart uint32,
	bitmap *batchBitmap,
	transferredChunkCount *uint32,
) (bool, error) {

	passCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	indices := make(chan uint32)

	var mutex sync.Mutex
	var nonRetriableErr error
	var panicValue interface{}

	handleErr := func(err error) bool {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {

			return true
		}
		if isNonRetriableError(err) {
			mutex.Lock()
			if nonRetriableErr == nil {
				nonRetriableErr = err
			}
			mutex.Unlock()
			cancel()
			return true
		}
		// Retriable: leave the chunk unwritten, keep going.
		return false
	}

	workerCount := t.WriterCount
	if workerCount == 0 {
		workerCount = 1
	}

	var wg sync.WaitGroup
	for i := uint32(0); i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					mutex.Lock()
					if panicValue == nil {
						panicValue = r
					}
					mutex.Unlock()
					cancel()
				}
			}()

			chunk := chunkPool.Get().(*Chunk)
			defer chunkPool.Put(chunk)

			for chunkIndex := range indices {
				if passCtx.Err() != nil {
					return
				}

				chunk.Index = chunkIndex
				chunk.Zero = false

				if err := source.Read(passCtx, chunk); err != nil {
					if handleErr(err) {
						return
					}
					continue
				}

				if !chunk.Zero {
					chunk.Zero = chunk.CheckDataIsAllZeroes()
				}

				if err := target.Write(passCtx, *chunk); err != nil {
					if handleErr(err) {
						return
					}
					continue
				}

				mutex.Lock()
				bitmap.set(chunkIndex - batchStart)
				mutex.Unlock()
				atomic.AddUint32(transferredChunkCount, 1)
			}
		}()
	}

	for _, chunkIndex := range pending {
		select {
		case indices <- chunkIndex:
		case <-passCtx.Done():
		}
	}
	close(indices)
	wg.Wait()

	if panicValue != nil {
		errors.NewPanicError(panicValue).Reraise()
	}
	if nonRetriableErr != nil {
		return false, nonRetriableErr
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}

	for _, chunkIndex := range pending {
		if !bitmap.isSet(chunkIndex - batchStart) {
			return false, nil
		}
	}
	return true, nil
}

func isNonRetriableError(err error) bool {
	return errors.Is(err, errors.NewEmptyNonRetriableError())
}

////////////////////////////////////////////////////////////////////////////////

// batchBitmap tracks which chunks of a batch have been written, one bit each.
type batchBitmap struct {
	bits []byte
}

func newBatchBitmap(batchSize uint32, data []byte) *batchBitmap {
	size := int((batchSize + 7) / 8)
	bits := make([]byte, size)
	// Reuse the persisted bits only if they match the current batch size,
	// otherwise start clean and redo the whole batch (writes are idempotent).
	if len(data) == size {
		copy(bits, data)
	}
	return &batchBitmap{bits: bits}
}

func (b *batchBitmap) set(index uint32) {
	b.bits[index/8] |= 1 << (index % 8)
}

func (b *batchBitmap) isSet(index uint32) bool {
	return b.bits[index/8]&(1<<(index%8)) != 0
}

func (b *batchBitmap) reset() {
	for i := range b.bits {
		b.bits[i] = 0
	}
}

func (b *batchBitmap) clone() []byte {
	out := make([]byte, len(b.bits))
	copy(out, b.bits)
	return out
}
