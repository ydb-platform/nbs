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
	ChunkCount(ctx context.Context) (uint32, error)
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
