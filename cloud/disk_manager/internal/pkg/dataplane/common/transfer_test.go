package common

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type sourceMock struct {
	mock.Mock
	// Use mutex for reads, because mock.Mock is not thread-safe.
	readMutex sync.Mutex
}

func (m *sourceMock) ChunkIndices(
	ctx context.Context,
	milestone Milestone,
	processedChunkIndices <-chan uint32,
	holeChunkIndices common.ChannelWithCancellation,
) (<-chan uint32, common.ChannelWithCancellation, <-chan error) {

	args := m.Called(ctx, milestone, processedChunkIndices, holeChunkIndices)
	return args.Get(0).(chan uint32), args.Get(1).(common.ChannelWithCancellation), args.Get(2).(chan error)
}

func (m *sourceMock) Read(ctx context.Context, chunk *Chunk) error {
	m.readMutex.Lock()
	defer m.readMutex.Unlock()

	args := m.Called(ctx, chunk)
	return args.Error(0)
}

func (m *sourceMock) Milestone() Milestone {
	args := m.Called()
	return args.Get(0).(Milestone)
}

func (m *sourceMock) ChunkCount(ctx context.Context) (uint32, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint32), args.Error(1)
}

func (m *sourceMock) EstimatedBytesToRead(ctx context.Context) (uint64, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *sourceMock) Close(ctx context.Context) {
	m.Called(ctx)
}

////////////////////////////////////////////////////////////////////////////////

type shallowSourceMock struct {
	mock.Mock
	// Use mutex for shallow copying, because mock.Mock is not thread-safe.
	shallowCopyMutex sync.Mutex
}

func (m *shallowSourceMock) ChunkIndices(
	ctx context.Context,
	milestone Milestone,
	processedChunkIndices <-chan uint32,
	holeChunkIndices common.ChannelWithCancellation,
) (<-chan uint32, common.ChannelWithCancellation, <-chan error) {

	args := m.Called(ctx, milestone, processedChunkIndices, holeChunkIndices)
	return args.Get(0).(chan uint32), args.Get(1).(common.ChannelWithCancellation), args.Get(2).(chan error)
}

func (m *shallowSourceMock) ShallowCopy(
	ctx context.Context,
	chunkIndex uint32,
) error {

	m.shallowCopyMutex.Lock()
	defer m.shallowCopyMutex.Unlock()

	args := m.Called(ctx, chunkIndex)
	return args.Error(0)
}

func (m *shallowSourceMock) Milestone() Milestone {
	args := m.Called()
	return args.Get(0).(Milestone)
}

func (m *shallowSourceMock) ChunkCount(ctx context.Context) (uint32, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint32), args.Error(1)
}

func (m *shallowSourceMock) Close(ctx context.Context) {
	m.Called(ctx)
}

////////////////////////////////////////////////////////////////////////////////

type targetMock struct {
	mock.Mock
	// Use mutex for writes, because mock.Mock is not thread-safe.
	writeMutex sync.Mutex
}

func (m *targetMock) Write(ctx context.Context, chunk Chunk) error {
	m.writeMutex.Lock()
	defer m.writeMutex.Unlock()

	args := m.Called(ctx, chunk)
	return args.Error(0)
}

func (m *targetMock) Close(ctx context.Context) {
	m.Called(ctx)
}

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestTransfer(t *testing.T) {
	ctx := newContext()
	source := &sourceMock{}
	target := &targetMock{}

	chunkSize := 16
	chunks := make([]Chunk, 0)

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 101; i++ {
		chunk := Chunk{}
		chunk.Index = uint32(i * 17)
		chunk.Data = make([]byte, chunkSize)
		rand.Read(chunk.Data)
		chunks = append(chunks, chunk)
	}

	milestone := Milestone{ChunkIndex: 42}
	chunkIndices := make(chan uint32, 100500)
	duplicatedChunkIndices := common.NewChannelWithCancellation(100500)
	chunkIndicesErrors := make(chan error)

	source.On("Milestone").Return(milestone)
	source.On("ChunkIndices", mock.Anything, milestone, mock.Anything, mock.Anything).Return(
		chunkIndices,
		duplicatedChunkIndices,
		chunkIndicesErrors,
	).Run(func(args mock.Arguments) {
		go func() {
			for _, chunk := range chunks {
				chunkIndices <- chunk.Index
			}

			chunkIndicesErrors <- nil
			close(chunkIndices)
			close(chunkIndicesErrors)
		}()
	})

	source.On("Read", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		chunk := args.Get(1).(*Chunk)

		var found bool
		for _, c := range chunks {
			if c.Index == chunk.Index {
				found = true
				copy(chunk.Data, c.Data)
			}
		}
		if !found {
			panic(fmt.Sprintf("chunk %v is not found", chunk.Index))
		}
	}).Times(len(chunks))

	for _, chunk := range chunks {
		target.On("Write", mock.Anything, chunk).Return(nil)
	}

	transferer := Transferer{
		ReaderCount:         uint32(11),
		WriterCount:         uint32(22),
		ChunksInflightLimit: uint32(100500),
		ChunkSize:           chunkSize,
	}

	transferredChunkCount, err := transferer.Transfer(
		ctx,
		source,
		target,
		Milestone{ChunkIndex: milestone.ChunkIndex},
		func(context.Context, Milestone) error { return nil },
	)
	require.NoError(t, err)
	require.Equal(t, len(chunks), int(transferredChunkCount))
}

func TestTransferWithShallowCopy(t *testing.T) {
	ctx := newContext()
	source := &sourceMock{}
	shallowSource := &shallowSourceMock{}
	target := &targetMock{}

	chunkSize := 16
	chunks := make([]Chunk, 0)

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 101; i++ {
		chunk := Chunk{}
		chunk.Index = uint32(i * 3)
		chunk.Data = make([]byte, chunkSize)
		rand.Read(chunk.Data)
		chunks = append(chunks, chunk)
	}

	shallowCopyChunks := make([]Chunk, 0)
	for i := 0; i < 101; i++ {
		chunk := Chunk{}
		chunk.Index = uint32(i * 9)
		shallowCopyChunks = append(shallowCopyChunks, chunk)
	}

	milestone := Milestone{ChunkIndex: 42}
	chunkIndices := make(chan uint32, 100500)
	duplicatedChunkIndices := common.NewChannelWithCancellation(100500)
	chunkIndicesErrors := make(chan error)

	source.On("Milestone").Return(milestone)
	source.On("ChunkIndices", mock.Anything, milestone, mock.Anything, mock.Anything).Return(
		chunkIndices,
		duplicatedChunkIndices,
		chunkIndicesErrors,
	).Run(func(args mock.Arguments) {
		go func() {
			for _, chunk := range chunks {
				chunkIndices <- chunk.Index
				_, _ = duplicatedChunkIndices.Send(
					context.Background(),
					chunk.Index,
				)
			}

			chunkIndicesErrors <- nil
			close(chunkIndices)
			duplicatedChunkIndices.Close()
			close(chunkIndicesErrors)
		}()
	})

	shallowCopyChunkIndices := make(chan uint32, 100500)
	shallowCopyChunkIndicesErrors := make(chan error)

	shallowSource.On("Milestone").Return(milestone)
	shallowSource.On("ChunkIndices", mock.Anything, milestone, mock.Anything, mock.Anything).Return(
		shallowCopyChunkIndices,
		common.ChannelWithCancellation{},
		shallowCopyChunkIndicesErrors,
	).Run(func(args mock.Arguments) {
		go func() {
			for _, chunk := range shallowCopyChunks {
				shallowCopyChunkIndices <- chunk.Index
			}

			shallowCopyChunkIndicesErrors <- nil
			close(shallowCopyChunkIndices)
			close(shallowCopyChunkIndicesErrors)
		}()
	})

	source.On("Read", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		chunk := args.Get(1).(*Chunk)

		var found bool
		for _, c := range chunks {
			if c.Index == chunk.Index {
				found = true
				copy(chunk.Data, c.Data)
			}
		}
		if !found {
			panic(fmt.Sprintf("chunk %v is not found", chunk.Index))
		}
	}).Times(len(chunks))

	shallowSource.On("ShallowCopy", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		chunkIndex := args.Get(1).(uint32)

		var found bool
		for _, c := range shallowCopyChunks {
			if c.Index == chunkIndex {
				found = true
			}
		}
		if !found {
			panic(fmt.Sprintf("chunk %v is not found", chunkIndex))
		}
	}).Times(len(shallowCopyChunks))

	for _, chunk := range chunks {
		target.On("Write", mock.Anything, chunk).Return(nil)
	}

	transferer := Transferer{
		ReaderCount:         uint32(11),
		WriterCount:         uint32(22),
		ChunksInflightLimit: uint32(100500),
		ChunkSize:           chunkSize,

		ShallowCopyWorkerCount:   uint32(22),
		ShallowCopyInflightLimit: uint32(100500),
		ShallowSource:            shallowSource,
	}

	transferredChunkCount, err := transferer.Transfer(
		ctx,
		source,
		target,
		milestone,
		func(context.Context, Milestone) error { return nil },
	)
	require.NoError(t, err)
	require.Equal(t, len(chunks), int(transferredChunkCount))
}

func TestTransferShouldUpdateMilestoneWhenShallowCopyIsDoneSuccessfully(t *testing.T) {
	ctx := newContext()
	source := &sourceMock{}
	shallowSource := &shallowSourceMock{}
	target := &targetMock{}

	chunkSize := 16
	milestone := Milestone{ChunkIndex: 42}
	chunkIndices := make(chan uint32, 100500)
	duplicatedChunkIndices := common.NewChannelWithCancellation(100500)
	chunkIndicesErrors := make(chan error)

	source.On("Milestone").Return(milestone)
	source.On("ChunkIndices", mock.Anything, milestone, mock.Anything, mock.Anything).Return(
		chunkIndices,
		duplicatedChunkIndices,
		chunkIndicesErrors,
	).Run(func(args mock.Arguments) {
		go func() {
			chunkIndicesErrors <- nil
			close(chunkIndices)
			duplicatedChunkIndices.Close()
			close(chunkIndicesErrors)
		}()
	})

	shallowCopyMilestone := Milestone{ChunkIndex: 1}
	shallowCopyChunkIndices := make(chan uint32, 100500)
	shallowCopyChunkIndicesErrors := make(chan error)

	shallowSource.On("Milestone").Return(shallowCopyMilestone)
	shallowSource.On("ChunkIndices", mock.Anything, milestone, mock.Anything, mock.Anything).Return(
		shallowCopyChunkIndices,
		common.ChannelWithCancellation{},
		shallowCopyChunkIndicesErrors,
	).Run(func(args mock.Arguments) {
		go func() {
			// Nothing to copy, so finish early.
			shallowCopyChunkIndicesErrors <- nil
			close(shallowCopyChunkIndices)
			close(shallowCopyChunkIndicesErrors)
		}()
	})

	transferer := Transferer{
		ReaderCount:         uint32(11),
		WriterCount:         uint32(22),
		ChunksInflightLimit: uint32(100500),
		ChunkSize:           chunkSize,

		ShallowCopyWorkerCount:   uint32(22),
		ShallowCopyInflightLimit: uint32(100500),
		ShallowSource:            shallowSource,
	}

	var actualMilestone Milestone

	_, err := transferer.Transfer(
		ctx,
		source,
		target,
		milestone,
		func(ctx context.Context, milestone Milestone) error {
			actualMilestone = milestone
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, milestone, actualMilestone)
}

func TestTransferShouldNotUpdateMilestoneWhenShallowCopyIsFailed(t *testing.T) {
	ctx := newContext()
	source := &sourceMock{}
	shallowSource := &shallowSourceMock{}
	target := &targetMock{}

	chunkSize := 16
	milestone := Milestone{ChunkIndex: 42}
	chunkIndices := make(chan uint32, 100500)
	duplicatedChunkIndices := common.NewChannelWithCancellation(100500)
	chunkIndicesErrors := make(chan error)

	source.On("Milestone").Return(milestone)
	source.On("ChunkIndices", mock.Anything, milestone, mock.Anything, mock.Anything).Return(
		chunkIndices,
		duplicatedChunkIndices,
		chunkIndicesErrors,
	).Run(func(args mock.Arguments) {
		go func() {
			chunkIndicesErrors <- nil
			close(chunkIndices)
			duplicatedChunkIndices.Close()
			close(chunkIndicesErrors)
		}()
	})

	shallowCopyMilestone := Milestone{ChunkIndex: 1}
	shallowCopyChunkIndices := make(chan uint32, 100500)
	shallowCopyChunkIndicesErrors := make(chan error)

	shallowSource.On("Milestone").Return(shallowCopyMilestone)
	shallowSource.On("ChunkIndices", mock.Anything, milestone, mock.Anything, mock.Anything).Return(
		shallowCopyChunkIndices,
		common.ChannelWithCancellation{},
		shallowCopyChunkIndicesErrors,
	).Run(func(args mock.Arguments) {
		go func() {
			time.Sleep(time.Second)
			// We hope that saveProgress was called before this moment.

			shallowCopyChunkIndicesErrors <- assert.AnError
			close(shallowCopyChunkIndices)
			close(shallowCopyChunkIndicesErrors)
		}()
	})

	transferer := Transferer{
		ReaderCount:         uint32(11),
		WriterCount:         uint32(22),
		ChunksInflightLimit: uint32(100500),
		ChunkSize:           chunkSize,

		ShallowCopyWorkerCount:   uint32(22),
		ShallowCopyInflightLimit: uint32(100500),
		ShallowSource:            shallowSource,
	}

	var actualMilestone Milestone

	_, err := transferer.Transfer(
		ctx,
		source,
		target,
		milestone,
		func(ctx context.Context, milestone Milestone) error {
			actualMilestone = milestone
			return nil
		},
	)
	require.Equal(t, assert.AnError, err)
	require.LessOrEqual(
		t,
		actualMilestone.ChunkIndex,
		shallowCopyMilestone.ChunkIndex,
	)
}

////////////////////////////////////////////////////////////////////////////////

// emitChunkIndices returns the channels for a sourceMock.ChunkIndices stub that
// emits the contiguous indices [0, chunkCount).
func emitChunkIndices(chunkCount uint32) (chan uint32, common.ChannelWithCancellation, chan error) {
	chunkIndices := make(chan uint32, 100500)
	duplicatedChunkIndices := common.NewChannelWithCancellation(100500)
	chunkIndicesErrors := make(chan error, 1)

	go func() {
		for i := uint32(0); i < chunkCount; i++ {
			chunkIndices <- i
		}

		chunkIndicesErrors <- nil
		close(chunkIndices)
		close(chunkIndicesErrors)
	}()

	return chunkIndices, duplicatedChunkIndices, chunkIndicesErrors
}

func TestTransferBatched(t *testing.T) {
	ctx := newContext()
	source := &sourceMock{}
	target := &targetMock{}

	chunkSize := 16
	chunkCount := uint32(101)

	chunkIndices, duplicatedChunkIndices, chunkIndicesErrors := emitChunkIndices(chunkCount)

	source.On("ChunkCount", mock.Anything).Return(chunkCount, nil)
	source.On("ChunkIndices", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		chunkIndices,
		duplicatedChunkIndices,
		chunkIndicesErrors,
	)
	source.On("Read", mock.Anything, mock.Anything).Return(nil)
	target.On("Write", mock.Anything, mock.Anything).Return(nil)

	transferer := Transferer{
		ReaderCount:         uint32(11),
		WriterCount:         uint32(22),
		ChunksInflightLimit: uint32(100500),
		ChunkSize:           chunkSize,
		BatchSize:           uint32(10),
	}

	transferredChunkCount, err := transferer.Transfer(
		ctx,
		source,
		target,
		Milestone{},
		func(context.Context, Milestone) error { return nil },
	)
	require.NoError(t, err)
	require.Equal(t, int(chunkCount), int(transferredChunkCount))
}

// When some chunk writes fail, the batch is left incomplete, the bitmap is
// persisted, and a rescheduled transfer redoes only the not-yet-written chunks.
func TestTransferBatchedResumesOnlyUnwrittenChunks(t *testing.T) {
	ctx := newContext()

	chunkSize := 16
	chunkCount := uint32(100)

	transferer := Transferer{
		ReaderCount:         uint32(4),
		WriterCount:         uint32(4),
		ChunksInflightLimit: uint32(100500),
		ChunkSize:           chunkSize,
		BatchSize:           uint32(128), // one batch covers all chunks
	}

	// First run: odd-index writes fail (retriable), even-index writes succeed.
	source1 := &sourceMock{}
	target1 := &targetMock{}
	chunkIndices1, duplicated1, errors1 := emitChunkIndices(chunkCount)

	source1.On("ChunkCount", mock.Anything).Return(chunkCount, nil)
	source1.On("ChunkIndices", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		chunkIndices1,
		duplicated1,
		errors1,
	)
	source1.On("Read", mock.Anything, mock.Anything).Return(nil)
	target1.On("Write", mock.Anything, mock.MatchedBy(func(c Chunk) bool {
		return c.Index%2 == 0
	})).Return(nil)
	target1.On("Write", mock.Anything, mock.MatchedBy(func(c Chunk) bool {
		return c.Index%2 == 1
	})).Return(assert.AnError)

	var savedMilestone Milestone
	_, err := transferer.Transfer(
		ctx,
		source1,
		target1,
		Milestone{},
		func(_ context.Context, milestone Milestone) error {
			savedMilestone = milestone
			return nil
		},
	)
	require.Error(t, err)
	require.NotEmpty(t, savedMilestone.CurrentBatchBitmap)

	// Second run resumes with the persisted bitmap. All writes succeed now, and
	// only the previously-failed (odd) chunks must be written again.
	source2 := &sourceMock{}
	target2 := &targetMock{}
	chunkIndices2, duplicated2, errors2 := emitChunkIndices(chunkCount)

	source2.On("ChunkCount", mock.Anything).Return(chunkCount, nil)
	source2.On("ChunkIndices", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		chunkIndices2,
		duplicated2,
		errors2,
	)
	source2.On("Read", mock.Anything, mock.Anything).Return(nil)

	var writeMutex sync.Mutex
	writtenIndices := make(map[uint32]bool)
	target2.On("Write", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		chunk := args.Get(1).(Chunk)
		writeMutex.Lock()
		writtenIndices[chunk.Index] = true
		writeMutex.Unlock()
	})

	transferredChunkCount, err := transferer.Transfer(
		ctx,
		source2,
		target2,
		savedMilestone,
		func(context.Context, Milestone) error { return nil },
	)
	require.NoError(t, err)
	require.Equal(t, int(chunkCount), int(transferredChunkCount))

	require.Equal(t, int(chunkCount/2), len(writtenIndices))
	for index := range writtenIndices {
		require.Equal(
			t,
			uint32(1),
			index%2,
			"only previously-failed odd chunks should be rewritten",
		)
	}
}
