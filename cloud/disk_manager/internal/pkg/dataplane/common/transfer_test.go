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
	holeChunkIndices common.ChannelWithCancellation[uint32],
) (<-chan uint32, common.ChannelWithCancellation[uint32], <-chan error) {

	args := m.Called(ctx, milestone, processedChunkIndices, holeChunkIndices)
	return args.Get(0).(chan uint32), args.Get(1).(common.ChannelWithCancellation[uint32]), args.Get(2).(chan error)
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
	holeChunkIndices common.ChannelWithCancellation[uint32],
) (<-chan uint32, common.ChannelWithCancellation[uint32], <-chan error) {

	args := m.Called(ctx, milestone, processedChunkIndices, holeChunkIndices)
	return args.Get(0).(chan uint32), args.Get(1).(common.ChannelWithCancellation[uint32]), args.Get(2).(chan error)
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
	duplicatedChunkIndices := common.NewChannelWithCancellation[uint32](100500)
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
	duplicatedChunkIndices := common.NewChannelWithCancellation[uint32](100500)
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
		common.ChannelWithCancellation[uint32]{},
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
	duplicatedChunkIndices := common.NewChannelWithCancellation[uint32](100500)
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
		common.ChannelWithCancellation[uint32]{},
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
	duplicatedChunkIndices := common.NewChannelWithCancellation[uint32](100500)
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
		common.ChannelWithCancellation[uint32]{},
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
