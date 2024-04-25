package common

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestInflightQueueAdd(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	milestone := Milestone{}
	inflightLimit := 10
	processedValues := make(chan uint32, inflightLimit)

	queue := NewInflightQueue(
		milestone,
		processedValues,
		ChannelWithCancellation{}, // holeValues
		inflightLimit,
	)

	var value uint32
	var values []uint32

	for i := 0; i < inflightLimit; i++ {
		value := uint32(i * 11)
		values = append(values, value)

		ok, err := queue.Add(ctx, value)
		require.NoError(t, err)
		require.True(t, ok)
	}

	go func() {
		for _, value := range values {
			processedValues <- value
		}
	}()

	for i := 0; i < inflightLimit; i++ {
		value += uint32(i * 11)

		ok, err := queue.Add(ctx, value)
		require.NoError(t, err)
		require.True(t, ok)
	}

	go func() {
		cancel()
	}()

	_, err := queue.Add(ctx, value+1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "context canceled")
}

func TestInflightQueueMilestone(t *testing.T) {
	ctx := newContext()
	milestone := Milestone{Value: 10, ProcessedValueCount: 100}
	inflightLimit := 10
	processedValues := make(chan uint32, inflightLimit)

	queue := NewInflightQueue(
		milestone,
		processedValues,
		ChannelWithCancellation{}, // holeValues
		inflightLimit,
	)

	require.Equal(t, queue.Milestone(), Milestone{Value: 10, ProcessedValueCount: 100})

	queue.Add(ctx, 10)
	require.Equal(t, queue.Milestone(), Milestone{Value: 10, ProcessedValueCount: 100})
	processedValues <- 10
	require.Equal(t, queue.Milestone(), Milestone{Value: 11, ProcessedValueCount: 101})

	queue.Add(ctx, 13)
	queue.Add(ctx, 15)
	queue.Add(ctx, 16)
	processedValues <- 13
	require.Equal(t, queue.Milestone(), Milestone{Value: 15, ProcessedValueCount: 102})
	processedValues <- 15
	require.Equal(t, queue.Milestone(), Milestone{Value: 16, ProcessedValueCount: 103})
	processedValues <- 16
	require.Equal(t, queue.Milestone(), Milestone{Value: 17, ProcessedValueCount: 103})

	queue.UpdateDefaultMilestoneValue(20)
	require.Equal(t, queue.Milestone(), Milestone{Value: 20, ProcessedValueCount: 103})

	queue.Add(ctx, 22)
	queue.Add(ctx, 25)
	queue.UpdateDefaultMilestoneValue(30)
	processedValues <- 22
	require.Equal(t, queue.Milestone(), Milestone{Value: 25, ProcessedValueCount: 104})
	processedValues <- 25
	require.Equal(t, queue.Milestone(), Milestone{Value: 30, ProcessedValueCount: 105})
}
