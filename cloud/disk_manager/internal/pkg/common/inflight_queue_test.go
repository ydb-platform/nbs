package common

import (
	"context"
	"testing"
	"time"

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

	expectedProcessedValueCount := uint32(100)
	sendProcessedValue := func(value uint32) {
		processedValues <- value
		expectedProcessedValueCount++
		// Wait for the queue to handle processed value
		for queue.Milestone().ProcessedValueCount != expectedProcessedValueCount {
			time.Sleep(100 * time.Millisecond)
		}
	}

	require.Equal(t, queue.Milestone().Value, 10)

	queue.Add(ctx, 10)
	require.Equal(t, queue.Milestone().Value, 10)
	sendProcessedValue(10)
	require.Equal(t, queue.Milestone().Value, 11)

	queue.Add(ctx, 13)
	queue.Add(ctx, 15)
	queue.Add(ctx, 16)
	sendProcessedValue(13)
	require.Equal(t, queue.Milestone().Value, 15)
	sendProcessedValue(15)
	require.Equal(t, queue.Milestone().Value, 16)
	sendProcessedValue(16)
	require.Equal(t, queue.Milestone().Value, 17)

	queue.UpdateDefaultMilestoneValue(20)
	require.Equal(t, queue.Milestone().Value, 20)

	queue.Add(ctx, 22)
	queue.Add(ctx, 25)
	queue.UpdateDefaultMilestoneValue(30)

	sendProcessedValue(22)
	require.Equal(t, queue.Milestone().Value, 25)
	sendProcessedValue(25)
	require.Equal(t, queue.Milestone().Value, 30)
}
