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
	milestone := Milestone{Value: 10, ProcessedValueCount: 3}
	inflightLimit := 10
	processedValues := make(chan uint32, inflightLimit)

	queue := NewInflightQueue(
		milestone,
		processedValues,
		ChannelWithCancellation{}, // holeValues
		inflightLimit,
	)

	addToQueue := func(ctx context.Context, value uint32) {
		added, err := queue.Add(ctx, value)
		require.NoError(t, err)
		require.True(t, added)
	}

	expectedProcessedValueCount := uint32(3)
	sendProcessedValue := func(value uint32) {
		processedValues <- value
		expectedProcessedValueCount++
		// Wait for the queue to handle processed value
		for queue.Milestone().ProcessedValueCount != expectedProcessedValueCount {
			time.Sleep(100 * time.Millisecond)
		}
	}

	require.Equal(t, Milestone{Value: 10, ProcessedValueCount: expectedProcessedValueCount}, queue.Milestone())

	addToQueue(ctx, 10)
	require.Equal(t, Milestone{Value: 10, ProcessedValueCount: expectedProcessedValueCount}, queue.Milestone())
	sendProcessedValue(10)
	require.Equal(t, Milestone{Value: 11, ProcessedValueCount: expectedProcessedValueCount}, queue.Milestone())

	addToQueue(ctx, 13)
	addToQueue(ctx, 15)
	addToQueue(ctx, 16)
	sendProcessedValue(13)
	require.Equal(t, Milestone{Value: 15, ProcessedValueCount: expectedProcessedValueCount}, queue.Milestone())
	sendProcessedValue(15)
	require.Equal(t, Milestone{Value: 16, ProcessedValueCount: expectedProcessedValueCount}, queue.Milestone())
	sendProcessedValue(16)
	require.Equal(t, Milestone{Value: 17, ProcessedValueCount: expectedProcessedValueCount}, queue.Milestone())

	queue.UpdateMilestoneHint(20)
	require.Equal(t, Milestone{Value: 20, ProcessedValueCount: expectedProcessedValueCount}, queue.Milestone())

	addToQueue(ctx, 22)
	addToQueue(ctx, 25)
	queue.UpdateMilestoneHint(30)

	sendProcessedValue(22)
	require.Equal(t, Milestone{Value: 25, ProcessedValueCount: expectedProcessedValueCount}, queue.Milestone())
	sendProcessedValue(25)
	require.Equal(t, Milestone{Value: 30, ProcessedValueCount: expectedProcessedValueCount}, queue.Milestone())

	// Milestone value should not decrease
	queue.UpdateMilestoneHint(0)
	require.Equal(t, Milestone{Value: 30, ProcessedValueCount: expectedProcessedValueCount}, queue.Milestone())
}
