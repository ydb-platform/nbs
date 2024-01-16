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
