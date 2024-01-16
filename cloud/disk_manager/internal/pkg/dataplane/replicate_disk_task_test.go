package dataplane

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestCheckReplicateProgress(t *testing.T) {
	ctx := newContext()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("TestCheckReplicateProgress_Task")

	task := replicateDiskTask{
		config: new(config.DataplaneConfig),
		state:  new(protos.ReplicateDiskTaskState),
	}

	uselessItersBeforeAbort := uint32(5)
	task.config.UselessReplicationIterationsBeforeAbort = &uselessItersBeforeAbort

	require.EqualValues(t, 0, task.state.MeasuredSecondsRemaining)
	require.EqualValues(t, 0, task.state.Iteration)

	require.NoError(t, task.checkReplicationProgress(ctx, execCtx))
	require.EqualValues(t, math.MaxInt64, task.state.MeasuredSecondsRemaining)

	task.state.Iteration = 1
	task.state.MeasuredSecondsRemaining = 0
	require.NoError(t, task.checkReplicationProgress(ctx, execCtx))
	require.EqualValues(t, math.MaxInt64, task.state.MeasuredSecondsRemaining)

	task.state.SecondsRemaining = 10

	require.NoError(t, task.checkReplicationProgress(ctx, execCtx))
	require.EqualValues(t, math.MaxInt64, task.state.MeasuredSecondsRemaining)

	task.state.Iteration = uselessItersBeforeAbort
	require.NoError(t, task.checkReplicationProgress(ctx, execCtx))
	require.EqualValues(t, 10, task.state.MeasuredSecondsRemaining)

	task.state.Iteration = 2 * uselessItersBeforeAbort
	task.state.SecondsRemaining = 11
	require.Error(t, task.checkReplicationProgress(ctx, execCtx))

	uselessItersBeforeAbort = 0
	task.state.SecondsRemaining = 12
	require.NoError(t, task.checkReplicationProgress(ctx, execCtx))
}
