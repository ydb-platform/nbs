// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package processorhelper

import (
    "context"
    "errors"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"

    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/component/componenttest"
    "go.opentelemetry.io/collector/consumer"
    "go.opentelemetry.io/collector/consumer/consumertest"
    "go.opentelemetry.io/collector/pdata/ptrace"
    "go.opentelemetry.io/collector/processor/processortest"
)

var testTracesCfg = struct{}{}

func TestNewTracesProcessor(t *testing.T) {
    tp, err := NewTracesProcessor(context.Background(), processortest.NewNopCreateSettings(), &testTracesCfg, consumertest.NewNop(), newTestTProcessor(nil))
    require.NoError(t, err)

    assert.True(t, tp.Capabilities().MutatesData)
    assert.NoError(t, tp.Start(context.Background(), componenttest.NewNopHost()))
    assert.NoError(t, tp.ConsumeTraces(context.Background(), ptrace.NewTraces()))
    assert.NoError(t, tp.Shutdown(context.Background()))
}

func TestNewTracesProcessor_WithOptions(t *testing.T) {
    want := errors.New("my_error")
    tp, err := NewTracesProcessor(context.Background(), processortest.NewNopCreateSettings(), &testTracesCfg, consumertest.NewNop(), newTestTProcessor(nil),
        WithStart(func(context.Context, component.Host) error { return want }),
        WithShutdown(func(context.Context) error { return want }),
        WithCapabilities(consumer.Capabilities{MutatesData: false}))
    assert.NoError(t, err)

    assert.Equal(t, want, tp.Start(context.Background(), componenttest.NewNopHost()))
    assert.Equal(t, want, tp.Shutdown(context.Background()))
    assert.False(t, tp.Capabilities().MutatesData)
}

func TestNewTracesProcessor_NilRequiredFields(t *testing.T) {
    _, err := NewTracesProcessor(context.Background(), processortest.NewNopCreateSettings(), &testTracesCfg, consumertest.NewNop(), nil)
    assert.Error(t, err)

    _, err = NewTracesProcessor(context.Background(), processortest.NewNopCreateSettings(), &testTracesCfg, nil, newTestTProcessor(nil))
    assert.Equal(t, component.ErrNilNextConsumer, err)
}

func TestNewTracesProcessor_ProcessTraceError(t *testing.T) {
    want := errors.New("my_error")
    tp, err := NewTracesProcessor(context.Background(), processortest.NewNopCreateSettings(), &testTracesCfg, consumertest.NewNop(), newTestTProcessor(want))
    require.NoError(t, err)
    assert.Equal(t, want, tp.ConsumeTraces(context.Background(), ptrace.NewTraces()))
}

func TestNewTracesProcessor_ProcessTracesErrSkipProcessingData(t *testing.T) {
    tp, err := NewTracesProcessor(context.Background(), processortest.NewNopCreateSettings(), &testTracesCfg, consumertest.NewNop(), newTestTProcessor(ErrSkipProcessingData))
    require.NoError(t, err)
    assert.Equal(t, nil, tp.ConsumeTraces(context.Background(), ptrace.NewTraces()))
}

func newTestTProcessor(retError error) ProcessTracesFunc {
    return func(_ context.Context, td ptrace.Traces) (ptrace.Traces, error) {
        return td, retError
    }
}
