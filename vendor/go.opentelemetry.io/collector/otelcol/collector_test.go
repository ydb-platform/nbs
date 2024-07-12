// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package collector handles the command-line, configuration, and runs the OC collector.
package otelcol

import (
    "context"
    "errors"
    "path/filepath"
    "sync"
    "syscall"
    "testing"
    "time"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"

    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/confmap"
    "go.opentelemetry.io/collector/confmap/converter/expandconverter"
)

func TestStateString(t *testing.T) {
    assert.Equal(t, "Starting", StateStarting.String())
    assert.Equal(t, "Running", StateRunning.String())
    assert.Equal(t, "Closing", StateClosing.String())
    assert.Equal(t, "Closed", StateClosed.String())
    assert.Equal(t, "UNKNOWN", State(13).String())
}

func TestCollectorStartAsGoRoutine(t *testing.T) {
    factories, err := nopFactories()
    require.NoError(t, err)

    cfgProvider, err := NewConfigProvider(newDefaultConfigProviderSettings([]string{filepath.Join("testdata", "otelcol-nop.yaml")}))
    require.NoError(t, err)

    set := CollectorSettings{
        BuildInfo:      component.NewDefaultBuildInfo(),
        Factories:      factories,
        ConfigProvider: cfgProvider,
    }
    col, err := NewCollector(set)
    require.NoError(t, err)

    wg := startCollector(context.Background(), t, col)

    assert.Eventually(t, func() bool {
        return StateRunning == col.GetState()
    }, 2*time.Second, 200*time.Millisecond)

    col.Shutdown()
    col.Shutdown()
    wg.Wait()
    assert.Equal(t, StateClosed, col.GetState())
}

func TestCollectorCancelContext(t *testing.T) {
    factories, err := nopFactories()
    require.NoError(t, err)

    cfgProvider, err := NewConfigProvider(newDefaultConfigProviderSettings([]string{filepath.Join("testdata", "otelcol-nop.yaml")}))
    require.NoError(t, err)

    set := CollectorSettings{
        BuildInfo:      component.NewDefaultBuildInfo(),
        Factories:      factories,
        ConfigProvider: cfgProvider,
    }
    col, err := NewCollector(set)
    require.NoError(t, err)

    ctx, cancel := context.WithCancel(context.Background())
    wg := startCollector(ctx, t, col)

    assert.Eventually(t, func() bool {
        return StateRunning == col.GetState()
    }, 2*time.Second, 200*time.Millisecond)

    cancel()
    wg.Wait()
    assert.Equal(t, StateClosed, col.GetState())
}

type mockCfgProvider struct {
    ConfigProvider
    watcher chan error
}

func (p mockCfgProvider) Watch() <-chan error {
    return p.watcher
}

func TestCollectorStateAfterConfigChange(t *testing.T) {
    factories, err := nopFactories()
    require.NoError(t, err)

    provider, err := NewConfigProvider(newDefaultConfigProviderSettings([]string{filepath.Join("testdata", "otelcol-nop.yaml")}))
    require.NoError(t, err)

    watcher := make(chan error, 1)
    col, err := NewCollector(CollectorSettings{
        BuildInfo:      component.NewDefaultBuildInfo(),
        Factories:      factories,
        ConfigProvider: &mockCfgProvider{ConfigProvider: provider, watcher: watcher},
    })
    require.NoError(t, err)

    wg := startCollector(context.Background(), t, col)

    assert.Eventually(t, func() bool {
        return StateRunning == col.GetState()
    }, 2*time.Second, 200*time.Millisecond)

    watcher <- nil

    assert.Eventually(t, func() bool {
        return StateRunning == col.GetState()
    }, 2*time.Second, 200*time.Millisecond)

    col.Shutdown()

    wg.Wait()
    assert.Equal(t, StateClosed, col.GetState())
}

func TestCollectorReportError(t *testing.T) {
    factories, err := nopFactories()
    require.NoError(t, err)

    cfgProvider, err := NewConfigProvider(newDefaultConfigProviderSettings([]string{filepath.Join("testdata", "otelcol-nop.yaml")}))
    require.NoError(t, err)

    col, err := NewCollector(CollectorSettings{
        BuildInfo:      component.NewDefaultBuildInfo(),
        Factories:      factories,
        ConfigProvider: cfgProvider,
    })
    require.NoError(t, err)

    wg := startCollector(context.Background(), t, col)

    assert.Eventually(t, func() bool {
        return StateRunning == col.GetState()
    }, 2*time.Second, 200*time.Millisecond)

    col.asyncErrorChannel <- errors.New("err2")

    wg.Wait()
    assert.Equal(t, StateClosed, col.GetState())
}

func TestCollectorSendSignal(t *testing.T) {
    factories, err := nopFactories()
    require.NoError(t, err)

    cfgProvider, err := NewConfigProvider(newDefaultConfigProviderSettings([]string{filepath.Join("testdata", "otelcol-nop.yaml")}))
    require.NoError(t, err)

    col, err := NewCollector(CollectorSettings{
        BuildInfo:      component.NewDefaultBuildInfo(),
        Factories:      factories,
        ConfigProvider: cfgProvider,
    })
    require.NoError(t, err)

    wg := startCollector(context.Background(), t, col)

    assert.Eventually(t, func() bool {
        return StateRunning == col.GetState()
    }, 2*time.Second, 200*time.Millisecond)

    col.signalsChannel <- syscall.SIGHUP

    assert.Eventually(t, func() bool {
        return StateRunning == col.GetState()
    }, 2*time.Second, 200*time.Millisecond)

    col.signalsChannel <- syscall.SIGTERM

    wg.Wait()
    assert.Equal(t, StateClosed, col.GetState())
}

func TestCollectorFailedShutdown(t *testing.T) {
    t.Skip("This test was using telemetry shutdown failure, switch to use a component that errors on shutdown.")
    factories, err := nopFactories()
    require.NoError(t, err)

    cfgProvider, err := NewConfigProvider(newDefaultConfigProviderSettings([]string{filepath.Join("testdata", "otelcol-nop.yaml")}))
    require.NoError(t, err)

    col, err := NewCollector(CollectorSettings{
        BuildInfo:      component.NewDefaultBuildInfo(),
        Factories:      factories,
        ConfigProvider: cfgProvider,
    })
    require.NoError(t, err)

    wg := sync.WaitGroup{}
    wg.Add(1)
    go func() {
        defer wg.Done()
        assert.EqualError(t, col.Run(context.Background()), "failed to shutdown collector telemetry: err1")
    }()

    assert.Eventually(t, func() bool {
        return StateRunning == col.GetState()
    }, 2*time.Second, 200*time.Millisecond)

    col.Shutdown()

    wg.Wait()
    assert.Equal(t, StateClosed, col.GetState())
}

func TestCollectorStartInvalidConfig(t *testing.T) {
    factories, err := nopFactories()
    require.NoError(t, err)

    cfgProvider, err := NewConfigProvider(newDefaultConfigProviderSettings([]string{filepath.Join("testdata", "otelcol-invalid.yaml")}))
    require.NoError(t, err)

    col, err := NewCollector(CollectorSettings{
        BuildInfo:      component.NewDefaultBuildInfo(),
        Factories:      factories,
        ConfigProvider: cfgProvider,
    })
    require.NoError(t, err)
    assert.Error(t, col.Run(context.Background()))
}

func TestCollectorStartWithTraceContextPropagation(t *testing.T) {
    tests := []struct {
        file        string
        errExpected bool
    }{
        {file: "otelcol-invalidprop.yaml", errExpected: true},
        {file: "otelcol-nop.yaml", errExpected: false},
        {file: "otelcol-validprop.yaml", errExpected: false},
    }

    for _, tt := range tests {
        t.Run(tt.file, func(t *testing.T) {
            factories, err := nopFactories()
            require.NoError(t, err)

            cfgProvider, err := NewConfigProvider(newDefaultConfigProviderSettings([]string{filepath.Join("testdata", tt.file)}))
            require.NoError(t, err)

            set := CollectorSettings{
                BuildInfo:      component.NewDefaultBuildInfo(),
                Factories:      factories,
                ConfigProvider: cfgProvider,
            }

            col, err := NewCollector(set)
            require.NoError(t, err)

            if tt.errExpected {
                require.Error(t, col.Run(context.Background()))
                assert.Equal(t, StateClosed, col.GetState())
            } else {
                wg := startCollector(context.Background(), t, col)
                col.Shutdown()
                wg.Wait()
                assert.Equal(t, StateClosed, col.GetState())
            }
        })
    }
}

func TestCollectorRun(t *testing.T) {
    tests := []struct {
        file string
    }{
        {file: "otelcol-nometrics.yaml"},
        {file: "otelcol-noaddress.yaml"},
    }

    for _, tt := range tests {
        t.Run(tt.file, func(t *testing.T) {
            factories, err := nopFactories()
            require.NoError(t, err)

            cfgProvider, err := NewConfigProvider(newDefaultConfigProviderSettings([]string{filepath.Join("testdata", tt.file)}))
            require.NoError(t, err)

            set := CollectorSettings{
                BuildInfo:      component.NewDefaultBuildInfo(),
                Factories:      factories,
                ConfigProvider: cfgProvider,
            }
            col, err := NewCollector(set)
            require.NoError(t, err)

            wg := startCollector(context.Background(), t, col)

            col.Shutdown()
            wg.Wait()
            assert.Equal(t, StateClosed, col.GetState())
        })
    }
}

func TestCollectorShutdownBeforeRun(t *testing.T) {
    factories, err := nopFactories()
    require.NoError(t, err)

    cfgProvider, err := NewConfigProvider(newDefaultConfigProviderSettings([]string{filepath.Join("testdata", "otelcol-nop.yaml")}))
    require.NoError(t, err)

    set := CollectorSettings{
        BuildInfo:      component.NewDefaultBuildInfo(),
        Factories:      factories,
        ConfigProvider: cfgProvider,
    }
    col, err := NewCollector(set)
    require.NoError(t, err)

    // Calling shutdown before collector is running should cause it to return quickly
    require.NotPanics(t, func() { col.Shutdown() })

    wg := startCollector(context.Background(), t, col)

    col.Shutdown()
    wg.Wait()
    assert.Equal(t, StateClosed, col.GetState())
}

func TestCollectorClosedStateOnStartUpError(t *testing.T) {
    factories, err := nopFactories()
    require.NoError(t, err)

    cfgProvider, err := NewConfigProvider(newDefaultConfigProviderSettings([]string{filepath.Join("testdata", "otelcol-invalid.yaml")}))
    require.NoError(t, err)

    // Load a bad config causing startup to fail
    set := CollectorSettings{
        BuildInfo:      component.NewDefaultBuildInfo(),
        Factories:      factories,
        ConfigProvider: cfgProvider,
    }
    col, err := NewCollector(set)
    require.NoError(t, err)

    // Expect run to error
    require.Error(t, col.Run(context.Background()))

    // Expect state to be closed
    assert.Equal(t, StateClosed, col.GetState())
}

func TestCollectorDryRun(t *testing.T) {
    factories, err := nopFactories()
    require.NoError(t, err)

    cfgProvider, err := NewConfigProvider(newDefaultConfigProviderSettings([]string{filepath.Join("testdata", "otelcol-invalid.yaml")}))
    require.NoError(t, err)

    // Load a bad config causing startup to fail
    set := CollectorSettings{
        BuildInfo:      component.NewDefaultBuildInfo(),
        Factories:      factories,
        ConfigProvider: cfgProvider,
    }
    col, err := NewCollector(set)
    require.NoError(t, err)

    require.Error(t, col.DryRun(context.Background()))
}

func TestPassConfmapToServiceFailure(t *testing.T) {
    factories, err := nopFactories()
    require.NoError(t, err)

    cfgProvider, err := NewConfigProvider(ConfigProviderSettings{
        ResolverSettings: confmap.ResolverSettings{
            URIs:       []string{filepath.Join("testdata", "otelcol-invalid.yaml")},
            Providers:  makeMapProvidersMap(newFailureProvider()),
            Converters: []confmap.Converter{expandconverter.New()},
        },
    })
    require.NoError(t, err)

    set := CollectorSettings{
        BuildInfo:      component.NewDefaultBuildInfo(),
        Factories:      factories,
        ConfigProvider: cfgProvider,
    }
    col, err := NewCollector(set)
    require.NoError(t, err)

    err = col.Run(context.Background())
    require.Error(t, err)
}

func startCollector(ctx context.Context, t *testing.T, col *Collector) *sync.WaitGroup {
    wg := &sync.WaitGroup{}
    wg.Add(1)
    go func() {
        defer wg.Done()
        require.NoError(t, col.Run(ctx))
    }()
    return wg
}

type failureProvider struct{}

func newFailureProvider() confmap.Provider {
    return &failureProvider{}
}

func (fmp *failureProvider) Retrieve(_ context.Context, _ string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
    return nil, errors.New("a failure occurred during configuration retrieval")
}

func (*failureProvider) Scheme() string {
    return "file"
}

func (*failureProvider) Shutdown(context.Context) error {
    return nil
}
