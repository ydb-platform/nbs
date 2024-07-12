// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exporterhelper

import (
    "context"
    "errors"
    "testing"

    "github.com/stretchr/testify/require"
    "go.opencensus.io/tag"
    "go.opentelemetry.io/otel/codes"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"

    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/component/componenttest"
    "go.opentelemetry.io/collector/exporter"
    "go.opentelemetry.io/collector/exporter/exportertest"
)

var (
    defaultID       = component.NewID("test")
    defaultSettings = func() exporter.CreateSettings {
        set := exportertest.NewNopCreateSettings()
        set.ID = defaultID
        return set
    }()
    exporterTag, _      = tag.NewKey("exporter")
    defaultExporterTags = []tag.Tag{
        {Key: exporterTag, Value: "test"},
    }
)

func newNoopObsrepSender(_ *obsExporter) requestSender {
    return &baseRequestSender{}
}

func TestBaseExporter(t *testing.T) {
    be, err := newBaseExporter(defaultSettings, "", false, nil, nil, newNoopObsrepSender)
    require.NoError(t, err)
    require.NoError(t, be.Start(context.Background(), componenttest.NewNopHost()))
    require.NoError(t, be.Shutdown(context.Background()))
    be, err = newBaseExporter(defaultSettings, "", true, nil, nil, newNoopObsrepSender)
    require.NoError(t, err)
    require.NoError(t, be.Start(context.Background(), componenttest.NewNopHost()))
    require.NoError(t, be.Shutdown(context.Background()))
}

func TestBaseExporterWithOptions(t *testing.T) {
    want := errors.New("my error")
    be, err := newBaseExporter(
        defaultSettings, "", false, nil, nil, newNoopObsrepSender,
        WithStart(func(ctx context.Context, host component.Host) error { return want }),
        WithShutdown(func(ctx context.Context) error { return want }),
        WithTimeout(NewDefaultTimeoutSettings()),
    )
    require.NoError(t, err)
    require.Equal(t, want, be.Start(context.Background(), componenttest.NewNopHost()))
    require.Equal(t, want, be.Shutdown(context.Background()))
}

func checkStatus(t *testing.T, sd sdktrace.ReadOnlySpan, err error) {
    if err != nil {
        require.Equal(t, codes.Error, sd.Status().Code, "SpanData %v", sd)
        require.Equal(t, err.Error(), sd.Status().Description, "SpanData %v", sd)
    } else {
        require.Equal(t, codes.Unset, sd.Status().Code, "SpanData %v", sd)
    }
}
