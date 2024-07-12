// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package obsreport

import (
    "context"
    "errors"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/codes"

    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/internal/obsreportconfig/obsmetrics"
    "go.opentelemetry.io/collector/obsreport/obsreporttest"
    "go.opentelemetry.io/collector/receiver/scrapererror"
)

const (
    transport = "fakeTransport"
    format    = "fakeFormat"
)

var (
    receiverID  = component.NewID("fakeReceiver")
    scraperID   = component.NewID("fakeScraper")
    processorID = component.NewID("fakeProcessor")
    exporterID  = component.NewID("fakeExporter")

    errFake        = errors.New("errFake")
    partialErrFake = scrapererror.NewPartialScrapeError(errFake, 1)
)

type testParams struct {
    items int
    err   error
}

func testTelemetry(t *testing.T, id component.ID, testFunc func(t *testing.T, tt obsreporttest.TestTelemetry, useOtel bool)) {
    t.Run("WithOC", func(t *testing.T) {
        tt, err := obsreporttest.SetupTelemetry(id)
        require.NoError(t, err)
        t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) })

        testFunc(t, tt, false)
    })

    t.Run("WithOTel", func(t *testing.T) {
        tt, err := obsreporttest.SetupTelemetry(id)
        require.NoError(t, err)
        t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) })

        testFunc(t, tt, true)
    })
}

func TestReceiveTraceDataOp(t *testing.T) {
    testTelemetry(t, receiverID, func(t *testing.T, tt obsreporttest.TestTelemetry, useOtel bool) {
        parentCtx, parentSpan := tt.TracerProvider.Tracer("test").Start(context.Background(), t.Name())
        defer parentSpan.End()

        params := []testParams{
            {items: 13, err: errFake},
            {items: 42, err: nil},
        }
        for i, param := range params {
            rec, err := newReceiver(ReceiverSettings{
                ReceiverID:             receiverID,
                Transport:              transport,
                ReceiverCreateSettings: tt.ToReceiverCreateSettings(),
            }, useOtel)
            require.NoError(t, err)
            ctx := rec.StartTracesOp(parentCtx)
            assert.NotNil(t, ctx)
            rec.EndTracesOp(ctx, format, params[i].items, param.err)
        }

        spans := tt.SpanRecorder.Ended()
        require.Equal(t, len(params), len(spans))

        var acceptedSpans, refusedSpans int
        for i, span := range spans {
            assert.Equal(t, "receiver/"+receiverID.String()+"/TraceDataReceived", span.Name())
            switch {
            case params[i].err == nil:
                acceptedSpans += params[i].items
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.AcceptedSpansKey, Value: attribute.Int64Value(int64(params[i].items))})
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.RefusedSpansKey, Value: attribute.Int64Value(0)})
                assert.Equal(t, codes.Unset, span.Status().Code)
            case errors.Is(params[i].err, errFake):
                refusedSpans += params[i].items
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.AcceptedSpansKey, Value: attribute.Int64Value(0)})
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.RefusedSpansKey, Value: attribute.Int64Value(int64(params[i].items))})
                assert.Equal(t, codes.Error, span.Status().Code)
                assert.Equal(t, params[i].err.Error(), span.Status().Description)
            default:
                t.Fatalf("unexpected param: %v", params[i])
            }
        }
        require.NoError(t, tt.CheckReceiverTraces(transport, int64(acceptedSpans), int64(refusedSpans)))
    })
}

func TestReceiveLogsOp(t *testing.T) {
    testTelemetry(t, receiverID, func(t *testing.T, tt obsreporttest.TestTelemetry, useOtel bool) {
        parentCtx, parentSpan := tt.TracerProvider.Tracer("test").Start(context.Background(), t.Name())
        defer parentSpan.End()

        params := []testParams{
            {items: 13, err: errFake},
            {items: 42, err: nil},
        }
        for i, param := range params {
            rec, err := newReceiver(ReceiverSettings{
                ReceiverID:             receiverID,
                Transport:              transport,
                ReceiverCreateSettings: tt.ToReceiverCreateSettings(),
            }, useOtel)
            require.NoError(t, err)

            ctx := rec.StartLogsOp(parentCtx)
            assert.NotNil(t, ctx)
            rec.EndLogsOp(ctx, format, params[i].items, param.err)
        }

        spans := tt.SpanRecorder.Ended()
        require.Equal(t, len(params), len(spans))

        var acceptedLogRecords, refusedLogRecords int
        for i, span := range spans {
            assert.Equal(t, "receiver/"+receiverID.String()+"/LogsReceived", span.Name())
            switch {
            case params[i].err == nil:
                acceptedLogRecords += params[i].items
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.AcceptedLogRecordsKey, Value: attribute.Int64Value(int64(params[i].items))})
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.RefusedLogRecordsKey, Value: attribute.Int64Value(0)})
                assert.Equal(t, codes.Unset, span.Status().Code)
            case errors.Is(params[i].err, errFake):
                refusedLogRecords += params[i].items
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.AcceptedLogRecordsKey, Value: attribute.Int64Value(0)})
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.RefusedLogRecordsKey, Value: attribute.Int64Value(int64(params[i].items))})
                assert.Equal(t, codes.Error, span.Status().Code)
                assert.Equal(t, params[i].err.Error(), span.Status().Description)
            default:
                t.Fatalf("unexpected param: %v", params[i])
            }
        }
        require.NoError(t, tt.CheckReceiverLogs(transport, int64(acceptedLogRecords), int64(refusedLogRecords)))
    })
}

func TestReceiveMetricsOp(t *testing.T) {
    testTelemetry(t, receiverID, func(t *testing.T, tt obsreporttest.TestTelemetry, useOtel bool) {
        parentCtx, parentSpan := tt.TracerProvider.Tracer("test").Start(context.Background(), t.Name())
        defer parentSpan.End()

        params := []testParams{
            {items: 23, err: errFake},
            {items: 29, err: nil},
        }
        for i, param := range params {
            rec, err := newReceiver(ReceiverSettings{
                ReceiverID:             receiverID,
                Transport:              transport,
                ReceiverCreateSettings: tt.ToReceiverCreateSettings(),
            }, useOtel)
            require.NoError(t, err)

            ctx := rec.StartMetricsOp(parentCtx)
            assert.NotNil(t, ctx)
            rec.EndMetricsOp(ctx, format, params[i].items, param.err)
        }

        spans := tt.SpanRecorder.Ended()
        require.Equal(t, len(params), len(spans))

        var acceptedMetricPoints, refusedMetricPoints int
        for i, span := range spans {
            assert.Equal(t, "receiver/"+receiverID.String()+"/MetricsReceived", span.Name())
            switch {
            case params[i].err == nil:
                acceptedMetricPoints += params[i].items
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.AcceptedMetricPointsKey, Value: attribute.Int64Value(int64(params[i].items))})
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.RefusedMetricPointsKey, Value: attribute.Int64Value(0)})
                assert.Equal(t, codes.Unset, span.Status().Code)
            case errors.Is(params[i].err, errFake):
                refusedMetricPoints += params[i].items
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.AcceptedMetricPointsKey, Value: attribute.Int64Value(0)})
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.RefusedMetricPointsKey, Value: attribute.Int64Value(int64(params[i].items))})
                assert.Equal(t, codes.Error, span.Status().Code)
                assert.Equal(t, params[i].err.Error(), span.Status().Description)
            default:
                t.Fatalf("unexpected param: %v", params[i])
            }
        }

        require.NoError(t, tt.CheckReceiverMetrics(transport, int64(acceptedMetricPoints), int64(refusedMetricPoints)))
    })
}

func TestScrapeMetricsDataOp(t *testing.T) {
    testTelemetry(t, receiverID, func(t *testing.T, tt obsreporttest.TestTelemetry, useOtel bool) {
        parentCtx, parentSpan := tt.TracerProvider.Tracer("test").Start(context.Background(), t.Name())
        defer parentSpan.End()

        params := []testParams{
            {items: 23, err: partialErrFake},
            {items: 29, err: errFake},
            {items: 15, err: nil},
        }
        for i := range params {
            scrp, err := newScraper(ScraperSettings{
                ReceiverID:             receiverID,
                Scraper:                scraperID,
                ReceiverCreateSettings: tt.ToReceiverCreateSettings(),
            }, useOtel)
            require.NoError(t, err)
            ctx := scrp.StartMetricsOp(parentCtx)
            assert.NotNil(t, ctx)
            scrp.EndMetricsOp(ctx, params[i].items, params[i].err)
        }

        spans := tt.SpanRecorder.Ended()
        require.Equal(t, len(params), len(spans))

        var scrapedMetricPoints, erroredMetricPoints int
        for i, span := range spans {
            assert.Equal(t, "scraper/"+receiverID.String()+"/"+scraperID.String()+"/MetricsScraped", span.Name())
            switch {
            case params[i].err == nil:
                scrapedMetricPoints += params[i].items
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.ScrapedMetricPointsKey, Value: attribute.Int64Value(int64(params[i].items))})
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.ErroredMetricPointsKey, Value: attribute.Int64Value(0)})
                assert.Equal(t, codes.Unset, span.Status().Code)
            case errors.Is(params[i].err, errFake):
                erroredMetricPoints += params[i].items
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.ScrapedMetricPointsKey, Value: attribute.Int64Value(0)})
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.ErroredMetricPointsKey, Value: attribute.Int64Value(int64(params[i].items))})
                assert.Equal(t, codes.Error, span.Status().Code)
                assert.Equal(t, params[i].err.Error(), span.Status().Description)

            case errors.Is(params[i].err, partialErrFake):
                scrapedMetricPoints += params[i].items
                erroredMetricPoints++
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.ScrapedMetricPointsKey, Value: attribute.Int64Value(int64(params[i].items))})
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.ErroredMetricPointsKey, Value: attribute.Int64Value(1)})
                assert.Equal(t, codes.Error, span.Status().Code)
                assert.Equal(t, params[i].err.Error(), span.Status().Description)
            default:
                t.Fatalf("unexpected err param: %v", params[i].err)
            }
        }

        require.NoError(t, obsreporttest.CheckScraperMetrics(tt, receiverID, scraperID, int64(scrapedMetricPoints), int64(erroredMetricPoints)))
    })
}

func TestExportTraceDataOp(t *testing.T) {
    testTelemetry(t, exporterID, func(t *testing.T, tt obsreporttest.TestTelemetry, useOtel bool) {
        parentCtx, parentSpan := tt.TracerProvider.Tracer("test").Start(context.Background(), t.Name())
        defer parentSpan.End()

        obsrep, err := newExporter(ExporterSettings{
            ExporterID:             exporterID,
            ExporterCreateSettings: tt.ToExporterCreateSettings(),
        }, useOtel)
        require.NoError(t, err)

        params := []testParams{
            {items: 22, err: nil},
            {items: 14, err: errFake},
        }
        for i := range params {
            ctx := obsrep.StartTracesOp(parentCtx)
            assert.NotNil(t, ctx)
            obsrep.EndTracesOp(ctx, params[i].items, params[i].err)
        }

        spans := tt.SpanRecorder.Ended()
        require.Equal(t, len(params), len(spans))

        var sentSpans, failedToSendSpans int
        for i, span := range spans {
            assert.Equal(t, "exporter/"+exporterID.String()+"/traces", span.Name())
            switch {
            case params[i].err == nil:
                sentSpans += params[i].items
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.SentSpansKey, Value: attribute.Int64Value(int64(params[i].items))})
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.FailedToSendSpansKey, Value: attribute.Int64Value(0)})
                assert.Equal(t, codes.Unset, span.Status().Code)
            case errors.Is(params[i].err, errFake):
                failedToSendSpans += params[i].items
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.SentSpansKey, Value: attribute.Int64Value(0)})
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.FailedToSendSpansKey, Value: attribute.Int64Value(int64(params[i].items))})
                assert.Equal(t, codes.Error, span.Status().Code)
                assert.Equal(t, params[i].err.Error(), span.Status().Description)
            default:
                t.Fatalf("unexpected error: %v", params[i].err)
            }
        }

        require.NoError(t, tt.CheckExporterTraces(int64(sentSpans), int64(failedToSendSpans)))
    })
}

func TestExportMetricsOp(t *testing.T) {
    testTelemetry(t, exporterID, func(t *testing.T, tt obsreporttest.TestTelemetry, useOtel bool) {
        parentCtx, parentSpan := tt.TracerProvider.Tracer("test").Start(context.Background(), t.Name())
        defer parentSpan.End()

        obsrep, err := newExporter(ExporterSettings{
            ExporterID:             exporterID,
            ExporterCreateSettings: tt.ToExporterCreateSettings(),
        }, useOtel)
        require.NoError(t, err)

        params := []testParams{
            {items: 17, err: nil},
            {items: 23, err: errFake},
        }
        for i := range params {
            ctx := obsrep.StartMetricsOp(parentCtx)
            assert.NotNil(t, ctx)

            obsrep.EndMetricsOp(ctx, params[i].items, params[i].err)
        }

        spans := tt.SpanRecorder.Ended()
        require.Equal(t, len(params), len(spans))

        var sentMetricPoints, failedToSendMetricPoints int
        for i, span := range spans {
            assert.Equal(t, "exporter/"+exporterID.String()+"/metrics", span.Name())
            switch {
            case params[i].err == nil:
                sentMetricPoints += params[i].items
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.SentMetricPointsKey, Value: attribute.Int64Value(int64(params[i].items))})
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.FailedToSendMetricPointsKey, Value: attribute.Int64Value(0)})
                assert.Equal(t, codes.Unset, span.Status().Code)
            case errors.Is(params[i].err, errFake):
                failedToSendMetricPoints += params[i].items
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.SentMetricPointsKey, Value: attribute.Int64Value(0)})
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.FailedToSendMetricPointsKey, Value: attribute.Int64Value(int64(params[i].items))})
                assert.Equal(t, codes.Error, span.Status().Code)
                assert.Equal(t, params[i].err.Error(), span.Status().Description)
            default:
                t.Fatalf("unexpected error: %v", params[i].err)
            }
        }

        require.NoError(t, tt.CheckExporterMetrics(int64(sentMetricPoints), int64(failedToSendMetricPoints)))
    })
}

func TestExportLogsOp(t *testing.T) {
    testTelemetry(t, exporterID, func(t *testing.T, tt obsreporttest.TestTelemetry, useOtel bool) {
        parentCtx, parentSpan := tt.TracerProvider.Tracer("test").Start(context.Background(), t.Name())
        defer parentSpan.End()

        obsrep, err := newExporter(ExporterSettings{
            ExporterID:             exporterID,
            ExporterCreateSettings: tt.ToExporterCreateSettings(),
        }, useOtel)
        require.NoError(t, err)

        params := []testParams{
            {items: 17, err: nil},
            {items: 23, err: errFake},
        }
        for i := range params {
            ctx := obsrep.StartLogsOp(parentCtx)
            assert.NotNil(t, ctx)

            obsrep.EndLogsOp(ctx, params[i].items, params[i].err)
        }

        spans := tt.SpanRecorder.Ended()
        require.Equal(t, len(params), len(spans))

        var sentLogRecords, failedToSendLogRecords int
        for i, span := range spans {
            assert.Equal(t, "exporter/"+exporterID.String()+"/logs", span.Name())
            switch {
            case params[i].err == nil:
                sentLogRecords += params[i].items
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.SentLogRecordsKey, Value: attribute.Int64Value(int64(params[i].items))})
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.FailedToSendLogRecordsKey, Value: attribute.Int64Value(0)})
                assert.Equal(t, codes.Unset, span.Status().Code)
            case errors.Is(params[i].err, errFake):
                failedToSendLogRecords += params[i].items
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.SentLogRecordsKey, Value: attribute.Int64Value(0)})
                require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.FailedToSendLogRecordsKey, Value: attribute.Int64Value(int64(params[i].items))})
                assert.Equal(t, codes.Error, span.Status().Code)
                assert.Equal(t, params[i].err.Error(), span.Status().Description)
            default:
                t.Fatalf("unexpected error: %v", params[i].err)
            }
        }

        require.NoError(t, tt.CheckExporterLogs(int64(sentLogRecords), int64(failedToSendLogRecords)))
    })
}

func TestReceiveWithLongLivedCtx(t *testing.T) {
    tt, err := obsreporttest.SetupTelemetry(receiverID)
    require.NoError(t, err)
    t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) })

    longLivedCtx, parentSpan := tt.TracerProvider.Tracer("test").Start(context.Background(), t.Name())
    defer parentSpan.End()

    params := []testParams{
        {items: 17, err: nil},
        {items: 23, err: errFake},
    }
    for i := range params {
        // Use a new context on each operation to simulate distinct operations
        // under the same long lived context.
        rec, rerr := NewReceiver(ReceiverSettings{
            ReceiverID:             receiverID,
            Transport:              transport,
            LongLivedCtx:           true,
            ReceiverCreateSettings: tt.ToReceiverCreateSettings(),
        })
        require.NoError(t, rerr)
        ctx := rec.StartTracesOp(longLivedCtx)
        assert.NotNil(t, ctx)
        rec.EndTracesOp(ctx, format, params[i].items, params[i].err)
    }

    spans := tt.SpanRecorder.Ended()
    require.Equal(t, len(params), len(spans))

    for i, span := range spans {
        assert.False(t, span.Parent().IsValid())
        require.Equal(t, 1, len(span.Links()))
        link := span.Links()[0]
        assert.Equal(t, parentSpan.SpanContext().TraceID(), link.SpanContext.TraceID())
        assert.Equal(t, parentSpan.SpanContext().SpanID(), link.SpanContext.SpanID())
        assert.Equal(t, "receiver/"+receiverID.String()+"/TraceDataReceived", span.Name())
        require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.TransportKey, Value: attribute.StringValue(transport)})
        switch {
        case params[i].err == nil:
            require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.AcceptedSpansKey, Value: attribute.Int64Value(int64(params[i].items))})
            require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.RefusedSpansKey, Value: attribute.Int64Value(0)})
            assert.Equal(t, codes.Unset, span.Status().Code)
        case errors.Is(params[i].err, errFake):
            require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.AcceptedSpansKey, Value: attribute.Int64Value(0)})
            require.Contains(t, span.Attributes(), attribute.KeyValue{Key: obsmetrics.RefusedSpansKey, Value: attribute.Int64Value(int64(params[i].items))})
            assert.Equal(t, codes.Error, span.Status().Code)
            assert.Equal(t, params[i].err.Error(), span.Status().Description)
        default:
            t.Fatalf("unexpected error: %v", params[i].err)
        }
    }
}

func TestProcessorTraceData(t *testing.T) {
    testTelemetry(t, processorID, func(t *testing.T, tt obsreporttest.TestTelemetry, useOtel bool) {
        const acceptedSpans = 27
        const refusedSpans = 19
        const droppedSpans = 13
        obsrep, err := newProcessor(ProcessorSettings{
            ProcessorID:             processorID,
            ProcessorCreateSettings: tt.ToProcessorCreateSettings(),
        }, useOtel)
        require.NoError(t, err)
        obsrep.TracesAccepted(context.Background(), acceptedSpans)
        obsrep.TracesRefused(context.Background(), refusedSpans)
        obsrep.TracesDropped(context.Background(), droppedSpans)

        require.NoError(t, tt.CheckProcessorTraces(acceptedSpans, refusedSpans, droppedSpans))
    })
}

func TestProcessorMetricsData(t *testing.T) {
    testTelemetry(t, processorID, func(t *testing.T, tt obsreporttest.TestTelemetry, useOtel bool) {
        const acceptedPoints = 29
        const refusedPoints = 11
        const droppedPoints = 17

        obsrep, err := newProcessor(ProcessorSettings{
            ProcessorID:             processorID,
            ProcessorCreateSettings: tt.ToProcessorCreateSettings(),
        }, useOtel)
        require.NoError(t, err)
        obsrep.MetricsAccepted(context.Background(), acceptedPoints)
        obsrep.MetricsRefused(context.Background(), refusedPoints)
        obsrep.MetricsDropped(context.Background(), droppedPoints)

        require.NoError(t, tt.CheckProcessorMetrics(acceptedPoints, refusedPoints, droppedPoints))
    })
}

func TestBuildProcessorCustomMetricName(t *testing.T) {
    tests := []struct {
        name string
        want string
    }{
        {
            name: "firstMeasure",
            want: "processor/test_type/firstMeasure",
        },
        {
            name: "secondMeasure",
            want: "processor/test_type/secondMeasure",
        },
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got := BuildProcessorCustomMetricName("test_type", tt.name)
            assert.Equal(t, tt.want, got)
        })
    }
}

func TestProcessorLogRecords(t *testing.T) {
    testTelemetry(t, processorID, func(t *testing.T, tt obsreporttest.TestTelemetry, useOtel bool) {
        const acceptedRecords = 29
        const refusedRecords = 11
        const droppedRecords = 17

        obsrep, err := newProcessor(ProcessorSettings{
            ProcessorID:             processorID,
            ProcessorCreateSettings: tt.ToProcessorCreateSettings(),
        }, useOtel)
        require.NoError(t, err)
        obsrep.LogsAccepted(context.Background(), acceptedRecords)
        obsrep.LogsRefused(context.Background(), refusedRecords)
        obsrep.LogsDropped(context.Background(), droppedRecords)

        require.NoError(t, tt.CheckProcessorLogs(acceptedRecords, refusedRecords, droppedRecords))
    })
}
