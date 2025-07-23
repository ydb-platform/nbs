#include "trace_reader.h"

#include "trace_convert.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/opentelemetry/iface/trace_service_client.h>

#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/resource/v1/resource.pb.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <utility>

namespace NCloud {

using namespace opentelemetry::proto::collector::trace::v1;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTraceOpenTelemetryExporter final: public ITraceReader
{
private:
    const TString ComponentName;
    const TString ServiceName;
    const TString Tag;
    const ITraceReaderPtr Consumer;

    ILoggingServicePtr Logging;
    ITraceServiceClientPtr TraceServiceClient;

    ui64 TracksCount = 0;

public:
    TTraceOpenTelemetryExporter(
            TString id,
            TString componentName,
            TString serviceName,
            TString tag,
            ITraceReaderPtr consumer,
            ILoggingServicePtr logging,
            ITraceServiceClientPtr traceServiceClient)
        : ITraceReader(std::move(id))
        , ComponentName(std::move(componentName))
        , ServiceName(std::move(serviceName))
        , Tag(std::move(tag))
        , Consumer(std::move(consumer))
        , Logging(std::move(logging))
        , TraceServiceClient(std::move(traceServiceClient))
    {}

    void Push(TThread::TId tid, const NLWTrace::TTrackLog& tl) override
    {
        Y_UNUSED(tid);

        if (tl.Items.empty() || ++TracksCount > DumpTracksLimit) {
            return;
        }

        auto traceInfo = ConvertToOpenTelemetrySpans(tl);
        const auto spans = std::move(traceInfo.Spans);

        ExportTraceServiceRequest traces;
        auto* resourceSpans = traces.add_resource_spans();

        auto* serviceNameAttribute =
            resourceSpans->mutable_resource()->mutable_attributes()->Add();
        serviceNameAttribute->set_key("service.name");
        serviceNameAttribute->mutable_value()->set_string_value(ServiceName);

        auto* tagAttribute =
            resourceSpans->mutable_resource()->mutable_attributes()->Add();
        tagAttribute->set_key("tag");
        tagAttribute->mutable_value()->set_string_value(Tag);

        if (traceInfo.RequestId) {
            auto* requestIdAttribute =
                resourceSpans->mutable_resource()->mutable_attributes()->Add();
            requestIdAttribute->set_key("requestId");
            requestIdAttribute->mutable_value()->set_string_value(
                ToString(traceInfo.RequestId));
        }

        if (traceInfo.DiskId) {
            auto* diskIdAttribute =
                resourceSpans->mutable_resource()->mutable_attributes()->Add();
            diskIdAttribute->set_key("diskId");
            diskIdAttribute->mutable_value()->set_string_value(
                std::move(traceInfo.DiskId));
        }

        auto* scopedSpans = resourceSpans->add_scope_spans();
        for (const auto& span: spans) {
            *scopedSpans->add_spans() = span;
        }

        TraceServiceClient->Export(std::move(traces), "")
            .Subscribe(
                [logging = Logging, componentName = ComponentName](
                    NThreading::TFuture<ITraceServiceClient::TResponse>
                        responseFuture)
                {
                    TLog Log = logging->CreateLog(componentName);
                    auto response = responseFuture.ExtractValue();
                    if (HasError(response)) {
                        STORAGE_WARN(
                            "Failed to export traces: "
                            << FormatError(response.GetError()));
                    }
                });

        Consumer->Push(tid, tl);
    }

    void Reset() override
    {
        TracksCount = 0;
        Consumer->Reset();
    }

    void ForEach(std::function<void(const TEntry&)> fn) override
    {
        Consumer->ForEach(fn);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITraceReaderPtr CreateTraceExporter(
    TString id,
    ILoggingServicePtr logging,
    TString componentName,
    TString tag,
    ITraceReaderPtr consumer,
    ITraceServiceClientPtr traceServiceClient,
    TString serviceName)
{
    return std::make_shared<TTraceOpenTelemetryExporter>(
        std::move(id),
        std::move(componentName),
        std::move(serviceName),
        std::move(tag),
        std::move(consumer),
        std::move(logging),
        std::move(traceServiceClient));
}

ITraceReaderPtr SetupTraceReaderWithOpentelemetryExport(
    TString id,
    ILoggingServicePtr logging,
    TString componentName,
    TString tag,
    ITraceServiceClientPtr traceServiceClient,
    TString serviceName,
    ELogPriority priority)
{
    ITraceReaderPtr traceReader =
        std::make_shared<TTraceReaderWithRingBuffer>(id, tag);
    traceReader = CreateTraceExporter(
        id,
        logging,
        componentName,
        tag,
        std::move(traceReader),
        std::move(traceServiceClient),
        std::move(serviceName));
    traceReader = CreateTraceLogger(
        std::move(id),
        std::move(traceReader),
        std::move(logging),
        std::move(componentName),
        std::move(tag),
        priority);
    return traceReader;
}

ITraceReaderPtr SetupTraceReaderForSlowRequestsWithOpentelemetryExport(
    TString id,
    ILoggingServicePtr logging,
    TString componentName,
    ITraceServiceClientPtr traceServiceClient,
    TString serviceName,
    TRequestThresholds requestThresholds,
    TString tag)
{
    auto traceReader = SetupTraceReaderWithOpentelemetryExport(
        id,
        logging,
        componentName,
        std::move(tag),
        std::move(traceServiceClient),
        std::move(serviceName),
        ELogPriority::TLOG_WARNING);

    return CreateSlowRequestsFilter(
        std::move(id),
        std::move(traceReader),
        std::move(logging),
        std::move(componentName),
        std::move(requestThresholds));
}

}   // namespace NCloud
