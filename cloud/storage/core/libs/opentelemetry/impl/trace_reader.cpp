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

class TTraceOpenTelemetryExporter final: public ITraceReaderWithRingBuffer
{
private:
    const TString ComponentName;
    const TString ServiceName;

    ILoggingServicePtr Logging;
    ITraceServiceClientPtr TraceServiceClient;

    ui64 TracksCount = 0;

public:
    TTraceOpenTelemetryExporter(
            TString id,
            TString componentName,
            TString serviceName,
            ILoggingServicePtr logging,
            ITraceServiceClientPtr traceServiceClient)
        : ITraceReaderWithRingBuffer(std::move(id))
        , ComponentName(std::move(componentName))
        , ServiceName(std::move(serviceName))
        , Logging(std::move(logging))
        , TraceServiceClient(std::move(traceServiceClient))
    {}

    void Push(TThread::TId tid, const NLWTrace::TTrackLog& tl) override
    {
        Y_UNUSED(tid);

        if (tl.Items.empty() || ++TracksCount > DumpTracksLimit) {
            return;
        }

        ui64 minSeenTimestamp = tl.Items[0].TimestampCycles;

        auto spans = ConvertToOpenTelemetrySpans(tl);

        ExportTraceServiceRequest traces;
        auto* resourceSpans = traces.add_resource_spans();
        auto* attribute =
            resourceSpans->mutable_resource()->mutable_attributes()->Add();
        attribute->set_key("service.name");
        attribute->mutable_value()->set_string_value(ServiceName);

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

        RingBuffer.PushBack(
            {.Ts = TInstant::Now(),
             .Date = minSeenTimestamp,
             .TrackLog = tl,
             .Tag = "AllRequests"});
    }

    void Reset() override
    {
        TracksCount = 0;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITraceReaderPtr CreateTraceExporter(
    TString id,
    ILoggingServicePtr logging,
    TString componentName,
    ITraceServiceClientPtr traceServiceClient,
    TString serviceName)
{
    return std::make_shared<TTraceOpenTelemetryExporter>(
        std::move(id),
        std::move(componentName),
        std::move(serviceName),
        std::move(logging),
        std::move(traceServiceClient));
}

}   // namespace NCloud
