#include "trace_reader.h"

#include "trace_convert.h"

#include <cloud/blockstore/libs/opentelemetry/iface/trace_service_client.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/resource/v1/resource.pb.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <utility>

namespace NCloud::NBlockStore {

using namespace opentelemetry::proto::collector::trace::v1;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTraceOpenTelemetryExporter final: public ITraceReaderWithRingBuffer
{
private:
    ui64 TracksCount = 0;
    ILoggingServicePtr Logging;
    TString ComponentName;
    ITraceServiceClientPtr TraceServiceClient;
    TString ServiceName;

public:
    TTraceOpenTelemetryExporter(
            TString id,
            ILoggingServicePtr logging,
            TString componentName,
            ITraceServiceClientPtr traceServiceClient,
            TString serviceName)
        : ITraceReaderWithRingBuffer(std::move(id))
        , Logging(std::move(logging))
        , ComponentName(std::move(componentName))
        , TraceServiceClient(std::move(traceServiceClient))
        , ServiceName(std::move(serviceName))
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
        auto* resourceSpans = traces.Addresource_spans();
        auto* attribute =
            resourceSpans->Mutableresource()->Mutableattributes()->Add();
        attribute->Setkey("service.name");
        attribute->Mutablevalue()->Setstring_value(ServiceName);

        auto* scopedSpans = resourceSpans->Addscope_spans();
        for (const auto& span: spans) {
            *scopedSpans->Addspans() = span;
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
            {TInstant::Now(), minSeenTimestamp, tl, "AllRequests"});
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
        std::move(logging),
        std::move(componentName),
        std::move(traceServiceClient),
        std::move(serviceName));
}

}   // namespace NCloud::NBlockStore
