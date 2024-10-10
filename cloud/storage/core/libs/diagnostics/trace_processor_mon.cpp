#include "trace_processor_mon.h"

#include "trace_processor.h"
#include "trace_reader.h"

#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/monlib/service/pages/html_mon_page.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TTraceKey = std::pair<TStringBuf, TStringBuf>;
using TTraceLog = std::pair<TStringBuf, TEntry>;

const TStringBuf slowName = "slow";
const TStringBuf randomName = "random";
const TStringBuf jsonName = "json";

const TTraceKey TRACE_TYPE_SLOW{slowName, "st_slow_requests_filter"};
const TTraceKey TRACE_TYPE_RANDOM{randomName, "st_trace_logger"};

using namespace NMonitoring;

////////////////////////////////////////////////////////////////////////////////

void SerializeTraceToJson(
    const NLWTrace::TTrackLog& tl,
    ui64 minSeenTimestamp,
    const TString& tag,
    NJsonWriter::TBuf& writer)
{
    TString paramValues[LWTRACE_MAX_PARAMS];

    writer.BeginList();
    for (const auto& item: tl.Items) {
        if (!item.Probe) {
            // NBS-492#5d45b57d701665001d0e946e
            continue;
        }

        const auto spanLength =
            CyclesToDurationSafe(item.TimestampCycles - minSeenTimestamp);

        item.Probe->Event.Signature.SerializeParams(
            item.Params,
            paramValues);

        writer.BeginList();
        {
            writer.WriteString(item.Probe->Event.Name);
            writer.WriteULongLong(spanLength.MicroSeconds());
            for (size_t i = 0; i < item.SavedParamsCount; ++i) {
                writer.WriteString(paramValues[i]);
            }
        }
        writer.EndList();
    }
    writer.BeginList();
    writer.WriteString(tag);
    writer.EndList();
    writer.EndList();
}

TString SerializeTraceToString(
    const NLWTrace::TTrackLog& tl,
    ui64 minSeenTimestamp,
    const TString& tag)
{
    TStringStream ss;
    NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE, &ss);

    SerializeTraceToJson(tl, minSeenTimestamp, tag, writer);

    return ss.Str();
}

////////////////////////////////////////////////////////////////////////////////

const NLWTrace::TParam* FindParam(
    const NLWTrace::TLogItem& logItem,
    const TStringBuf name)
{
    if (const auto* probe = logItem.Probe) {
        for (ui32 i = 0; i < probe->Event.Signature.ParamCount; ++i) {
            if (probe->Event.Signature.ParamNames[i] == name) {
                return &logItem.Params.Param[i];
            }
        }
    }

    return nullptr;
}

template <typename TResult>
    requires std::is_default_constructible_v<TResult>
TResult GetIdParam(const NLWTrace::TTrackLog::TItem& ringItem)
{
    if (const auto* diskId = FindParam(ringItem, NProbeParam::DiskId); diskId) {
        return diskId->Get<TResult>();
    }

    if (const auto* fsId = FindParam(ringItem, NProbeParam::FsId); fsId) {
        return fsId->Get<TResult>();
    }

    return TResult{};
}

template <typename TResult>
    requires std::is_default_constructible_v<TResult>
TResult GetIdParam(const TCgiParameters& cgiParams)
{
    if (auto diskId = cgiParams.Get(NProbeParam::DiskId); !diskId.empty()) {
        return diskId;
    }

    if (auto fsId = cgiParams.Get(NProbeParam::FsId); !fsId.empty()) {
        return fsId;
    }

    return TResult{};
}

void DisplayJsonErrorMessage(IOutputStream& out, const TString& message)
{
    out << "HTTP/1.1 400 Invalid Request\r\n"
           "Content-Type: application/json\r\n"
           "Connection: Close\r\n\r\n";
    out << "{\"status\": \"error\", \"message\": \"" << message << "\"}";
}

bool ReaderIdMatch(const TString& traceType, const TString& readerId)
{
    const TTraceKey key{traceType, readerId};
    return traceType.empty() ||
        key == TRACE_TYPE_SLOW ||
        key == TRACE_TYPE_RANDOM;
}

TVector<TTraceLog> PrepareTraceLogDump(
    ITraceProcessorPtr processor,
    const TString& traceType,
    const TString& id)
{
    TVector<TTraceLog> traceLogDump;

    processor->ForEach(
        [&traceType, &id, &traceLogDump](ITraceReaderPtr reader)
        {
            const auto& readerId = reader->Id;

            if (traceType && !ReaderIdMatch(traceType, readerId)) {
                return;
            }

            reader->ForEach(
                [&traceLogDump, &id, &readerId](const TEntry& item)
                {
                    const auto traceId =
                        GetIdParam<TString>(item.TrackLog.Items.front());

                    if (!traceId.empty() && (id.empty() || traceId == id)) {
                        traceLogDump.emplace_back(readerId, item);
                    }
                });
        });

    return traceLogDump;
}


void DumpTraceLogHtml(
    IOutputStream& out,
    const TVector<TTraceLog>& traceLogDump)
{
    HTML(out) {
        TABLE_SORTABLE() {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "Date"; }
                    TABLED() { out << "Id"; }
                    TABLED() { out << "Trace"; }
                    TABLED() { out << "Actions"; }
                }
            }
            TABLEBODY() {
                for (auto& [requestId, entry]: traceLogDump) {
                    TABLER() {
                        TABLED() { out << entry.Ts.ToStringLocalUpToSeconds(); }
                        TABLED() { out << requestId; }
                        TABLED() { out << SerializeTraceToString(entry.TrackLog, entry.Date, entry.Tag); }
                        TABLED() { out << ""; }
                    }
                }
            }
        }
    }
}

void DumpTraceLogJson(
    IOutputStream& out,
    const TVector<TTraceLog>& traceLogDump)
{
    TStringStream ss;
    NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE, &ss);

    writer.BeginList();
    for (const auto& [requestId, entry]: traceLogDump) {
        writer.BeginList();
        writer.WriteString(entry.Ts.ToStringLocalUpToSeconds());
        writer.WriteString(requestId);
        SerializeTraceToJson(entry.TrackLog, entry.Date, entry.Tag, writer);
        writer.EndList();
    }
    writer.EndList();

    out << ss.Str();
}

////////////////////////////////////////////////////////////////////////////////

class TTraceProcessorMon
    : public ITraceProcessor
{
private:
    class TMonPageHtml final
        : public THtmlMonPage
    {
    private:
        TTraceProcessorMon& TraceProcessor;
        const TString TraceType;

    public:
        TMonPageHtml(
                TTraceProcessorMon& traceProcessor,
                const TString& traceType,
                const TString& traceName)
            : THtmlMonPage(traceType, traceName, true)
            , TraceProcessor(traceProcessor)
            , TraceType(traceType)
        {}

        void OutputContent(IMonHttpRequest& request) override
        {
            TraceProcessor.OutputHtml(request.Output(), request, TraceType);
        }
    };

    class TMonPageJson final
        : public IMonPage
    {
    public:
        TTraceProcessorMon& TraceProcessor;

        TMonPageJson(TTraceProcessorMon& traceProcessor, const TString& path)
            : IMonPage(path)
            , TraceProcessor(traceProcessor)
        {}

        void Output(IMonHttpRequest& request) override
        {
            return TraceProcessor.OutputJson(request.Output(), request);
        }
    };

private:
    ITraceProcessorPtr Impl;

public:
    TTraceProcessorMon(
            IMonitoringServicePtr monitoring,
            ITraceProcessorPtr impl)
        : Impl(std::move(impl))
    {
        auto rootPage =
            monitoring->RegisterIndexPage("tracelogs", "Traces Logs");
        auto& index = static_cast<TIndexMonPage&>(*rootPage);
        index.Register(
            new TMonPageHtml(*this, randomName.data(), "Random samples"));
        index.Register(
            new TMonPageHtml(*this, slowName.data(), "Slow samples"));
        index.Register(new TMonPageJson(*this, jsonName.data()));
    }

    void Start() override
    {
        return Impl->Start();
    }

    void Stop() override
    {
        return Impl->Stop();
    }

    void ForEach(std::function<void(ITraceReaderPtr)> fn) override
    {
        return Impl->ForEach(std::move(fn));
    }

    void OutputHtml(
        IOutputStream& out,
        IMonHttpRequest& request,
        const TString& traceType)
    {
        const auto traceLogDump = PrepareTraceLogDump(
            Impl,
            traceType,
            GetIdParam<TString>(request.GetParams()));
        DumpTraceLogHtml(out, traceLogDump);
    }

    void OutputJson(IOutputStream& out, IMonHttpRequest& request)
    {
        const TString traceType = request.GetParams().Get("traceType");
        if (slowName != traceType && randomName != traceType &&
            !traceType.empty())
        {
            DisplayJsonErrorMessage(
                out,
                "Invalid traceType set '" + traceType + "'");
        }

        const auto traceLogDump = PrepareTraceLogDump(
            Impl,
            traceType,
            GetIdParam<TString>(request.GetParams()));

        out << HTTPOKJSON;
        DumpTraceLogJson(out, traceLogDump);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITraceProcessorPtr CreateTraceProcessorMon(
    IMonitoringServicePtr monitoring,
    ITraceProcessorPtr impl)
{
    return std::make_shared<TTraceProcessorMon>(
        std::move(monitoring),
        std::move(impl));
}

}   // namespace NCloud
