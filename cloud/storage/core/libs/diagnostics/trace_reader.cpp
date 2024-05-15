#include "trace_reader.h"

#include "logging.h"

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/protos/media.pb.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/lwtrace/control.h>
#include <library/cpp/lwtrace/log.h>

#include <util/datetime/cputimer.h>
#include <util/generic/hash.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/stream/str.h>

namespace NCloud {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TTraceKey = std::pair<TStringBuf, TStringBuf>;
using TTraceLog = std::pair<TStringBuf, TEntry>;

const TStringBuf slowName = "slow";
const TStringBuf randomName = "random";

const TTraceKey TRACE_TYPE_SLOW{slowName, "st_slow_requests_filter"};
const TTraceKey TRACE_TYPE_RANDOM{randomName, "st_trace_logger"};

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
    if (auto* diskId = FindParam(ringItem, NProbeParam::DiskId); diskId) {
        return diskId->Get<TResult>();
    } else if (auto* fsId = FindParam(ringItem, NProbeParam::FsId); fsId) {
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
    } else if (auto fsId = cgiParams.Get(NProbeParam::FsId); !fsId.empty()) {
        return fsId;
    }

    return TResult{};
}

[[maybe_unused]] TString DumpLogItem(const NLWTrace::TLogItem& logItem)
{
    TStringStream ss;
    if (const auto* probe = logItem.Probe) {
        ss << "probe: " << probe->Event.Name << ", params: " ;
        for (ui32 i = 0; i < probe->Event.Signature.ParamCount; ++i) {
            ss << probe->Event.Signature.ParamNames[i] << "|";
        }
    } else {
        ss << "no probe attached";
    }

    if (ss.Str().EndsWith("|")) {
        ss.Str().pop_back();
    }

    return std::move(ss.Str());
}

void DisplayJsonErrorMessage(IOutputStream& out, const TString& message) {
    out <<  "HTTP/1.1 400 Invalid Request\r\n"
            "Content-Type: application/json\r\n"
            "Connection: Close\r\n\r\n";
    out << "{\"status\": \"error\", \"message\": \"" << message << "\"}";
}

void SerializeTraceEntry(
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

TString SerializeTrace(
    const NLWTrace::TTrackLog& tl,
    ui64 minSeenTimestamp,
    const TString& tag)
{
    TStringStream ss;
    NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE, &ss);

    SerializeTraceEntry(tl, minSeenTimestamp, tag, writer);

    return ss.Str();
}


bool ReaderIdMatch(const TString& traceType, const TString& readerId)
{
    const TTraceKey key{traceType, readerId};
    return traceType.empty() || key == TRACE_TYPE_SLOW || key == TRACE_TYPE_RANDOM;
}

TVector<TTraceLog> PrepareTraceLogDump(
    const TVector<ITraceReaderPtr>& readers,
    const TString& traceType,
    const TString& id)
{
    TVector<TTraceLog> traceLogDump;

    for (auto& reader: readers) {
        const auto& readerId = reader->Id;

        if (traceType && !ReaderIdMatch(traceType, readerId)) {
            continue;
        }

        reader->ForEachTraceLog([&] (const auto& item) {
            const auto traceId = GetIdParam<TString>(item.TrackLog.Items.front());

            if (!traceId.empty() && (id.empty() || traceId == id)) {
                traceLogDump.emplace_back(readerId, item);
            }
        });
    }

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
                        TABLED() { out << SerializeTrace(entry.TrackLog, entry.Date, entry.Tag); }
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
    for (auto& [requestId, entry]: traceLogDump) {
        writer.BeginList();
        writer.WriteString(entry.Ts.ToStringLocalUpToSeconds());
        writer.WriteString(requestId);
        SerializeTraceEntry(entry.TrackLog, entry.Date, entry.Tag, writer);
        writer.EndList();
    }
    writer.EndList();

    out << ss.Str();
}

////////////////////////////////////////////////////////////////////////////////

struct TTraceReaderWithRingBuffer
    : public ITraceReader
{
    TSimpleRingBuffer<TEntry> RingBuffer;

    TTraceReaderWithRingBuffer(TString id)
        : ITraceReader(std::move(id))
        , RingBuffer(1000)
    {}

    void ForEachTraceLog(std::function<void (const TEntry&)> fn) override
    {
        for (ui64 i = RingBuffer.FirstIndex(); i < RingBuffer.TotalSize(); ++i) {
            fn(RingBuffer[i]);
        }
    }
};

class TTraceLogger final
    : public TTraceReaderWithRingBuffer
{
private:
    TLog Log;
    ui64 TracksCount = 0;

public:
    TTraceLogger(TString id, ILoggingServicePtr logging, TString componentName)
        : TTraceReaderWithRingBuffer(std::move(id))
    {
        Log = logging->CreateLog(std::move(componentName));
    }

    void Push(TThread::TId tid, const NLWTrace::TTrackLog& tl) override
    {
        Y_UNUSED(tid);

        if (tl.Items.empty() || ++TracksCount > DumpTracksLimit) {
            return;
        }

        ui64 minSeenTimestamp = tl.Items[0].TimestampCycles;

        Log.Write(
            ELogPriority::TLOG_INFO,
            SerializeTrace(tl, minSeenTimestamp, "AllRequests"));

        RingBuffer.PushBack(
            {TInstant::Now(), minSeenTimestamp, tl, "AllRequests"}
        );
    }

    void Reset() override
    {
        TracksCount = 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSlowRequestsFilter final
    : public TTraceReaderWithRingBuffer
{
private:
    const TString Id;
    TLog Log;

    TRequestThresholds RequestThresholds;

    ui64 TracksCount = 0;

    THashMap<TString, ui64> SeenStartProbes;

public:
    TSlowRequestsFilter(
            TString id,
            ILoggingServicePtr logging,
            TString componentName,
            TRequestThresholds requestThresholds)
        : TTraceReaderWithRingBuffer(id)
        , Id(std::move(id))
        , RequestThresholds(std::move(requestThresholds))
    {
        Log = logging->CreateLog(std::move(componentName));
    }

    void Push(TThread::TId tid, const NLWTrace::TTrackLog& tl) override
    {
        Y_UNUSED(tid);

        if (tl.Items.empty()) {
            return;
        }

        // first pass to measure track length
        ui64 maxSeenTimestamp = 0;
        ui64 minSeenTimestamp = tl.Items[0].TimestampCycles;

        for (const auto& item: tl.Items) {
            // item.Timestamp is usually 0
            const auto ts = item.TimestampCycles;
            maxSeenTimestamp = Max(maxSeenTimestamp, ts);
        }

        using namespace NProbeParam;

        const auto* mediaKindParam = FindParam(tl.Items.front(), MediaKind);
        const auto* requestTypeParam = FindParam(tl.Items.front(), RequestType);

        if (tl.Items.front().Probe) {
            ++SeenStartProbes[tl.Items.front().Probe->Event.Name];
        }

        Y_DEBUG_ABORT_UNLESS(mediaKindParam, "expected to find mediaKind at start: %s",
            DumpLogItem(tl.Items.front()).c_str());

        if (!mediaKindParam) {
            return;
        }

        auto mediaKind = mediaKindParam->Get<NProto::EStorageMediaKind>();

        const auto* requestSizeParam = FindParam(tl.Items.front(), RequestSize);

        auto srt = GetThresholdByRequestType(
            mediaKind,
            RequestThresholds,
            requestTypeParam,
            requestSizeParam);

        const auto* executionTimeParam = FindParam(
            tl.Items.back(), RequestExecutionTime);

        TDuration trackLength;
        if (executionTimeParam) {
            trackLength = TDuration::MicroSeconds(
                executionTimeParam->Get<ui64>());
        } else {
            trackLength =
                CyclesToDurationSafe(maxSeenTimestamp - minSeenTimestamp);
        }

        if (trackLength >= srt && ++TracksCount <= DumpTracksLimit) {
            Log.Write(
                ELogPriority::TLOG_WARNING,
                SerializeTrace(tl, minSeenTimestamp, "SlowRequests"));

            RingBuffer.PushBack(
                {TInstant::Now(), minSeenTimestamp, tl, "SlowRequests"}
            );
        }
    }

    void Reset() override
    {
        TStringBuilder out;
        out << "Filter: " << Id << " stats";
        for (auto& p: SeenStartProbes) {
            out << '[' << p.first << ',' << p.second << ']';
            p.second = 0;
        }
        Log.Write(ELogPriority::TLOG_DEBUG, out);
        TracksCount = 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTraceProcessor final
    : public ITraceProcessor
    , public std::enable_shared_from_this<TTraceProcessor>
{
    class TMonPageHtml final
        : public THtmlMonPage
    {
    private:
        TTraceProcessor& TraceProcessor;
        const TString TraceType;

    public:
        TMonPageHtml(
                TTraceProcessor& traceProcessor,
                const TString& traceType,
                const TString& traceName)
            : THtmlMonPage(traceType, traceName, true)
            , TraceProcessor(traceProcessor)
            , TraceType(traceType)
        {
        }

        void OutputContent(IMonHttpRequest& request) override
        {
            TraceProcessor.OutputHtml(request.Output(), request, TraceType);
        }
    };

    class TMonPageJson final
        : public IMonPage
    {
    public:
        TTraceProcessor& TraceProcessor;

        TMonPageJson(
                TTraceProcessor& traceProcessor,
                const TString& path)
            : IMonPage(path)
            , TraceProcessor(traceProcessor)
        {}

        void Output(IMonHttpRequest& request) override
        {
            return TraceProcessor.OutputJson(request.Output(), request);
        }
    };

private:
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    const TString ComponentName;
    NLWTrace::TManager& LWManager;
    TVector<ITraceReaderPtr> Readers;

    TLog Log;
    TAtomic ShouldStop = 0;

public:
    TTraceProcessor(
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring,
            TString componentName,
            NLWTrace::TManager& lwManager,
            TVector<ITraceReaderPtr> readers)
        : Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , ComponentName(std::move(componentName))
        , LWManager(lwManager)
        , Readers(std::move(readers))
    {
        Y_DEBUG_ABORT_UNLESS(Readers.size());

        auto rootPage = monitoring->RegisterIndexPage("tracelogs", "Traces Logs");
        auto& index = static_cast<TIndexMonPage&>(*rootPage);
        index.Register(new TMonPageHtml(*this, randomName.Data(), "Random samples"));
        index.Register(new TMonPageHtml(*this, slowName.Data(), "Slow samples"));
        index.Register(new TMonPageJson(*this, "json"));
    }

    void Start() override
    {
        Log = Logging->CreateLog(ComponentName);
        ScheduleProcessLWDepot();
    }

    void Stop() override
    {
        AtomicSet(ShouldStop, 1);
    }

private:
    void ScheduleProcessLWDepot()
    {
        if (AtomicGet(ShouldStop)) {
            return;
        }

        auto weak_ptr = weak_from_this();

        Scheduler->Schedule(
            Timer->Now() + DumpTracksInterval,
            [weak_ptr = std::move(weak_ptr)] {
                if (auto p = weak_ptr.lock()) {
                    p->ProcessLWDepot();
                    p->ScheduleProcessLWDepot();
                }
            });
    }

    void ProcessLWDepot()
    {
        for (auto& reader: Readers) {
            try {
                reader->Reset();
                LWManager.ExtractItemsFromCyclicDepot(reader->Id, *reader);
            } catch (...) {
                STORAGE_ERROR("Tracing error: " << CurrentExceptionMessage());
                Y_DEBUG_ABORT_UNLESS(0);
            }
        }
    }

    void OutputHtml(
        IOutputStream& out,
        IMonHttpRequest& request,
        const TString& traceType)
    {
        const auto traceLogDump = PrepareTraceLogDump(
            Readers, traceType, GetIdParam<TString>(request.GetParams())
        );
        DumpTraceLogHtml(out, traceLogDump);
    }

    void OutputJson(IOutputStream& out, IMonHttpRequest& request)
    {
        const TString traceType = request.GetParams().Get("traceType");
        if (slowName != traceType &&
            randomName != traceType &&
            !traceType.empty())
        {
            DisplayJsonErrorMessage(
                out, "Invalid traceType set '" + traceType + "'"
            );
        }

        const auto traceLogDump = PrepareTraceLogDump(
            Readers, traceType, GetIdParam<TString>(request.GetParams())
        );

        out << HTTPOKJSON;
        DumpTraceLogJson(out, traceLogDump);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTraceProcessorStub final
    : public ITraceProcessor
{
    void Start() override
    {}

    void Stop() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITraceProcessorPtr CreateTraceProcessor(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TString componentName,
    NLWTrace::TManager& lwManager,
    TVector<ITraceReaderPtr> readers)
{
    return std::make_shared<TTraceProcessor>(
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(monitoring),
        std::move(componentName),
        lwManager,
        std::move(readers));
}

ITraceProcessorPtr CreateTraceProcessorStub()
{
    return std::make_shared<TTraceProcessorStub>();
}

ITraceReaderPtr CreateTraceLogger(
    TString id,
    ILoggingServicePtr logging,
    TString componentName)
{
    return std::make_shared<TTraceLogger>(
        std::move(id),
        std::move(logging),
        std::move(componentName));
}

ITraceReaderPtr CreateSlowRequestsFilter(
    TString id,
    ILoggingServicePtr logging,
    TString componentName,
    TRequestThresholds requestThresholds)
{
    return std::make_shared<TSlowRequestsFilter>(
        std::move(id),
        std::move(logging),
        std::move(componentName),
        std::move(requestThresholds));
}

NLWTrace::TQuery ProbabilisticQuery(
    const TVector<std::tuple<TString, TString>>& probes,
    ui32 samplingRate)
{
    return ProbabilisticQuery(probes, samplingRate, 0);
}

NLWTrace::TQuery ProbabilisticQuery(
    const TVector<std::tuple<TString, TString>>& probes,
    ui32 samplingRate,
    ui32 shuttleCount)
{
    NLWTrace::TQuery q;

    Y_DEBUG_ABORT_UNLESS(samplingRate > 0);
    if (samplingRate == 0) {
        return q;
    }

    for (const auto& x: probes) {
        auto* block = q.AddBlocks();
        block->MutableProbeDesc()->SetName(std::get<0>(x));
        block->MutableProbeDesc()->SetProvider(std::get<1>(x));
        auto& action = *block->AddAction()->MutableRunLogShuttleAction();
        action.SetMaxTrackLength(1000);
        if (shuttleCount) {
            action.SetShuttlesCount(shuttleCount);
        }

        auto* predicate = block->MutablePredicate();
        predicate->SetSampleRate(1.0 / samplingRate);
    }

    return q;
}

////////////////////////////////////////////////////////////////////////////////

TRequestThresholds ConvertRequestThresholds(
    const TProtoRequestThresholds& value)
{
    TRequestThresholds requestThresholds;
    for (const auto& threshold: value) {
        TLWTraceThreshold requestThreshold;
        requestThreshold.Default = TDuration::MilliSeconds(
            threshold.GetDefault());
        requestThreshold.PerSizeUnit = TDuration::MilliSeconds(
            threshold.GetPerSizeUnit());
        for (const auto& [rType, typeThresh]: threshold.GetByRequestType()) {
            requestThreshold.ByRequestType[rType] = \
                TDuration::MilliSeconds(typeThresh);
        }
        const auto mediaKind = threshold.GetMediaKind();
        Y_DEBUG_ABORT_UNLESS(!requestThresholds.count(mediaKind));
        requestThresholds[mediaKind] = std::move(requestThreshold);
    }
    return requestThresholds;
}

void OutRequestThresholds(
    IOutputStream& out,
    const NCloud::TRequestThresholds& value)
{
    for (const auto& [mediaKind, tr]: value) {
        NCloud::NProto::TLWTraceThreshold protoTr;
        protoTr.SetMediaKind(
            static_cast<NCloud::NProto::EStorageMediaKind>(mediaKind));
        protoTr.SetDefault(tr.Default.MilliSeconds());
        protoTr.SetPerSizeUnit(tr.PerSizeUnit.MilliSeconds());
        auto& protoByRequestType = *protoTr.MutableByRequestType();
        for (const auto& [requestType, duration]: tr.ByRequestType) {
            protoByRequestType[requestType] = duration.MilliSeconds();
        }
        SerializeToTextFormat(protoTr, out);
    }
}

TDuration GetThresholdByRequestType(
    const NProto::EStorageMediaKind mediaKind,
    const TRequestThresholds& requestThresholds,
    const NLWTrace::TParam* requestTypeParam,
    const NLWTrace::TParam* requestSizeParam)
{
    TDuration srt;
    auto mediaKindThresholdsIt = requestThresholds.find(mediaKind);
    if (mediaKindThresholdsIt == requestThresholds.end()) {
        mediaKindThresholdsIt = requestThresholds.find(
            NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT);
    }

    if (mediaKindThresholdsIt != requestThresholds.end()) {
        const auto& mediaKindThresholds = mediaKindThresholdsIt->second;
        srt = mediaKindThresholds.Default;
        if (requestTypeParam) {
            auto requestType = requestTypeParam->Get<TString>();
            const auto threshold =
                mediaKindThresholds.ByRequestType.find(requestType);
            if (threshold != mediaKindThresholds.ByRequestType.end()) {
                srt = Max(srt, threshold->second);
            }
        }
        if (requestSizeParam) {
            auto requestSize = requestSizeParam->Get<ui64>();
            srt += mediaKindThresholds.PerSizeUnit * requestSize / 1_KB;
        }
    }
    return srt;
}

}   // namespace NCloud
