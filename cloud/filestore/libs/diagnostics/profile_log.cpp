#include "profile_log.h"

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/eventlog/eventlog.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/cputimer.h>
#include <util/generic/array_ref.h>
#include <util/generic/hash.h>
#include <util/thread/lfstack.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TProfileLogCounters
{
    using TCounterPtr = NMonitoring::TDynamicCounters::TCounterPtr;

    struct TFlushStats
    {
        TInstant StartedTs;
        ui64 ProfileLogRecordProtoBytes = 0;
        ui64 CompressedBytes = 0;
    };

private:
    TCounterPtr EnqueuedRecords;

    TCounterPtr FlushCount;
    TCounterPtr LastFlushIntervalUs;
    TCounterPtr LastFlushDurationUs;

    TCounterPtr DequeuedRecords;
    TCounterPtr DiscardedRequests;

    TCounterPtr EventLogCompressedBytes;
    TCounterPtr ProfileLogRecordProtoBytes;
    TCounterPtr LastFlushCompressedBytes;

    // Updated only from the Flush path, which is expected to be single
    // threaded.
    TInstant LastFlushTs;

public:
    void Register(NMonitoring::TDynamicCountersPtr counters)
    {
        if (!counters) {
            return;
        }

        EnqueuedRecords =
            counters->GetCounter("ProfileLog/EnqueuedRecords", true);

        FlushCount = counters->GetCounter("ProfileLog/FlushCount", true);
        LastFlushIntervalUs =
            counters->GetCounter("ProfileLog/LastFlushIntervalUs", false);
        LastFlushDurationUs =
            counters->GetCounter("ProfileLog/LastFlushDurationUs", false);

        DequeuedRecords =
            counters->GetCounter("ProfileLog/DequeuedRecords", true);
        DiscardedRequests =
            counters->GetCounter("ProfileLog/DiscardedRequests", true);

        EventLogCompressedBytes =
            counters->GetCounter("ProfileLog/EventLogCompressedBytes", true);
        ProfileLogRecordProtoBytes =
            counters->GetCounter("ProfileLog/ProfileLogRecordProtoBytes", true);
        LastFlushCompressedBytes =
            counters->GetCounter("ProfileLog/LastFlushCompressedBytes", false);
    }

    bool IsEnabled() const
    {
        return !!EnqueuedRecords;
    }

    void OnRecordEnqueued();
    TFlushStats OnFlushStarted(ITimer& timer);
    void OnRecordsDequeued(ui64 dequeuedRecords);
    void OnRequestsDiscarded(ui64 discardedRequestCount);
    void OnProfileLogRecordBuilt(
        TFlushStats& flushStats,
        const NProto::TProfileLogRecord& record);
    TWriteFrameCallbackPtr CreateFrameCallback(TFlushStats& flushStats);
    void OnFrameCompressed(TFlushStats& flushStats, ui64 compressedBytes);
    void OnFlushFinished(ITimer& timer, const TFlushStats& flushStats);
};

////////////////////////////////////////////////////////////////////////////////

class TFrameStatsCallback final
    : public IWriteFrameCallback
{
private:
    TProfileLogCounters* Counters = nullptr;
    TProfileLogCounters::TFlushStats* FlushStats = nullptr;

public:
    TFrameStatsCallback(
            TProfileLogCounters* counters,
            TProfileLogCounters::TFlushStats* flushStats)
        : Counters(counters)
        , FlushStats(flushStats)
    {}

    void OnAfterCompress(
        const TBuffer& compressedFrame,
        TEventTimestamp startTimestamp,
        TEventTimestamp endTimestamp) override
    {
        Y_UNUSED(startTimestamp);
        Y_UNUSED(endTimestamp);

        const ui64 frameSize = compressedFrame.Size();

        Counters->OnFrameCompressed(*FlushStats, frameSize);
    }
};

void TProfileLogCounters::OnRecordEnqueued()
{
    if (!IsEnabled()) {
        return;
    }

    EnqueuedRecords->Add(1);
}

TProfileLogCounters::TFlushStats TProfileLogCounters::OnFlushStarted(
    ITimer& timer)
{
    TFlushStats flushStats;

    if (!IsEnabled()) {
        return flushStats;
    }

    flushStats.StartedTs = timer.Now();
    FlushCount->Add(1);

    if (LastFlushTs) {
        LastFlushIntervalUs->Set(
            (flushStats.StartedTs - LastFlushTs).MicroSeconds());
    }
    LastFlushTs = flushStats.StartedTs;

    return flushStats;
}

void TProfileLogCounters::OnRecordsDequeued(ui64 dequeuedRecords)
{
    if (!IsEnabled() || !dequeuedRecords) {
        return;
    }

    DequeuedRecords->Add(dequeuedRecords);
}

void TProfileLogCounters::OnRequestsDiscarded(ui64 discardedRequestCount)
{
    if (!IsEnabled() || !discardedRequestCount) {
        return;
    }

    DiscardedRequests->Add(discardedRequestCount);
}

void TProfileLogCounters::OnProfileLogRecordBuilt(
    TFlushStats& flushStats,
    const NProto::TProfileLogRecord& record)
{
    if (!IsEnabled()) {
        return;
    }

    flushStats.ProfileLogRecordProtoBytes += record.ByteSizeLong();
}

TWriteFrameCallbackPtr TProfileLogCounters::CreateFrameCallback(
    TFlushStats& flushStats)
{
    if (!IsEnabled()) {
        return {};
    }

    return MakeIntrusive<TFrameStatsCallback>(this, &flushStats);
}

void TProfileLogCounters::OnFrameCompressed(
    TFlushStats& flushStats,
    ui64 compressedBytes)
{
    if (!IsEnabled()) {
        return;
    }

    EventLogCompressedBytes->Add(compressedBytes);
    flushStats.CompressedBytes += compressedBytes;
}

void TProfileLogCounters::OnFlushFinished(
    ITimer& timer,
    const TFlushStats& flushStats)
{
    if (!IsEnabled()) {
        return;
    }

    ProfileLogRecordProtoBytes->Add(flushStats.ProfileLogRecordProtoBytes);

    LastFlushCompressedBytes->Set(flushStats.CompressedBytes);

    const ui64 flushDurationUs =
        (timer.Now() - flushStats.StartedTs).MicroSeconds();
    LastFlushDurationUs->Set(flushDurationUs);
}

////////////////////////////////////////////////////////////////////////////////

class TProfileLog final
    : public IProfileLog
    , public std::enable_shared_from_this<TProfileLog>
{
private:
    TEventLog EventLog;
    TProfileLogSettings Settings;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;

    TLockFreeStack<TRecord> Records;

    TProfileLogCounters Counters;

    TAtomic ShouldStop = false;

public:
    TProfileLog(
            TProfileLogSettings settings,
            ITimerPtr timer,
            ISchedulerPtr scheduler)
        : EventLog(settings.FilePath, NEvClass::Factory()->CurrentFormat())
        , Settings(std::move(settings))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
    {
    }

    ~TProfileLog() override;

public:
    void Start() override;
    void Stop() override;

    void SetupCounters(NMonitoring::TDynamicCountersPtr counters) override;
    void Write(TRecord record) override;

private:
    void ScheduleFlush();
    void Flush();
};

TProfileLog::~TProfileLog()
{
    Flush();
}

void TProfileLog::Start()
{
    ScheduleFlush();
}

void TProfileLog::Stop()
{
    AtomicSet(ShouldStop, 1);
}

void TProfileLog::SetupCounters(NMonitoring::TDynamicCountersPtr counters)
{
    Counters.Register(std::move(counters));
}

void TProfileLog::Write(TRecord record)
{
    Records.Enqueue(std::move(record));
    Counters.OnRecordEnqueued();
}

void TProfileLog::ScheduleFlush()
{
    if (AtomicGet(ShouldStop)) {
        return;
    }

    Scheduler->Schedule(
        Timer->Now() + Settings.TimeThreshold,
        [weakPtr = weak_from_this()] {
            if (auto self = weakPtr.lock()) {
                self->Flush();
                self->ScheduleFlush();
            }
        }
    );
}

void TProfileLog::Flush()
{
    auto flushStats = Counters.OnFlushStarted(*Timer);

    TVector<TRecord> records;
    Records.DequeueAllSingleConsumer(&records);

    const ui64 dequeuedRecords = records.size();
    Counters.OnRecordsDequeued(dequeuedRecords);

    ui64 discardedRequestCount = 0;
    if (Settings.MaxFlushRecords && records.size() > Settings.MaxFlushRecords) {
        discardedRequestCount = records.size() - Settings.MaxFlushRecords;
        records.resize(Settings.MaxFlushRecords);
    }
    Counters.OnRequestsDiscarded(discardedRequestCount);

    auto recordsRef = MakeArrayRef(records);
    size_t frameRecordsOffset = 0;
    size_t maxFrameRecords = Settings.MaxFrameFlushRecords
                                 ? Settings.MaxFrameFlushRecords
                                 : records.size();

    while (frameRecordsOffset < recordsRef.size()) {
        auto frameRecords = recordsRef.Slice(
            frameRecordsOffset,
            std::min(maxFrameRecords, recordsRef.size() - frameRecordsOffset));

        TSelfFlushLogFrame logFrame(
            EventLog,
            false,
            Counters.CreateFrameCallback(flushStats));
        THashMap<TString, TVector<ui32>> fsId2records;

        for (ui32 i = 0; i < frameRecords.size(); ++i) {
            fsId2records[frameRecords[i].FileSystemId].push_back(i);
        }

        for (auto& x: fsId2records) {
            NProto::TProfileLogRecord pb;

            if (discardedRequestCount) {
                pb.SetDiscardedRequestCount(discardedRequestCount);
                discardedRequestCount = 0;
            }

            pb.SetFileSystemId(x.first);
            for (const auto r: x.second) {
                auto& record = frameRecords[r];
                *pb.AddRequests() = std::move(record.Request);
            }

            Counters.OnProfileLogRecordBuilt(flushStats, pb);

            logFrame.LogEvent(pb);
        }

        frameRecordsOffset += frameRecords.size();
    }

    EventLog.Flush();
    Counters.OnFlushFinished(*Timer, flushStats);
}

////////////////////////////////////////////////////////////////////////////////

class TProfileLogStub final
    : public IProfileLog
{
public:
    void Start() override
    {
    }

    void Stop() override
    {
    }

    void SetupCounters(NMonitoring::TDynamicCountersPtr counters) override
    {
        Y_UNUSED(counters);
    }

    void Write(TRecord record) override
    {
        Y_UNUSED(record);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IProfileLogPtr CreateProfileLog(
    TProfileLogSettings settings,
    ITimerPtr timer,
    ISchedulerPtr scheduler)
{
    return std::make_shared<TProfileLog>(
        std::move(settings),
        std::move(timer),
        std::move(scheduler)
    );
}

IProfileLogPtr CreateProfileLogStub()
{
    return std::make_shared<TProfileLogStub>();
}

}   // namespace NCloud::NFileStore
