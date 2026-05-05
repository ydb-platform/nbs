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

using namespace NMonitoring;

class TProfileLog final
    : public IProfileLog
    , public std::enable_shared_from_this<TProfileLog>
{
private:
    struct TCounters
    {
        TDynamicCounters::TCounterPtr Requests;
        TDynamicCounters::TCounterPtr Bytes;
        TDynamicCounters::TCounterPtr Flushes;
        TDynamicCounters::TCounterPtr Discards;
    };

private:
    TEventLog EventLog;
    TProfileLogSettings Settings;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    TCounters Counters;
    bool CountersRegistered = false;

    TLockFreeStack<TRecord> Records;

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

    void Write(TRecord record) override;
    void RegisterCounters(NMonitoring::TDynamicCounters& root) override;

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

void TProfileLog::Write(TRecord record)
{
    if (CountersRegistered) {
        Counters.Requests->Inc();
        Counters.Bytes->Add(static_cast<i64>(record.Request.ByteSizeLong()));
    }

    Records.Enqueue(std::move(record));
}

void TProfileLog::RegisterCounters(NMonitoring::TDynamicCounters& root)
{
    auto pg = root.GetSubgroup("component", "profile_log");
    Counters.Requests = pg->GetCounter("Count", true);
    Counters.Bytes = pg->GetCounter("RequestBytes", true);
    Counters.Flushes = pg->GetCounter("FlushCount", true);
    Counters.Discards = pg->GetCounter("DiscardCount", true);
    CountersRegistered = true;
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
    TVector<TRecord> records;
    Records.DequeueAllSingleConsumer(&records);

    ui64 discardedRequestCount = 0;
    if (Settings.MaxFlushRecords && records.size() > Settings.MaxFlushRecords) {
        discardedRequestCount = records.size() - Settings.MaxFlushRecords;
        records.resize(Settings.MaxFlushRecords);
    }

    auto recordsRef = MakeArrayRef(records);
    size_t frameRecordsOffset = 0;
    size_t maxFrameRecords = Settings.MaxFrameFlushRecords
                                 ? Settings.MaxFrameFlushRecords
                                 : records.size();

    while (frameRecordsOffset < recordsRef.size()) {
        auto frameRecords = recordsRef.Slice(
            frameRecordsOffset,
            std::min(maxFrameRecords, recordsRef.size() - frameRecordsOffset));

        TSelfFlushLogFrame logFrame(EventLog);
        THashMap<TString, TVector<ui32>> fsId2records;

        for (ui64 i = 0; i < frameRecords.size(); ++i) {
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

            logFrame.LogEvent(pb);
        }

        frameRecordsOffset += frameRecords.size();
    }

    if (CountersRegistered) {
        Counters.Flushes->Inc();
        Counters.Discards->Add(static_cast<i64>(discardedRequestCount));
    }

    EventLog.Flush();
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

    void Write(TRecord record) override
    {
        Y_UNUSED(record);
    }

    void RegisterCounters(NMonitoring::TDynamicCounters& root) override
    {
        Y_UNUSED(root);
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
