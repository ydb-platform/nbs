#include "profile_log.h"

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/eventlog/eventlog.h>

#include <util/datetime/cputimer.h>
#include <util/generic/hash.h>
#include <util/thread/lfstack.h>

namespace NCloud::NFileStore {

namespace {

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
    // TODO(proller): Remove debug after bug fix 
    STORAGE_VERIFY_DEBUG_C(record.FileSystemId.empty(), "", "", "Missing filesystem id in profile log");

    Records.Enqueue(std::move(record));
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

    if (records.size()) {
        TSelfFlushLogFrame logFrame(EventLog);
        THashMap<TString, TVector<ui32>> fsId2records;

        for (ui32 i = 0; i < records.size(); ++i) {
            fsId2records[records[i].FileSystemId].push_back(i);
        }

        for (auto& x: fsId2records) {
            NProto::TProfileLogRecord pb;
            pb.SetFileSystemId(x.first);
            for (const auto r: x.second) {
                auto& record = records[r];
                *pb.AddRequests() = std::move(record.Request);
            }

            logFrame.LogEvent(pb);
        }
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
