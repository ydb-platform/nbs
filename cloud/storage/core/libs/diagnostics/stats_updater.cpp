#include "stats_updater.h"

#include "incomplete_request_processor.h"

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/deprecated/atomic/atomic.h>

#include <memory>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TStatsUpdater
    : public IStatsUpdater
    , public std::enable_shared_from_this<TStatsUpdater>
{
protected:
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const IIncompleteRequestProcessorPtr Collector;

    TAtomic ShouldStop = 0;

    size_t UpdateStatsCounter = 0;

public:
    TStatsUpdater(
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        IIncompleteRequestProcessorPtr collector);

    void Start() override;
    void Stop() override;

private:
    void UpdateStats();
    void ScheduleUpdateStats();
};

////////////////////////////////////////////////////////////////////////////////

TStatsUpdater::TStatsUpdater(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IIncompleteRequestProcessorPtr collector)
    : Timer(std::move(timer))
    , Scheduler(std::move(scheduler))
    , Collector(std::move(collector))
{}

void TStatsUpdater::Start()
{
    ScheduleUpdateStats();
}

void TStatsUpdater::Stop()
{
    AtomicSet(ShouldStop, 1);
}

void TStatsUpdater::UpdateStats()
{
    bool updateIntervalFinished =
        (++UpdateStatsCounter % UpdateCountersInterval.Seconds() == 0);

    Collector->UpdateStats(updateIntervalFinished);
}

void TStatsUpdater::ScheduleUpdateStats()
{
    if (AtomicGet(ShouldStop)) {
        return;
    }

    auto weak_ptr = weak_from_this();

    Scheduler->Schedule(
        Timer->Now() + UpdateStatsInterval,
        [weak_ptr = std::move(weak_ptr)]
        {
            if (auto p = weak_ptr.lock()) {
                p->UpdateStats();
                p->ScheduleUpdateStats();
            }
        });
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStatsUpdaterPtr CreateStatsUpdater(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IIncompleteRequestProcessorPtr collector)
{
    return std::make_shared<TStatsUpdater>(
        std::move(timer),
        std::move(scheduler),
        std::move(collector));
}

}   // namespace NCloud
