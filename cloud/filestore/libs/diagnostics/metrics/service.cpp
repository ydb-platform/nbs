#include "service.h"

#include "label.h"
#include "registry.h"

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <atomic>

namespace NCloud::NFileStore::NMetrics {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMetricsService
    : public IMetricsService
    , public std::enable_shared_from_this<TMetricsService>
{
private:
    const TMetricsServiceConfig Config;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;

    IMainMetricsRegistryPtr Registry{nullptr};
    std::atomic_flag ShouldStop{false};

public:
    TMetricsService(
        const TMetricsServiceConfig& config,
        ITimerPtr timer,
        ISchedulerPtr scheduler)
        : Config(config)
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
    {}

    // IStartable
    void Start() override
    {
        Y_ABORT_UNLESS(Registry);

        ScheduleUpdate();
    }

    void Stop() override
    {
        ShouldStop.test_and_set(std::memory_order_release);
    }

    // IMetricsService
    void SetupCounters(NMonitoring::TDynamicCountersPtr root) override
    {
        Y_ABORT_UNLESS(!Registry);

        Registry = CreateMetricsRegistry(
            {CreateLabel("counters", "filestore")},
            std::move(root));
    }

    IMetricsRegistryPtr GetRegistry() const override
    {
        return Registry;
    }

private:
    template <typename TFunction>
    void ScheduleFunction(TDuration duration, TFunction&& function)
    {
        if (ShouldStop.test(std::memory_order_acquire)) {
            return;
        }

        Scheduler->Schedule(
            Timer->Now() + duration,
            [func = std::forward<TFunction>(function),
             weakPtr = weak_from_this()]() mutable
            {
                auto thisPtr = weakPtr.lock();
                if (!thisPtr) {
                    return;
                }

                std::invoke(std::move(func), thisPtr.get());
            });
    }

    void ScheduleUpdate()
    {
        Registry->Update(Timer->Now());

        ScheduleFunction(
            Config.UpdateInterval,
            &TMetricsService::ScheduleUpdate);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IMetricsServicePtr CreateMetricsService(
    const TMetricsServiceConfig& config,
    ITimerPtr timer,
    ISchedulerPtr scheduler)
{
    return std::make_shared<TMetricsService>(
        config,
        std::move(timer),
        std::move(scheduler));
}

}   // namespace NCloud::NFileStore::NMetrics
