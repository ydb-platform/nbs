#include "transport_switcher.h"

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Owns itself for as long as an attempt is pending: every scheduled retry
// holds a strong reference, and once the router is gone nothing schedules any
// more.
class TTransportSwitcher final
    : public std::enable_shared_from_this<TTransportSwitcher>
{
private:
    const std::weak_ptr<IEndpointRouter> Router;
    const TEndpointFactory Factory;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const TString Host;

    TLog Log;
    TBackoffDelayProvider RetryDelay;

public:
    TTransportSwitcher(
            IEndpointRouterPtr router,
            TEndpointFactory factory,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            const ILoggingServicePtr& logging,
            TString host,
            const TTransportSwitcherConfig& config)
        : Router(std::move(router))
        , Factory(std::move(factory))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Host(std::move(host))
        , Log(logging->CreateLog("BLOCKSTORE_CELLS"))
        , RetryDelay(config.InitialRetryDelay, config.MaxRetryDelay)
    {}

    void Attempt()
    {
        if (Router.expired()) {
            return;
        }

        Factory().Subscribe(
            [self = shared_from_this()](const auto& future)
            { self->OnAttemptCompleted(future.GetValue()); });
    }

private:
    void OnAttemptCompleted(const TResultOrError<IBlockStorePtr>& result)
    {
        auto router = Router.lock();
        if (!router) {
            return;
        }

        if (!HasError(result) && result.GetResult()) {
            STORAGE_INFO(
                "[" << Host << "] switched over to the preferred transport");
            router->SetTarget(result.GetResult());
            return;
        }

        const auto delay = RetryDelay.GetDelayAndIncrease();

        STORAGE_WARN(
            "[" << Host << "] can't set up the preferred transport: "
                << FormatError(result.GetError()) << ", retrying in " << delay);

        Scheduler->Schedule(
            Timer->Now() + delay,
            [self = shared_from_this()] { self->Attempt(); });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void StartTransportSwitching(
    IEndpointRouterPtr router,
    TEndpointFactory factory,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    TString host,
    TTransportSwitcherConfig config)
{
    auto switcher = std::make_shared<TTransportSwitcher>(
        std::move(router),
        std::move(factory),
        std::move(timer),
        std::move(scheduler),
        logging,
        std::move(host),
        config);

    switcher->Attempt();
}

}   // namespace NCloud::NBlockStore::NCells
