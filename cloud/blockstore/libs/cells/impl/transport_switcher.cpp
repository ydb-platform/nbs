#include "transport_switcher.h"

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/string/builder.h>
#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Keeps the router pointing at the transport that can serve data right now: the
// fallback until the preferred endpoint is acquired and settled, and back onto
// the fallback the moment that endpoint breaks.
//
// Owns itself for as long as an attempt is pending: every scheduled retry holds
// a strong reference, and once the router is gone nothing schedules any more.
class TTransportSwitcher final
    : public ITransportSwitcher
    , public std::enable_shared_from_this<TTransportSwitcher>
{
private:
    const std::weak_ptr<IEndpointRouter> Router;
    const IBlockStorePtr Fallback;   // the endpoint the router started with
    const TEndpointFactory Factory;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const TString Host;
    const TDuration SettleTime;

    TLog Log;
    TBackoffDelayProvider RetryDelay;

    TAdaptiveLock Lock;
    IBlockStorePtr Preferred;       // the rdma endpoint, once acquired
    bool PreferredActive = false;   // is the router pointing at it
    bool Connected = false;
    bool EverActive = false;
    ui64 SettleGeneration = 0;

public:
    TTransportSwitcher(
            IEndpointRouterPtr router,
            IBlockStorePtr fallback,
            TEndpointFactory factory,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            const ILoggingServicePtr& logging,
            TString host,
            const TTransportSwitcherConfig& config)
        : Router(std::move(router))
        , Fallback(std::move(fallback))
        , Factory(std::move(factory))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Host(std::move(host))
        , SettleTime(config.SettleTime)
        , Log(logging->CreateLog("BLOCKSTORE_CELLS"))
        , RetryDelay(config.InitialRetryDelay, config.MaxRetryDelay)
    {}

    NCloud::NStorage::NRdma::IClientEndpointHandlerPtr GetEndpointHandler()
        override;

    void Attempt()
    {
        if (Router.expired()) {
            return;
        }

        // a failed attempt destroys the endpoint it was building, and with it
        // the handler handed out here, so no stale handler can ever drive the
        // endpoint a later attempt produces
        Factory(GetEndpointHandler())
            .Subscribe(
                [self = shared_from_this()](const auto& future)
                { self->OnAttemptCompleted(future.GetValue()); });
    }

    void OnConnected()
    {
        ui64 generation = 0;

        with_lock (Lock) {
            Connected = true;
            generation = ++SettleGeneration;
        }

        StartSettling(generation);
    }

    void OnDisconnected()
    {
        auto router = Router.lock();
        if (!router) {
            return;
        }

        with_lock (Lock) {
            Connected = false;
            // invalidates a settle in flight
            ++SettleGeneration;

            if (!PreferredActive) {
                return;
            }
            PreferredActive = false;

            // under the lock, so that a settle racing this break cannot store
            // its target after ours; the store is a wait-free swap
            router->SetTarget(Fallback);
        }

        STORAGE_INFO("[" << Host << "] moving data back onto the fallback");
    }

private:
    void OnAttemptCompleted(const TResultOrError<IBlockStorePtr>& result)
    {
        auto router = Router.lock();
        if (!router) {
            return;
        }

        if (!HasError(result) && result.GetResult()) {
            ui64 generation = 0;
            bool connected = false;

            with_lock (Lock) {
                Preferred = result.GetResult();
                connected = Connected;
                if (connected) {
                    generation = ++SettleGeneration;
                }
            }

            if (connected) {
                // the endpoint reported itself connected before we got hold of
                // it, so nothing else is going to start the wait
                StartSettling(generation);
            }
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

    // Starts the wait after which the data may move onto the preferred
    // transport. The generation lets a break invalidate a wait in flight.
    void StartSettling(ui64 generation)
    {
        bool everActive = false;

        with_lock (Lock) {
            everActive = EverActive;
        }

        // The wait guards a return to a link that has already proved it can
        // drop. A link that has never carried our data has proved nothing, so
        // making it wait would only keep the data on the slower transport.
        if (!SettleTime || !everActive) {
            Settle(generation);
            return;
        }

        Scheduler->Schedule(
            Timer->Now() + SettleTime,
            [weakSelf = weak_from_this(), generation]
            {
                if (auto self = weakSelf.lock()) {
                    self->Settle(generation);
                }
            });
    }

    void Settle(ui64 generation)
    {
        auto router = Router.lock();
        if (!router) {
            return;
        }

        with_lock (Lock) {
            if (generation != SettleGeneration || !Connected ||
                PreferredActive || !Preferred)
            {
                return;
            }
            PreferredActive = true;
            EverActive = true;

            // under the lock, so that a break racing this settle cannot store
            // its target before ours; the store is a wait-free swap
            router->SetTarget(Preferred);
        }

        STORAGE_INFO(
            "[" << Host << "] switched over to the preferred transport");
    }
};

////////////////////////////////////////////////////////////////////////////////

// Turns the endpoint state reported by the rdma client into switching
// decisions. Holds the switcher weakly: the endpoint holds the handler, and the
// switcher holds the endpoint, so a strong reference here would keep both alive
// forever.
class TEndpointHandler final
    : public NCloud::NStorage::NRdma::IClientEndpointHandler
{
private:
    const std::weak_ptr<TTransportSwitcher> Switcher;

    TLog Log;

public:
    TEndpointHandler(std::weak_ptr<TTransportSwitcher> switcher, TLog log)
        : Switcher(std::move(switcher))
        , Log(std::move(log))
    {}

    void HandleConnected(const TString& host, ui32 port) override
    {
        Y_UNUSED(host);
        Y_UNUSED(port);
        if (auto self = Switcher.lock()) {
            self->OnConnected();
        }
    }

    void HandleDisconnected(const TString& host, ui32 port) override
    {
        Y_UNUSED(host);
        Y_UNUSED(port);
        if (auto self = Switcher.lock()) {
            self->OnDisconnected();
        }
    }

    void HandleUnavailable(const TString& host, ui32 port) override
    {
        Y_UNUSED(port);
        // nothing to do: by now the data is already on the fallback. The signal
        // belongs to host liveness, which is a separate concern.
        STORAGE_WARN("[" << host << "] rdma endpoint is unavailable");
    }
};

////////////////////////////////////////////////////////////////////////////////

NCloud::NStorage::NRdma::IClientEndpointHandlerPtr
TTransportSwitcher::GetEndpointHandler()
{
    return std::make_shared<TEndpointHandler>(weak_from_this(), Log);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITransportSwitcherPtr StartTransportSwitching(
    IEndpointRouterPtr router,
    IBlockStorePtr fallback,
    TEndpointFactory factory,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    TString host,
    TTransportSwitcherConfig config)
{
    auto switcher = std::make_shared<TTransportSwitcher>(
        std::move(router),
        std::move(fallback),
        std::move(factory),
        std::move(timer),
        std::move(scheduler),
        logging,
        std::move(host),
        config);

    switcher->Attempt();

    return switcher;
}

}   // namespace NCloud::NBlockStore::NCells
