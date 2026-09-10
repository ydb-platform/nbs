#include "transport_switcher.h"

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/string/builder.h>
#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Keeps the router pointing at the transport that can serve data right now: the
// fallback until the preferred endpoint has connected and settled, and back
// onto the fallback the moment that endpoint breaks.
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
    {}

    NCloud::NStorage::NRdma::IClientEndpointHandlerPtr GetEndpointHandler()
        override;

    // Asks for the preferred endpoint once. There is nothing to retry: the
    // endpoint is handed back before it has connected and reconnects on its
    // own from then on, so a failure here means the rdma client itself cannot
    // give us one, and we stay on the fallback.
    void Start()
    {
        auto result = Factory(GetEndpointHandler());

        if (HasError(result) || !result.GetResult()) {
            STORAGE_WARN(
                "[" << Host << "] can't set up the preferred transport: "
                    << FormatError(result.GetError())
                    << ", staying on the fallback");
            return;
        }

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
            // the endpoint reported itself connected before we got hold of it,
            // so nothing else is going to start the wait
            StartSettling(generation);
        }
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
    const TString Host;   // only for logging: the calls carry no host

    TLog Log;

public:
    TEndpointHandler(
            std::weak_ptr<TTransportSwitcher> switcher,
            TString host,
            TLog log)
        : Switcher(std::move(switcher))
        , Host(std::move(host))
        , Log(std::move(log))
    {}

    void HandleConnected() override
    {
        if (auto self = Switcher.lock()) {
            self->OnConnected();
        }
    }

    void HandleDisconnected() override
    {
        if (auto self = Switcher.lock()) {
            self->OnDisconnected();
        }
    }

    void HandleUnavailable() override
    {
        // nothing to do: by now the data is already on the fallback. The signal
        // belongs to host liveness, which is a separate concern. It repeats on
        // every reconnect attempt for as long as the endpoint stays down.
        STORAGE_WARN("[" << Host << "] rdma endpoint is unavailable");
    }
};

////////////////////////////////////////////////////////////////////////////////

NCloud::NStorage::NRdma::IClientEndpointHandlerPtr
TTransportSwitcher::GetEndpointHandler()
{
    return std::make_shared<TEndpointHandler>(weak_from_this(), Host, Log);
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

    switcher->Start();

    return switcher;
}

}   // namespace NCloud::NBlockStore::NCells
