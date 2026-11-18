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
    const std::weak_ptr<ITransportTarget> Target;
    const IBlockStorePtr Fallback;   // the endpoint the router started with
    const TEndpointFactory Factory;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const TString EndpointName;
    const TDuration SettleTime;

    TLog Log;

    TAdaptiveLock Lock;
    IBlockStorePtr Preferred;       // the preferred endpoint, once acquired
    bool Active = false;
    bool PreferredActive = false;   // is the router pointing at it
    bool Connected = false;
    bool EverActive = false;
    ui64 SettleGeneration = 0;

public:
    TTransportSwitcher(
        ITransportTargetPtr target,
        IBlockStorePtr fallback,
        TEndpointFactory factory,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        const ILoggingServicePtr& logging,
        TString endpointName,
        const TTransportSwitcherConfig& config)
        : Target(std::move(target))
        , Fallback(std::move(fallback))
        , Factory(std::move(factory))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , EndpointName(std::move(endpointName))
        , SettleTime(config.SettleTime)
        , Log(logging->CreateLog("BLOCKSTORE_CELLS"))
    {}

    NCloud::IClientEndpointHandlerPtr GetEndpointHandler() override;

    bool IsPreferredActive() override
    {
        with_lock (Lock) {
            return PreferredActive;
        }
    }

    void Activate() override
    {
        ui64 generation = 0;
        bool connected = false;
        with_lock (Lock) {
            if (Active) {
                return;
            }
            Active = true;
            connected = Connected;
            if (connected) {
                generation = ++SettleGeneration;
            }
        }

        if (connected) {
            StartSettling(generation);
        }
    }

    NProto::TError Start()
    {
        auto result = Factory(GetEndpointHandler());

        if (HasError(result) || !result.GetResult()) {
            auto error = HasError(result)
                             ? result.GetError()
                             : MakeError(
                                   E_FAIL,
                                   "Preferred transport endpoint is missing");
            STORAGE_WARN(
                "[" << EndpointName
                    << "] can't set up the preferred transport: "
                    << FormatError(error) << ", staying on the fallback");
            return error;
        }

        with_lock (Lock) {
            Preferred = result.ExtractResult();
        }

        return {};
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
        auto target = Target.lock();
        if (!target) {
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

            // inside the lock: a settle racing this break must not store its
            // target after ours
            target->SetTarget(Fallback);
        }

        STORAGE_INFO(
            "[" << EndpointName << "] moving data back onto the fallback");
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
        auto target = Target.lock();
        if (!target) {
            return;
        }

        bool firstTime = false;

        with_lock (Lock) {
            if (!Active || generation != SettleGeneration || !Connected ||
                PreferredActive || !Preferred)
            {
                return;
            }
            PreferredActive = true;
            firstTime = !EverActive;
            EverActive = true;

            // inside the lock: a break racing this settle must not store its
            // target before ours
            target->SetTarget(Preferred);
        }

        STORAGE_INFO(
            "[" << EndpointName << "] "
                << (firstTime ? "switched over to" : "returned to")
                << " the preferred transport");
    }
};

////////////////////////////////////////////////////////////////////////////////

// The switcher owns the endpoint, which owns this handler.
// Keep the reference back weak to avoid a cycle.
class TEndpointHandler final: public NCloud::IClientEndpointHandler
{
private:
    const std::weak_ptr<TTransportSwitcher> Switcher;
    const TString EndpointName;

    TLog Log;

public:
    TEndpointHandler(
        std::weak_ptr<TTransportSwitcher> switcher,
        TString endpointName,
        TLog log)
        : Switcher(std::move(switcher))
        , EndpointName(std::move(endpointName))
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
        STORAGE_WARN("[" << EndpointName << "] endpoint is unavailable");
    }
};

////////////////////////////////////////////////////////////////////////////////

NCloud::IClientEndpointHandlerPtr TTransportSwitcher::GetEndpointHandler()
{
    return std::make_shared<TEndpointHandler>(
        weak_from_this(),
        EndpointName,
        Log);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<ITransportSwitcherPtr> PrepareTransportSwitching(
    ITransportTargetPtr target,
    IBlockStorePtr fallback,
    TEndpointFactory factory,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    TString endpointName,
    TTransportSwitcherConfig config)
{
    auto switcher = std::make_shared<TTransportSwitcher>(
        std::move(target),
        std::move(fallback),
        std::move(factory),
        std::move(timer),
        std::move(scheduler),
        logging,
        std::move(endpointName),
        config);

    auto error = switcher->Start();
    if (HasError(error)) {
        return error;
    }

    return ITransportSwitcherPtr(std::move(switcher));
}

}   // namespace NCloud::NBlockStore::NCells
