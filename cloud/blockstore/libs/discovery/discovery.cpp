#include "discovery.h"

#include "balancing.h"
#include "ban.h"
#include "config.h"
#include "fetch.h"
#include "healthcheck.h"

#include <cloud/blockstore/public/api/protos/discovery.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/hash_set.h>

#include <algorithm>
#include <array>
#include <exception>
#include <mutex>
#include <utility>

using namespace NMonitoring;

namespace NCloud::NBlockStore::NDiscovery {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool FilterOutByPort(const TInstanceInfo& ii, NProto::EDiscoveryPortFilter filter)
{
    using NProto::EDiscoveryPortFilter;

    if (filter == EDiscoveryPortFilter::DISCOVERY_ANY_PORT) {
        return false;
    }

    const bool acceptSecurePorts = filter == EDiscoveryPortFilter::DISCOVERY_SECURE_PORT;

    return acceptSecurePorts != ii.IsSecurePort;
}

////////////////////////////////////////////////////////////////////////////////

struct TDiscoveryServiceCounters
{
    TDynamicCounters::TCounterPtr UnexpectedFetchErrors;
    TDynamicCounters::TCounterPtr UnexpectedHealthCheckErrors;

    void Register(TDynamicCounters& counters)
    {
        UnexpectedFetchErrors =
            counters.GetCounter("UnexpectedFetchErrors", true);
        UnexpectedHealthCheckErrors =
            counters.GetCounter("UnexpectedHealthCheckErrors", true);
    }

    void OnUnexpectedFetchError()
    {
        UnexpectedFetchErrors->Inc();
    }

    void OnUnexpectedHealthCheckError()
    {
        UnexpectedHealthCheckErrors->Inc();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryService final
    : public IDiscoveryService
    , public std::enable_shared_from_this<TDiscoveryService>
{
private:
    TDiscoveryConfigConstPtr Config;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;
    IBanListPtr BanList;
    IInstanceFetcherPtr StaticInstanceFetcher;
    IInstanceFetcherPtr ConductorInstanceFetcher;
    IInstanceFetcherPtr EdsInstanceFetcher;
    IHealthCheckerPtr HealthChecker;
    IBalancingPolicyPtr BalancingPolicy;

    TLog Log;
    TDiscoveryServiceCounters Counters;

    TInstanceList ConductorInstances;
    TInstanceList StaticInstances;
    TInstanceList EdsInstances;

    TAtomic ShouldStop = false;

public:
    TDiscoveryService(
            TDiscoveryConfigConstPtr config,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring,
            IBanListPtr banList,
            IInstanceFetcherPtr staticInstanceFetcher,
            IInstanceFetcherPtr conductorInstanceFetcher,
            IInstanceFetcherPtr edsInstanceFetcher,
            IHealthCheckerPtr healthChecker,
            IBalancingPolicyPtr balancingPolicy)
        : Config(std::move(config))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , BanList(std::move(banList))
        , StaticInstanceFetcher(std::move(staticInstanceFetcher))
        , ConductorInstanceFetcher(std::move(conductorInstanceFetcher))
        , EdsInstanceFetcher(std::move(edsInstanceFetcher))
        , HealthChecker(std::move(healthChecker))
        , BalancingPolicy(std::move(balancingPolicy))
    {
    }

public:
    void ServeRequest(
        const NProto::TDiscoverInstancesRequest& request,
        NProto::TDiscoverInstancesResponse* response) override;

    void Start() override;
    void Stop() override;

private:
    void AddInstance(
        const TInstanceInfo& ii,
        NProto::EDiscoveryPortFilter filter,
        NProto::TDiscoverInstancesResponse* r);

    void InitializeInstances(
        const TDuration interval,
        IInstanceFetcher& fetcher,
        TInstanceList& instances);

    void ScheduleFetch(
        const TDuration interval,
        IInstanceFetcher& fetcher,
        TInstanceList& instances);
    NThreading::TFuture<NProto::TError> FetchInstances(
        const TDuration interval,
        IInstanceFetcher& fetcher,
        TInstanceList& instances);

    void ScheduleHealthCheck(
        TInstanceList& instances,
        NThreading::TPromise<void> firstCheck);
    void HealthCheckInstances(
        TInstanceList& instances,
        NThreading::TPromise<void> firstCheck);
};

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryService::ServeRequest(
    const NProto::TDiscoverInstancesRequest& request,
    NProto::TDiscoverInstancesResponse* response)
{
    const auto filter = request.GetInstanceFilter();
    auto instances = response->MutableInstances();

    // Preserve source order when balancing scores are equal.
    const std::array<TInstanceList*, 3> sources = {
        &ConductorInstances,
        &StaticInstances,
        &EdsInstances,
    };
    std::array<std::unique_lock<TAdaptiveLock>, 3> locks;
    std::array<const TDeque<TInstanceInfo>*, 3> lists = {};
    std::array<TDeque<TInstanceInfo>::const_iterator, 3> iterators;
    bool allReady = true;
    for (size_t i = 0; i < sources.size(); ++i) {
        auto& source = *sources[i];
        if (!AtomicGet(source.HealthCheckDone)) {
            allReady = false;
            continue;
        }
        locks[i] = std::unique_lock(source.Lock);
        lists[i] = &source.Instances;
        iterators[i] = lists[i]->begin();
    }

    THashSet<std::pair<TString, ui16>> seen;
    while (ui32(instances->size()) < request.GetLimit()) {
        const TInstanceInfo* next = nullptr;
        size_t source = 0;
        for (size_t i = 0; i < lists.size(); ++i) {
            if (lists[i] && iterators[i] != lists[i]->end()
                    && (!next || iterators[i]->BalancingScore > next->BalancingScore))
            {
                next = &*iterators[i];
                source = i;
            }
        }
        if (!next) {
            break;
        }
        ++iterators[source];
        if (!FilterOutByPort(*next, filter)
                && seen.emplace(next->Host, next->Port).second)
        {
            AddInstance(*next, filter, response);
        }
    }

    if (!allReady && instances->empty()) {
        *response->MutableError() =
            MakeError(E_REJECTED, "not initialized yet");
    }
}

void TDiscoveryService::Start()
{
    Log = Logging->CreateLog("BLOCKSTORE_DISCOVERY");
    auto counters = Monitoring->GetCounters();
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");

    auto discoveryCounters = rootGroup->GetSubgroup("component", "discovery");
    auto serviceCounters = discoveryCounters->GetSubgroup("subcomponent", "service");
    Counters.Register(*serviceCounters);

    BanList->Start();
    StaticInstanceFetcher->Start();
    ConductorInstanceFetcher->Start();
    EdsInstanceFetcher->Start();
    HealthChecker->Start();

    BanList->Update();
    InitializeInstances(
        Config->GetConductorRequestInterval(),
        *ConductorInstanceFetcher,
        ConductorInstances
    );
    InitializeInstances(
        Config->GetEdsRequestInterval(),
        *EdsInstanceFetcher,
        EdsInstances
    );
    InitializeInstances(
        Config->GetLocalFilesReloadInterval(),
        *StaticInstanceFetcher,
        StaticInstances
    );
}

void TDiscoveryService::Stop()
{
    AtomicSet(ShouldStop, true);

    HealthChecker->Stop();
    ConductorInstanceFetcher->Stop();
    EdsInstanceFetcher->Stop();
    StaticInstanceFetcher->Stop();
    BanList->Stop();
}

void TDiscoveryService::AddInstance(
    const TInstanceInfo& ii,
    NProto::EDiscoveryPortFilter filter,
    NProto::TDiscoverInstancesResponse* r)
{
    if (FilterOutByPort(ii, filter)) {
        return;
    }

    if (!BanList->IsBanned(ii.Host, ii.Port)) {
        auto* i = r->AddInstances();
        i->SetHost(ii.Host);
        i->SetPort(ii.Port);
    }
}

void TDiscoveryService::InitializeInstances(
    const TDuration interval,
    IInstanceFetcher& fetcher,
    TInstanceList& instances)
{
    if (AtomicGet(ShouldStop)) {
        return;
    }

    auto weak_ptr = weak_from_this();
    FetchInstances(interval, fetcher, instances).Subscribe(
        [weak_ptr = std::move(weak_ptr), interval, &fetcher, &instances] (const auto& future) {
            auto p = weak_ptr.lock();
            if (!p || AtomicGet(p->ShouldStop)) {
                return;
            }
            bool retry = false;
            if (HasError(future.GetValue())) {
                with_lock (instances.Lock) {
                    retry = instances.Instances.empty();
                }
            }
            if (retry) {
                const auto retryDelay = std::min(interval, TDuration::Seconds(1));
                p->Scheduler->Schedule(
                    p->Timer->Now() + retryDelay,
                    [weak_ptr, interval, &fetcher, &instances] {
                        if (auto p = weak_ptr.lock()) {
                            p->InitializeInstances(interval, fetcher, instances);
                        }
                    }
                );
                return;
            }
            auto firstCheck = NThreading::NewPromise();
            firstCheck.GetFuture().Subscribe(
                [weak_ptr, interval, &fetcher, &instances] (const auto&) {
                    if (auto p = weak_ptr.lock()) {
                        p->ScheduleFetch(interval, fetcher, instances);
                    }
                }
            );
            p->HealthCheckInstances(instances, std::move(firstCheck));
        }
    );
}

void TDiscoveryService::ScheduleFetch(
    const TDuration interval,
    IInstanceFetcher& fetcher,
    TInstanceList& instances)
{
    if (AtomicGet(ShouldStop)) {
        return;
    }

    auto weak_ptr = weak_from_this();
    Scheduler->Schedule(
        Timer->Now() + interval,
        [weak_ptr = std::move(weak_ptr), interval, &fetcher, &instances] {
            if (auto p = weak_ptr.lock()) {
                p->FetchInstances(interval, fetcher, instances);
            }
        }
    );
}

NThreading::TFuture<NProto::TError> TDiscoveryService::FetchInstances(
    const TDuration interval,
    IInstanceFetcher& fetcher,
    TInstanceList& instances)
{
    if (AtomicGet(ShouldStop)) {
        return NThreading::MakeFuture(NProto::TError{});
    }

    NThreading::TFuture<NProto::TError> response;
    try {
        response = fetcher.FetchInstances(instances);
    } catch (...) {
        const auto message = CurrentExceptionMessage();
        STORAGE_ERROR("unexpected fetch error: " << message);
        Counters.OnUnexpectedFetchError();
        response = NThreading::MakeFuture(MakeError(E_FAIL, message));
    }

    auto weak_ptr = weak_from_this();
    return response.Apply(
        [weak_ptr = std::move(weak_ptr), interval, &fetcher, &instances] (const auto& future) {
            auto p = weak_ptr.lock();
            if (!p || AtomicGet(p->ShouldStop)) {
                return NProto::TError{};
            }

            NProto::TError error;
            try {
                error = future.GetValue();
            } catch (...) {
                const auto message = CurrentExceptionMessage();
                auto& Log = p->Log;
                STORAGE_ERROR("unexpected fetch error: " << message);
                p->Counters.OnUnexpectedFetchError();
                error = MakeError(E_FAIL, message);
            }

            if (AtomicGet(instances.HealthCheckDone)) {
                p->ScheduleFetch(interval, fetcher, instances);
            }
            return error;
        }
    );
}

void TDiscoveryService::ScheduleHealthCheck(
    TInstanceList& instances,
    NThreading::TPromise<void> firstCheck)
{
    if (AtomicGet(ShouldStop)) {
        return;
    }

    auto weak_ptr = weak_from_this();
    Scheduler->Schedule(
        Timer->Now() + Config->GetHealthCheckInterval(),
        [weak_ptr = std::move(weak_ptr),
         &instances,
         firstCheck = std::move(firstCheck)] () mutable {
            if (auto p = weak_ptr.lock()) {
                p->HealthCheckInstances(instances, std::move(firstCheck));
            }
        }
    );
}

void TDiscoveryService::HealthCheckInstances(
    TInstanceList& instances,
    NThreading::TPromise<void> firstCheck)
{
    if (AtomicGet(ShouldStop)) {
        return;
    }

    NThreading::TFuture<void> response;
    try {
        response = HealthChecker->UpdateStatus(instances);
    } catch (...) {
        response = NThreading::MakeErrorFuture<void>(std::current_exception());
    }

    auto weak_ptr = weak_from_this();
    response.Subscribe(
        [weak_ptr = std::move(weak_ptr),
         &instances,
         firstCheck = std::move(firstCheck)] (const auto& future) mutable {
            auto p = weak_ptr.lock();
            if (!p || AtomicGet(p->ShouldStop)) {
                return;
            }

            try {
                future.GetValue();
                p->BalancingPolicy->Reorder(instances);
            } catch (...) {
                auto& Log = p->Log;
                STORAGE_ERROR("unexpected healthcheck error: "
                    << CurrentExceptionMessage());
                p->Counters.OnUnexpectedHealthCheckError();
                p->ScheduleHealthCheck(instances, std::move(firstCheck));
                return;
            }

            AtomicSet(instances.HealthCheckDone, true);
            p->ScheduleHealthCheck(instances, {});
            if (firstCheck.Initialized()) {
                firstCheck.SetValue();
            }
        }
    );
}

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServiceStub final
    : public IDiscoveryService
{
private:
    TString DefaultHost;
    ui16 DefaultInsecurePort;
    ui16 DefaultSecurePort;

public:
    TDiscoveryServiceStub(TString defaultHost, ui16 defaultInsecurePort, ui16 defaultSecurePort)
        : DefaultHost(std::move(defaultHost))
        , DefaultInsecurePort(defaultInsecurePort)
        , DefaultSecurePort(defaultSecurePort)
    {
    }

public:
    void ServeRequest(
        const NProto::TDiscoverInstancesRequest& request,
        NProto::TDiscoverInstancesResponse* response) override
    {
        using NProto::EDiscoveryPortFilter;

        if (!request.GetLimit() || !DefaultHost) {
            return;
        }

        switch (request.GetInstanceFilter()) {
        case EDiscoveryPortFilter::DISCOVERY_INSECURE_PORT:
            if (DefaultInsecurePort) {
                auto* instance = response->AddInstances();

                instance->SetHost(DefaultHost);
                instance->SetPort(DefaultInsecurePort);
            }
            break;
        case EDiscoveryPortFilter::DISCOVERY_SECURE_PORT:
            if (DefaultSecurePort) {
                auto* instance = response->AddInstances();

                instance->SetHost(DefaultHost);
                instance->SetPort(DefaultSecurePort);
            }
            break;
        case EDiscoveryPortFilter::DISCOVERY_ANY_PORT:
            if (DefaultInsecurePort) {
                auto* instance = response->AddInstances();

                instance->SetHost(DefaultHost);
                instance->SetPort(DefaultInsecurePort);
            } else if (DefaultSecurePort) {
                auto* instance = response->AddInstances();

                instance->SetHost(DefaultHost);
                instance->SetPort(DefaultSecurePort);
            }
            break;
        default:
            break;
        }
    }

    void Start() override
    {
    }

    void Stop() override
    {
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDiscoveryServicePtr CreateDiscoveryService(
    TDiscoveryConfigConstPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IBanListPtr banList,
    IInstanceFetcherPtr staticInstanceFetcher,
    IInstanceFetcherPtr conductorInstanceFetcher,
    IInstanceFetcherPtr edsInstanceFetcher,
    IHealthCheckerPtr healthChecker,
    IBalancingPolicyPtr balancingPolicy)
{
    return std::make_shared<TDiscoveryService>(
        std::move(config),
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(monitoring),
        std::move(banList),
        std::move(staticInstanceFetcher),
        std::move(conductorInstanceFetcher),
        std::move(edsInstanceFetcher),
        std::move(healthChecker),
        std::move(balancingPolicy)
    );
}

IDiscoveryServicePtr CreateDiscoveryServiceStub(
    TString defaultHost, ui16 defaultInsecurePort, ui16 defaultSecurePort)
{
    return std::make_shared<TDiscoveryServiceStub>(
        std::move(defaultHost), defaultInsecurePort, defaultSecurePort);
}

}   // namespace NCloud::NBlockStore::NDiscovery
