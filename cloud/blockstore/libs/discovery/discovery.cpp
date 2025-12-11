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

using namespace NMonitoring;

namespace NCloud::NBlockStore::NDiscovery {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool FilterOutByPort(
    const TInstanceInfo& ii,
    NProto::EDiscoveryPortFilter filter)
{
    using NProto::EDiscoveryPortFilter;

    if (filter == EDiscoveryPortFilter::DISCOVERY_ANY_PORT) {
        return false;
    }

    const bool acceptSecurePorts =
        filter == EDiscoveryPortFilter::DISCOVERY_SECURE_PORT;

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
    TDiscoveryConfigPtr Config;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;
    IBanListPtr BanList;
    IInstanceFetcherPtr StaticInstanceFetcher;
    IInstanceFetcherPtr ConductorInstanceFetcher;
    IHealthCheckerPtr HealthChecker;
    IBalancingPolicyPtr BalancingPolicy;

    TLog Log;
    TDiscoveryServiceCounters Counters;

    TInstanceList ConductorInstances;
    TInstanceList StaticInstances;

    TAtomic ShouldStop = false;

public:
    TDiscoveryService(
        TDiscoveryConfigPtr config,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IBanListPtr banList,
        IInstanceFetcherPtr staticInstanceFetcher,
        IInstanceFetcherPtr conductorInstanceFetcher,
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
        , HealthChecker(std::move(healthChecker))
        , BalancingPolicy(std::move(balancingPolicy))
    {}

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

    void ScheduleFetch(
        const TDuration interval,
        IInstanceFetcher& fetcher,
        TInstanceList& instances);
    void FetchInstances(
        const TDuration interval,
        IInstanceFetcher& fetcher,
        TInstanceList& instances);

    void ScheduleHealthCheck(TInstanceList& instances);
    void HealthCheckInstances(TInstanceList& instances);
};

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryService::ServeRequest(
    const NProto::TDiscoverInstancesRequest& request,
    NProto::TDiscoverInstancesResponse* response)
{
    if (!AtomicGet(StaticInstances.HealthCheckDone) ||
        !AtomicGet(ConductorInstances.HealthCheckDone))
    {
        *response->MutableError() =
            MakeError(E_REJECTED, "not initialized yet");
        return;
    }

    const auto filter = request.GetInstanceFilter();

    auto instances = response->MutableInstances();
    auto cg = Guard(ConductorInstances.Lock);
    auto sg = Guard(StaticInstances.Lock);

    auto sit = StaticInstances.Instances.begin();
    auto cit = ConductorInstances.Instances.begin();
    while (ui32(instances->size()) < request.GetLimit()) {
        if (sit == StaticInstances.Instances.end()) {
            while (ui32(instances->size()) < request.GetLimit() &&
                   cit < ConductorInstances.Instances.end())
            {
                AddInstance(*cit, filter, response);
                ++cit;
            }

            break;
        }

        if (cit == ConductorInstances.Instances.end()) {
            while (ui32(instances->size()) < request.GetLimit() &&
                   sit < StaticInstances.Instances.end())
            {
                AddInstance(*sit, filter, response);
                ++sit;
            }

            break;
        }

        if (sit->BalancingScore > cit->BalancingScore) {
            AddInstance(*sit, filter, response);
            ++sit;
        } else {
            AddInstance(*cit, filter, response);
            ++cit;
        }
    }
}

void TDiscoveryService::Start()
{
    Log = Logging->CreateLog("BLOCKSTORE_DISCOVERY");
    auto counters = Monitoring->GetCounters();
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");

    auto discoveryCounters = rootGroup->GetSubgroup("component", "discovery");
    auto serviceCounters =
        discoveryCounters->GetSubgroup("subcomponent", "service");
    Counters.Register(*serviceCounters);

    BanList->Start();
    StaticInstanceFetcher->Start();
    ConductorInstanceFetcher->Start();
    HealthChecker->Start();

    BanList->Update();
    FetchInstances(
        Config->GetConductorRequestInterval(),
        *ConductorInstanceFetcher,
        ConductorInstances);
    FetchInstances(
        Config->GetLocalFilesReloadInterval(),
        *StaticInstanceFetcher,
        StaticInstances);
    ScheduleHealthCheck(ConductorInstances);
    ScheduleHealthCheck(StaticInstances);
}

void TDiscoveryService::Stop()
{
    AtomicSet(ShouldStop, true);

    HealthChecker->Stop();
    ConductorInstanceFetcher->Stop();
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
        [weak_ptr = std::move(weak_ptr), interval, &fetcher, &instances]
        {
            if (auto p = weak_ptr.lock()) {
                p->FetchInstances(interval, fetcher, instances);
            }
        });
}

void TDiscoveryService::FetchInstances(
    const TDuration interval,
    IInstanceFetcher& fetcher,
    TInstanceList& instances)
{
    try {
        auto weak_ptr = weak_from_this();
        fetcher.FetchInstances(instances).Subscribe(
            [weak_ptr = std::move(weak_ptr), interval, &fetcher, &instances](
                auto)
            {
                if (auto p = weak_ptr.lock()) {
                    p->ScheduleFetch(interval, fetcher, instances);
                }
            });
    } catch (...) {
        STORAGE_ERROR("unexpected fetch error: " << CurrentExceptionMessage());
        Counters.OnUnexpectedFetchError();
        ScheduleFetch(interval, fetcher, instances);
    }
}

void TDiscoveryService::ScheduleHealthCheck(TInstanceList& instances)
{
    if (AtomicGet(ShouldStop)) {
        return;
    }

    auto weak_ptr = weak_from_this();
    Scheduler->Schedule(
        Timer->Now() + Config->GetHealthCheckInterval(),
        [weak_ptr = std::move(weak_ptr), &instances]
        {
            if (auto p = weak_ptr.lock()) {
                p->HealthCheckInstances(instances);
            }
        });
}

void TDiscoveryService::HealthCheckInstances(TInstanceList& instances)
{
    try {
        auto weak_ptr = weak_from_this();
        HealthChecker->UpdateStatus(instances).Subscribe(
            [weak_ptr = std::move(weak_ptr), &instances](auto)
            {
                if (auto p = weak_ptr.lock()) {
                    p->BalancingPolicy->Reorder(instances);
                    AtomicSet(instances.HealthCheckDone, true);
                    p->ScheduleHealthCheck(instances);
                }
            });
    } catch (...) {
        STORAGE_ERROR(
            "unexpected healthcheck error: " << CurrentExceptionMessage());
        Counters.OnUnexpectedHealthCheckError();
        ScheduleHealthCheck(instances);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServiceStub final: public IDiscoveryService
{
private:
    TString DefaultHost;
    ui16 DefaultInsecurePort;
    ui16 DefaultSecurePort;

public:
    TDiscoveryServiceStub(
        TString defaultHost,
        ui16 defaultInsecurePort,
        ui16 defaultSecurePort)
        : DefaultHost(std::move(defaultHost))
        , DefaultInsecurePort(defaultInsecurePort)
        , DefaultSecurePort(defaultSecurePort)
    {}

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
    {}

    void Stop() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDiscoveryServicePtr CreateDiscoveryService(
    TDiscoveryConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IBanListPtr banList,
    IInstanceFetcherPtr staticInstanceFetcher,
    IInstanceFetcherPtr conductorInstanceFetcher,
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
        std::move(healthChecker),
        std::move(balancingPolicy));
}

IDiscoveryServicePtr CreateDiscoveryServiceStub(
    TString defaultHost,
    ui16 defaultInsecurePort,
    ui16 defaultSecurePort)
{
    return std::make_shared<TDiscoveryServiceStub>(
        std::move(defaultHost),
        defaultInsecurePort,
        defaultSecurePort);
}

}   // namespace NCloud::NBlockStore::NDiscovery
