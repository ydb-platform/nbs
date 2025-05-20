#include "healthcheck.h"

#include "config.h"

#include "ping.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/histogram.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/weighted_percentile.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

using namespace NMonitoring;
using namespace NThreading;

namespace NCloud::NBlockStore::NDiscovery {

namespace {

////////////////////////////////////////////////////////////////////////////////

TInstanceInfo* FindInstance(
    TInstanceList& instances,
    const TString& host,
    ui16 port)
{
    for (auto& ii: instances.Instances) {
        if (ii.Host == host && ii.Port == port) {
            return &ii;
        }
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

struct THealthCheckCounters
{
    TDynamicCounters::TCounterPtr HealthCheckCount;
    TDynamicCounters::TCounterPtr HealthCheckErrors;
    TDynamicCounters::TCounterPtr ReachableCount;
    TDynamicCounters::TCounterPtr UnreachableCount;
    TVector<TDynamicCounters::TCounterPtr> PingTimePercentiles;

    TLatencyHistogram PingTimeHistogram;

    void Register(TDynamicCounters& counters)
    {
        HealthCheckCount = counters.GetCounter("HealthCheckCount", true);
        HealthCheckErrors = counters.GetCounter("HealthCheckErrors", true);
        ReachableCount = counters.GetCounter("ReachableCount");
        UnreachableCount = counters.GetCounter("UnreachableCount");

        auto percentileGroup = counters.GetSubgroup("percentiles", "PingTime");
        const auto& percentiles = GetDefaultPercentiles();
        for (ui32 i = 0; i < percentiles.size(); ++i) {
            PingTimePercentiles.emplace_back(
                percentileGroup->GetCounter(percentiles[i].second)
            );
        }
    }

    void OnHealthCheck()
    {
        HealthCheckCount->Inc();
    }

    void OnHealthCheckError()
    {
        HealthCheckErrors->Inc();
    }

    void OnPingResult(TDuration d)
    {
        PingTimeHistogram.RecordValue(d);
    }

    void UpdatePingTimePercentiles()
    {
        auto counter = PingTimePercentiles.begin();
        for (const auto& p: GetDefaultPercentiles()) {
            Y_ABORT_UNLESS(counter != PingTimePercentiles.end());

            **counter = PingTimeHistogram.GetValueAtPercentile(100 * p.first);
            ++counter;
        }
    }

    void SetReachableCount(int count)
    {
        *ReachableCount = count;
    }

    void SetUnreachableCount(int count)
    {
        *UnreachableCount = count;
    }
};

////////////////////////////////////////////////////////////////////////////////

class THealthChecker final
    : public IHealthChecker
{
private:
    TDiscoveryConfigPtr Config;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;

    TLog Log;
    THealthCheckCounters Counters;

    IPingClientPtr InsecureClient;
    IPingClientPtr SecureClient;

public:
    THealthChecker(
            TDiscoveryConfigPtr config,
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring,
            IPingClientPtr insecureClient,
            IPingClientPtr secureClient)
        : Config(std::move(config))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , InsecureClient(std::move(insecureClient))
        , SecureClient(std::move(secureClient))
    {
    }

    ~THealthChecker()
    {
    }

public:
    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_DISCOVERY");
        auto counters = Monitoring->GetCounters();
        auto rootGroup = counters->GetSubgroup("counters", "blockstore");

        auto discoveryCounters = rootGroup->GetSubgroup("component", "discovery");
        auto healthCheckCounters = discoveryCounters->GetSubgroup("subcomponent", "healthcheck");
        Counters.Register(*healthCheckCounters);

        InsecureClient->Start();
        SecureClient->Start();
    }

    void Stop() override
    {
        InsecureClient->Stop();
        SecureClient->Stop();
    }

    TFuture<void> UpdateStatus(TInstanceList& instances) override
    {
        Counters.OnHealthCheck();

        auto promise = NewPromise();
        TVector<TFuture<TPingResponseInfo>> pings;

        std::shared_ptr<int> countdown;

        with_lock (instances.Lock) {
            const auto limit = Config->GetMaxPingRequestsPerHealthCheck();
            TVector<std::pair<TInstant, TInstanceInfo*>> byLastPingTs;
            byLastPingTs.reserve(instances.Instances.size());
            for (auto& ii: instances.Instances) {
                byLastPingTs.emplace_back(ii.LastStat.Ts, &ii);
            }

            if (byLastPingTs.size() > limit) {
                NthElement(
                    byLastPingTs.begin(),
                    byLastPingTs.begin() + limit,
                    byLastPingTs.end()
                );

                byLastPingTs.resize(limit);
            }

            for (auto& x: byLastPingTs) {
                auto& client = x.second->IsSecurePort
                    ? SecureClient
                    : InsecureClient;

                auto ping = client->Ping(
                    x.second->Host,
                    x.second->Port,
                    Config->GetPingRequestTimeout()
                );

                pings.push_back(std::move(ping));
            }

            countdown = std::make_shared<int>(pings.size());
        }

        for (auto& ping: pings) {
            ping.Subscribe([=, this, &instances] (TFuture<TPingResponseInfo> f) mutable {
                bool done = false;

                with_lock (instances.Lock) {
                    OnPingResponse(f.GetValue(), instances);

                    done = --*countdown == 0;
                    if (done) {
                        int reachable = 0;
                        int unreachable = 0;
                        for (const auto& ii: instances.Instances) {
                            if (ii.Status == TInstanceInfo::EStatus::Unreachable) {
                                ++unreachable;
                            } else {
                                ++reachable;
                            }
                        }
                        Counters.SetReachableCount(reachable);
                        Counters.SetUnreachableCount(unreachable);
                        Counters.UpdatePingTimePercentiles();
                    }
                }

                if (done) {
                    promise.SetValue();
                }
            });
        }

        if (pings.empty()) {
            promise.SetValue();
        }

        return promise;
    }

private:
    void OnPingResponse(
        const TPingResponseInfo& response,
        TInstanceList& instances)
    {
        auto ii = FindInstance(
            instances,
            response.Host,
            response.Port
        );

        if (ii) {
            if (HasError(response.Record)) {
                Counters.OnHealthCheckError();
                STORAGE_WARN("unreachable instance: "
                    << ii->Host << ":" << ii->Port
                    << " " << FormatError(response.Record.GetError()));
                ii->Status = TInstanceInfo::EStatus::Unreachable;
            } else {
                ii->Status = TInstanceInfo::EStatus::Reachable;
            }

            ii->PrevStat = ii->LastStat;
            ii->LastStat.Ts = Now();
            ii->LastStat.Bytes =
                response.Record.GetLastByteCount();
            ii->LastStat.Requests =
                response.Record.GetLastRequestCount();
        }

        Counters.OnPingResult(response.Time);
    }
};

////////////////////////////////////////////////////////////////////////////////

class THealthCheckerStub final
    : public IHealthChecker
{
public:
    TFuture<void> UpdateStatus(TInstanceList& instances) override
    {
        with_lock (instances.Lock) {
            for (ui32 i = 0; i < instances.Instances.size(); ++i) {
                instances.Instances[i].Status = TInstanceInfo::EStatus::Reachable;
            }
        }

        return MakeFuture();
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

IHealthCheckerPtr CreateHealthChecker(
    TDiscoveryConfigPtr config,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IPingClientPtr insecurePingClient,
    IPingClientPtr securePingClient)
{
    return std::make_shared<THealthChecker>(
        std::move(config),
        std::move(logging),
        std::move(monitoring),
        std::move(insecurePingClient),
        std::move(securePingClient)
    );
}

IHealthCheckerPtr CreateHealthCheckerStub()
{
    return std::make_shared<THealthCheckerStub>();
}

}   // namespace NCloud::NBlockStore::NDiscovery
