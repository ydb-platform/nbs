#include "fetch.h"

#include "config.h"

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/neh/neh.h>

#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/utility.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/rwlock.h>

using namespace NMonitoring;
using namespace NThreading;

namespace NCloud::NBlockStore::NDiscovery {

namespace {

////////////////////////////////////////////////////////////////////////////////

void DeleteAt(TInstanceList& instances, const TVector<ui32>& indices)
{
    ui32 offset = 0;
    for (ui32 i = 0; i < instances.Instances.size(); ++i) {
        if (offset < indices.size()
                && i == indices[offset])
        {
            ++offset;
        } else if (offset) {
            const auto j = i - offset;
            instances.Instances[j] = std::move(instances.Instances[i]);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TInstanceFetcherCounters
{
    TDynamicCounters::TCounterPtr FetchCount;
    TDynamicCounters::TCounterPtr FetchErrors;
    TDynamicCounters::TCounterPtr InstanceCount;

    void Register(TDynamicCounters& counters)
    {
        FetchCount = counters.GetCounter("FetchCount", true);
        FetchErrors = counters.GetCounter("FetchErrors", true);
        InstanceCount = counters.GetCounter("InstanceCount");
    }

    void OnFetch()
    {
        FetchCount->Inc();
    }

    void OnFetchError()
    {
        FetchErrors->Inc();
    }

    void SetInstanceCount(int count)
    {
        *InstanceCount = count;
    }
};

namespace NNehUtil {

////////////////////////////////////////////////////////////////////////////////

struct TResult
{
    TString Group;
    TString Data;
    TString Error;
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestContext
{
    ui32 RequestCount;
    TVector<bool> Status;
    TAdaptiveLock Lock;

    TRequestContext(ui32 requestCount)
        : RequestCount(requestCount)
        , Status(requestCount)
    {
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TOnRecv
    : NNeh::IOnRecv
{
    TString Group;
    TPromise<TResult> Result;

    TOnRecv(TString group)
        : Group(std::move(group))
        , Result(NewPromise<TResult>())
    {
    }

    void OnNotify(NNeh::THandle& handle) override {
        Result.SetValue({
            std::move(Group),
            handle.Response()->Data,
            handle.Response()->GetErrorText()
        });
    }

    void OnEnd() override {
        delete this;
    }

    void OnRecv(NNeh::THandle&) override {
        delete this;
    }
};

}   // namespace NNehUtil

////////////////////////////////////////////////////////////////////////////////

struct TFetcherState: TRefCounted<TFetcherState, TAtomicCounter>
{
    TRWMutex Lock;
    bool Running = true;
};

using TFetcherStatePtr = TIntrusivePtr<TFetcherState>;

////////////////////////////////////////////////////////////////////////////////

class TConductorInstanceFetcher final
    : public IInstanceFetcher
{
private:
    TDiscoveryConfigPtr Config;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;

    TLog Log;
    TInstanceFetcherCounters Counters;

    TFetcherStatePtr State;

public:
    TConductorInstanceFetcher(
            TDiscoveryConfigPtr config,
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring,
            ITimerPtr timer,
            ISchedulerPtr scheduler)
        : Config(std::move(config))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , State(MakeIntrusive<TFetcherState>())
    {
        auto result = NNeh::SetProtocolOption("http/AsioThreads", "1");
        Y_DEBUG_ABORT_UNLESS(result);
    }

public:
    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_DISCOVERY");
        auto counters = Monitoring->GetCounters();
        auto rootGroup = counters->GetSubgroup("counters", "blockstore");

        auto discoveryCounters = rootGroup->GetSubgroup("component", "discovery");
        auto fetcherCounters = discoveryCounters->GetSubgroup("subcomponent", "conductor-fetcher");
        Counters.Register(*fetcherCounters);
    }

    void Stop() override
    {
        TWriteGuard guard(State->Lock);
        State->Running = false;
    }

    TFuture<void> FetchInstances(TInstanceList& instances) override
    {
        using namespace NNehUtil;

        Counters.OnFetch();

        if (!Config->GetConductorApiUrl()) {
            return MakeFuture();
        }

        auto promise = NewPromise();
        auto requestContext = std::make_shared<TRequestContext>(
            Config->GetConductorGroups().size()
        );

        for (ui32 i = 0; i < Config->GetConductorGroups().size(); ++i) {
            const auto group = Config->GetConductorGroups()[i];

            auto onRecv = std::make_unique<TOnRecv>(group);
            onRecv->Result.GetFuture().Subscribe(
                [=, this, s = State, &instances] (auto f) mutable {
                    TReadGuard guard(s->Lock);

                    if (!s->Running) {
                        return;
                    }

                    const auto& result = f.GetValue();

                    if (OnConductorResult(i, result, *requestContext, instances)) {
                        promise.SetValue();
                    }
                }
            );

            NNeh::Request(Config->GetConductorApiUrl() + "/" + group, &*onRecv);
            onRecv.release();

            Scheduler->Schedule(
                Timer->Now() + Config->GetConductorRequestTimeout(),
                [=, this, &instances] () mutable {
                    const auto done = OnConductorResult(
                        i,
                        {group, {}, "timeout"},
                        *requestContext,
                        instances
                    );

                    if (done) {
                        promise.SetValue();
                    }
                }
            );
        }

        return promise;
    }

private:
    bool OnConductorResult(
        const ui32 i,
        const NNehUtil::TResult& result,
        NNehUtil::TRequestContext& requestContext,
        TInstanceList& instances)
    {
        using namespace NNehUtil;

        with_lock (requestContext.Lock) {
            if (requestContext.Status[i]) {
                return false;
            }

            requestContext.Status[i] = true;
            --requestContext.RequestCount;

            if (result.Error) {
                Counters.OnFetchError();
                STORAGE_ERROR(TStringBuilder()
                    << "conductor instance fetch error: " << result.Error.Quote()
                    << ", group: " << result.Group.Quote());
            } else {
                TStringBuf s(result.Data);
                THashSet<TStringBuf> hosts;
                TStringBuf host;
                while (s.NextTok('\n', host)) {
                    hosts.emplace(host);
                }

                with_lock (instances.Lock) {
                    THashSet<TStringBuf> deletedHosts;
                    THashSet<TStringBuf> oldHosts;
                    THashSet<TStringBuf> newHosts;

                    for (const auto& ii : instances.Instances) {
                        if (hosts.contains(ii.Host)) {
                            oldHosts.insert(ii.Host);
                        } else if (ii.Tag == result.Group) {
                            deletedHosts.insert(ii.Host);
                        }
                    }

                    for (auto host : hosts) {
                        if (!oldHosts.contains(host)) {
                            newHosts.insert(host);
                        }
                    }

                    EraseIf(instances.Instances, [&] (const auto& ii) {
                        return deletedHosts.contains(ii.Host);
                    });

                    auto e = instances.Instances.size();

                    if (Config->GetConductorSecureInstancePort()) {
                        instances.Instances.resize(e + 2*newHosts.size());
                    } else {
                        instances.Instances.resize(e + newHosts.size());
                    }

                    for (auto host : newHosts) {
                        auto& ii = instances.Instances[e];
                        ii.Host = host;
                        ii.Port = Config->GetConductorInstancePort();
                        ii.Tag = result.Group;
                        ++e;
                    }

                    if (Config->GetConductorSecureInstancePort()) {
                        for (auto host : newHosts) {
                            auto& ii = instances.Instances[e];
                            ii.Host = host;
                            ii.Port = Config->GetConductorSecureInstancePort();
                            ii.Tag = result.Group;
                            ii.IsSecurePort = true;
                            ++e;
                        }
                    }
                }
            }

            if (requestContext.RequestCount == 0) {
                with_lock (instances.Lock) {
                    Counters.SetInstanceCount(instances.Instances.size());
                }

                return true;
            }

            return false;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStaticInstanceFetcher final
    : public IInstanceFetcher
{
private:
    TDiscoveryConfigPtr Config;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;

    TLog Log;
    TInstanceFetcherCounters Counters;

public:
    TStaticInstanceFetcher(
            TDiscoveryConfigPtr config,
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring)
        : Config(std::move(config))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
    {
    }

public:
    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_DISCOVERY");
        auto counters = Monitoring->GetCounters();
        auto rootGroup = counters->GetSubgroup("counters", "blockstore");

        auto discoveryCounters = rootGroup->GetSubgroup("component", "discovery");
        auto fetcherCounters = rootGroup->GetSubgroup("subcomponent", "conductor-fetcher");
        Counters.Register(*fetcherCounters);
    }

    void Stop() override
    {
    }

    TFuture<void> FetchInstances(TInstanceList& instances) override
    {
        Counters.OnFetch();

        TIFStream data(Config->GetInstanceListFile());
        TString line;
        THashMap<std::pair<TString, ui16>, bool> newInstances;
        while (data.ReadLine(line)) {
            TStringBuf s(line);
            TStringBuf host, portStr;
            TStringBuf(line).Split('\t', host, portStr);

            TStringBuf secPortStr = portStr.SplitOff('\t');

            ui16 port;
            ui16 secPort = 0;
            if (!TryFromString(portStr, port) ||
                (secPortStr && !TryFromString(secPortStr, secPort)))
            {
                Counters.OnFetchError();
                STORAGE_ERROR(TStringBuilder()
                    << "static instance config parsing error, broken entry: "
                    << line.Quote());

                continue;
            }

            newInstances.emplace(std::make_pair(TString{host}, port), false);

            if (secPort) {
                newInstances.emplace(std::make_pair(TString{host}, secPort), true);
            }
        }

        with_lock (instances.Lock) {
            TVector<ui32> deletedIndices;
            for (ui32 i = 0; i < instances.Instances.size(); ++i) {
                auto& ii = instances.Instances[i];
                auto key = std::make_pair(ii.Host, ii.Port);
                if (newInstances.contains(key)) {
                    newInstances.erase(key);
                } else {
                    deletedIndices.push_back(i);
                }
            }

            DeleteAt(instances, deletedIndices);

            auto e = instances.Instances.size() - deletedIndices.size();
            instances.Instances.resize(e + newInstances.size());
            for (auto& [newInstance, secure] : newInstances) {
                auto& ii = instances.Instances[e];
                ii.Host = newInstance.first;
                ii.Port = newInstance.second;
                ii.IsSecurePort = secure;
                ++e;
            }

            Counters.SetInstanceCount(instances.Instances.size());
        }

        return MakeFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TInstanceFetcherStub final
    : public IInstanceFetcher
{
private:
    const TDeque<TInstanceInfo> Instances;

public:
    TInstanceFetcherStub(TDeque<TInstanceInfo> instances)
        : Instances(std::move(instances))
    {
    }

public:
    void Start() override
    {
    }

    void Stop() override
    {
    }

    TFuture<void> FetchInstances(TInstanceList& instances) override
    {
        with_lock (instances.Lock) {
            if (instances.Instances.empty()) {
                instances.Instances = Instances;
            }
        }

        return MakeFuture();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IInstanceFetcherPtr CreateConductorInstanceFetcher(
    TDiscoveryConfigPtr config,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    ITimerPtr timer,
    ISchedulerPtr scheduler)
{
    return std::make_shared<TConductorInstanceFetcher>(
        std::move(config),
        std::move(logging),
        std::move(monitoring),
        std::move(timer),
        std::move(scheduler)
    );
}

IInstanceFetcherPtr CreateStaticInstanceFetcher(
    TDiscoveryConfigPtr config,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring)
{
    return std::make_shared<TStaticInstanceFetcher>(
        std::move(config),
        std::move(logging),
        std::move(monitoring)
    );
}

IInstanceFetcherPtr CreateInstanceFetcherStub(
    TDeque<TInstanceInfo> instances)
{
    return std::make_shared<TInstanceFetcherStub>(std::move(instances));
}

}   // namespace NCloud::NBlockStore::NDiscovery
