#include "ban.h"

#include "config.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/spinlock.h>

using namespace NMonitoring;

namespace NCloud::NBlockStore::NDiscovery {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBanListCounters
{
    TDynamicCounters::TCounterPtr UpdateCount;
    TDynamicCounters::TCounterPtr UpdateErrors;
    TDynamicCounters::TCounterPtr BannedCount;

    void Register(TDynamicCounters& counters)
    {
        UpdateCount = counters.GetCounter("UpdateCount", true);
        UpdateErrors = counters.GetCounter("UpdateErrors", true);
        BannedCount = counters.GetCounter("BannedCount");
    }

    void OnUpdate()
    {
        UpdateCount->Inc();
    }

    void OnUpdateError()
    {
        UpdateErrors->Inc();
    }

    void SetBannedCount(int count)
    {
        *BannedCount = count;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBanList final: public IBanList
{
private:
    TDiscoveryConfigPtr Config;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;

    TLog Log;
    TBanListCounters Counters;

    THashSet<std::pair<TString, ui16>> Instances;
    TAdaptiveLock Lock;

public:
    TBanList(
        TDiscoveryConfigPtr config,
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring)
        : Config(std::move(config))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
    {}

public:
    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_DISCOVERY");
        auto counters = Monitoring->GetCounters();
        auto rootGroup = counters->GetSubgroup("counters", "blockstore");

        auto discoveryCounters =
            rootGroup->GetSubgroup("component", "discovery");
        auto banListCounters =
            rootGroup->GetSubgroup("subcomponent", "banlist");
        Counters.Register(*banListCounters);
    }

    void Stop() override
    {}

    void Update() override
    {
        Counters.OnUpdate();

        THashSet<std::pair<TString, ui16>> instances;
        TIFStream is(Config->GetBannedInstanceListFile());
        TString line;
        while (is.ReadLine(line)) {
            TStringBuf host, portStr;
            TStringBuf(line).Split('\t', host, portStr);
            ui16 port;
            if (TryFromString(portStr, port)) {
                instances.insert(std::make_pair(TString{host}, port));
            } else {
                Counters.OnUpdateError();
                STORAGE_ERROR(
                    TStringBuilder()
                    << "broken banlist entry: " << line.Quote());
            }
        }

        with_lock (Lock) {
            Instances.swap(instances);
            Counters.SetBannedCount(Instances.size());
        }
    }

    bool IsBanned(const TString& host, ui16 port) const override
    {
        with_lock (Lock) {
            return Instances.contains(std::make_pair(host, port));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBanListStub final: public IBanList
{
private:
    TDeque<std::pair<TString, ui16>> Instances;

public:
    TBanListStub(TDeque<std::pair<TString, ui16>> instances)
        : Instances(std::move(instances))
    {}

public:
    void Start() override
    {}

    void Stop() override
    {}

    void Update() override
    {}

    bool IsBanned(const TString& host, ui16 port) const override
    {
        return FindIf(
                   Instances.begin(),
                   Instances.end(),
                   [&](const auto& i) {
                       return host == i.first && port == i.second;
                   }) != Instances.end();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBanListPtr CreateBanList(
    TDiscoveryConfigPtr config,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring)
{
    return std::make_shared<TBanList>(
        std::move(config),
        std::move(logging),
        std::move(monitoring));
}

IBanListPtr CreateBanListStub(TDeque<std::pair<TString, ui16>> instances)
{
    return std::make_shared<TBanListStub>(std::move(instances));
}

}   // namespace NCloud::NBlockStore::NDiscovery
