#include "module_stats.h"

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/rwlock.h>

namespace NCloud::NFileStore {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TModuleStatsRegistry final
    : public IModuleStatsRegistry
{
private:
    TString Component;
    TDynamicCountersPtr RootCounters;

    TDynamicCountersPtr FsCounters;

    TRWMutex Lock;
    THashMap<std::pair<TString, TString>, TVector<IModuleStatsPtr>> StatsMap;

public:
    TModuleStatsRegistry(
        TString component,
        TDynamicCountersPtr rootCounters)
        : Component(std::move(component))
        , RootCounters(std::move(rootCounters))
    {
        FsCounters = RootCounters->GetSubgroup("component", Component + "_fs")
                         ->GetSubgroup("host", "cluster");
    }

    void UpdateStats(bool updateIntervalFinished) override
    {
        Y_UNUSED(updateIntervalFinished);

        TReadGuard guard(Lock);

        for (const auto& [key, statsList]: StatsMap) {
            for (const auto& stats: statsList) {
                stats->UpdateStats();
            }
        }
    }

    TDynamicCountersPtr GetFileSystemModuleCounters(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId,
        const TString& moduleName) override
    {
        return FsCounters->GetSubgroup("filesystem", fileSystemId)
            ->GetSubgroup("client", clientId)
            ->GetSubgroup("cloud", cloudId)
            ->GetSubgroup("folder", folderId)
            ->GetSubgroup("module", moduleName);
    }

    void Register(
        const TString& fileSystemId,
        const TString& clientId,
        IModuleStatsPtr stats) override
    {
        TWriteGuard guard(Lock);
        auto key = std::make_pair(fileSystemId, clientId);
        StatsMap[key].push_back(std::move(stats));
    }

    void Unregister(
        const TString& fileSystemId,
        const TString& clientId) override
    {
        TWriteGuard guard(Lock);

        auto key = std::make_pair(fileSystemId, clientId);
        StatsMap.erase(key);

        FsCounters->GetSubgroup("filesystem", fileSystemId)
            ->RemoveSubgroup("client", clientId);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TModuleStatsRegistryStub final: public IModuleStatsRegistry
{
public:
    void UpdateStats(bool updateIntervalFinished) override
    {
        Y_UNUSED(updateIntervalFinished);
    }

    TDynamicCountersPtr GetFileSystemModuleCounters(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId,
        const TString& moduleName) override
    {
        Y_UNUSED(fileSystemId);
        Y_UNUSED(clientId);
        Y_UNUSED(cloudId);
        Y_UNUSED(folderId);
        Y_UNUSED(moduleName);
        return MakeIntrusive<TDynamicCounters>();
    }

    void Register(
        const TString& fileSystemId,
        const TString& clientId,
        IModuleStatsPtr stats) override
    {
        Y_UNUSED(fileSystemId);
        Y_UNUSED(clientId);
        Y_UNUSED(stats);
    }

    void Unregister(
        const TString& fileSystemId,
        const TString& clientId) override
    {
        Y_UNUSED(fileSystemId);
        Y_UNUSED(clientId);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IModuleStatsRegistryPtr CreateModuleStatsRegistry(
    TString component,
    TDynamicCountersPtr rootCounters)
{
    return std::make_shared<TModuleStatsRegistry>(
        std::move(component),
        std::move(rootCounters));
}

IModuleStatsRegistryPtr CreateModuleStatsRegistryStub()
{
    return std::make_shared<TModuleStatsRegistryStub>();
}

}   // namespace NCloud::NFileStore
