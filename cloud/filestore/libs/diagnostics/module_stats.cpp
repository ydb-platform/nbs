#include "module_stats.h"

#include "filesystem_counters.h"

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/rwlock.h>

namespace NCloud::NFileStore {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TModuleStatsEntry
{
    TDynamicCountersPtr Counters;
    TVector<IModuleStatsPtr> StatsList;
};

class TModuleStatsRegistry final: public IModuleStatsRegistry
{
private:
    IFsCountersProviderPtr FsCountersProvider;

    TRWMutex Lock;
    THashMap<std::pair<TString, TString>, TModuleStatsEntry> StatsMap;

public:
    explicit TModuleStatsRegistry(IFsCountersProviderPtr fsCountersProvider)
        : FsCountersProvider(std::move(fsCountersProvider))
    {}

    void UpdateStats(bool updateIntervalFinished) override
    {
        Y_UNUSED(updateIntervalFinished);

        TReadGuard guard(Lock);

        for (const auto& [key, entry]: StatsMap) {
            for (const auto& stats: entry.StatsList) {
                stats->UpdateStats();
            }
        }
    }

    void Register(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId,
        IModuleStatsPtr stats) override
    {
        auto key = std::make_pair(fileSystemId, clientId);

        TWriteGuard guard(Lock);

        auto it = StatsMap.find(key);
        if (it == StatsMap.end()) {
            auto counters = FsCountersProvider->Register(
                fileSystemId,
                clientId,
                cloudId,
                folderId);
            it = StatsMap.emplace(key, TModuleStatsEntry{counters, {}}).first;
        }

        // First create a placeholder subgroup, then replace it with the actual
        // counters (same pattern as in filesystem_counters.cpp)
        it->second.Counters->GetSubgroup("module", TString{stats->GetName()});
        it->second.Counters->ReplaceSubgroup(
            "module",
            TString{stats->GetName()},
            stats->GetCounters());
        it->second.StatsList.push_back(std::move(stats));
    }

    void Unregister(
        const TString& fileSystemId,
        const TString& clientId) override
    {
        TWriteGuard guard(Lock);

        auto key = std::make_pair(fileSystemId, clientId);
        if (StatsMap.erase(key)) {
            FsCountersProvider->Unregister(fileSystemId, clientId);
        }
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

    void Register(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId,
        IModuleStatsPtr stats) override
    {
        Y_UNUSED(fileSystemId);
        Y_UNUSED(clientId);
        Y_UNUSED(cloudId);
        Y_UNUSED(folderId);
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
    IFsCountersProviderPtr fsCountersProvider)
{
    return std::make_shared<TModuleStatsRegistry>(
        std::move(fsCountersProvider));
}

IModuleStatsRegistryPtr CreateModuleStatsRegistryStub()
{
    return std::make_shared<TModuleStatsRegistryStub>();
}

}   // namespace NCloud::NFileStore
