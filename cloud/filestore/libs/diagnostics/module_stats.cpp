#include "module_stats.h"

#include "filesystem_counters.h"

#include <cloud/filestore/libs/diagnostics/metrics/registry.h>

#include <cloud/storage/core/libs/common/timer.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/rwlock.h>

namespace NCloud::NFileStore {

using namespace NMonitoring;
using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TModuleStatsEntry
{
    // DynamicCounters are automatically deleted when the registry is deleted
    IMetricsRegistryPtr LocalMetricsRegistry;
    IMetricsRegistryPtr AggregatableMetricsRegistry;
    IModuleStatsPtr ModuleStats;
};

////////////////////////////////////////////////////////////////////////////////

struct TFileSystemStatsEntry
{
    IMainMetricsRegistryPtr FileSystemMetricsRegistry;

    // Key: ModuleName
    // Value: IModuleStatsPtr + metric registries
    THashMap<TString, TModuleStatsEntry> ModuleStatsMap;

    explicit TFileSystemStatsEntry(NMonitoring::TDynamicCountersPtr fsCounters)
        : FileSystemMetricsRegistry(
              CreateMetricsRegistry({}, std::move(fsCounters)))
    {}

    void UpdateStats(TInstant now) const
    {
        for (const auto& [_, moduleStatsEntry]: ModuleStatsMap) {
            moduleStatsEntry.ModuleStats->UpdateStats(now);
        }
        FileSystemMetricsRegistry->Update(now);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TModuleStatsRegistry final: public IModuleStatsRegistry
{
private:
    ITimerPtr Timer;
    IFsCountersProviderPtr FsCountersProvider;
    IMainMetricsRegistryPtr TotalMetricsRegistry;

    TRWMutex Lock;
    THashMap<std::pair<TString, TString>, TFileSystemStatsEntry> StatsMap;

public:
    TModuleStatsRegistry(
        ITimerPtr timer,
        IFsCountersProviderPtr fsCountersProvider,
        NMonitoring::TDynamicCountersPtr totalCounters)
        : Timer(std::move(timer))
        , FsCountersProvider(std::move(fsCountersProvider))
        , TotalMetricsRegistry(
              CreateMetricsRegistry({}, std::move(totalCounters)))
    {}

    void UpdateStats(bool updateIntervalFinished) override
    {
        Y_UNUSED(updateIntervalFinished);

        const auto now = Timer->Now();

        TReadGuard guard(Lock);

        for (const auto& [key, entry]: StatsMap) {
            entry.UpdateStats(now);
        }

        TotalMetricsRegistry->Update(now);
    }

    void Register(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId,
        IModuleStatsPtr stats) override
    {
        auto key = std::make_pair(fileSystemId, clientId);
        auto moduleName = TString(stats->GetName());

        TWriteGuard guard(Lock);

        auto it = StatsMap.find(key);
        if (it == StatsMap.end()) {
            auto counters = FsCountersProvider->Register(
                fileSystemId,
                clientId,
                cloudId,
                folderId);
            it = StatsMap.emplace(key, counters).first;
        }

        auto& fileSystemStatsEntry = it->second;

        Y_ABORT_UNLESS(
            !fileSystemStatsEntry.ModuleStatsMap.contains(moduleName),
            "Module %s is already registered for (fsId: %s, clientId: %s)",
            moduleName.c_str(),
            fileSystemId.c_str(),
            clientId.c_str());

        auto localMetricsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("module", moduleName)},
            fileSystemStatsEntry.FileSystemMetricsRegistry);

        auto totalMetricsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("module", moduleName)},
            TotalMetricsRegistry);

        auto aggregatableMetricsRegistry = CreateScopedMetricsRegistry(
            {},
            {std::move(totalMetricsRegistry), localMetricsRegistry});

        stats->RegisterCounters(
            *localMetricsRegistry,
            *aggregatableMetricsRegistry);

        fileSystemStatsEntry.ModuleStatsMap[moduleName] = {
            .LocalMetricsRegistry = std::move(localMetricsRegistry),
            .AggregatableMetricsRegistry =
                std::move(aggregatableMetricsRegistry),
            .ModuleStats = std::move(stats)};
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
    ITimerPtr timer,
    IFsCountersProviderPtr fsCountersProvider,
    NMonitoring::TDynamicCountersPtr totalCounters)
{
    return std::make_shared<TModuleStatsRegistry>(
        std::move(timer),
        std::move(fsCountersProvider),
        std::move(totalCounters));
}

IModuleStatsRegistryPtr CreateModuleStatsRegistryStub()
{
    return std::make_shared<TModuleStatsRegistryStub>();
}

}   // namespace NCloud::NFileStore
