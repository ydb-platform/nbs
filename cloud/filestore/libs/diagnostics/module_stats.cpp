#include "module_stats.h"

#include "filesystem_counters.h"

#include <cloud/filestore/libs/diagnostics/metrics/registry.h>

#include <cloud/storage/core/libs/common/timer.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

namespace NCloud::NFileStore {

using namespace NMonitoring;
using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

// (TFileSystemId, TClientId)
using TFileSystemKey = std::pair<TString, TString>;

////////////////////////////////////////////////////////////////////////////////

struct TFileSystemMetricsRegistryEntry
{
    const IMainMetricsRegistryPtr FileSystemMetricsRegistry;
    ui64 RefCount = 0;

    explicit TFileSystemMetricsRegistryEntry(
        NMonitoring::TDynamicCountersPtr fsCounters)
        : FileSystemMetricsRegistry(
              CreateMetricsRegistry({}, std::move(fsCounters)))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TModuleStatsEntry
{
    TString FileSystemId;
    TString ClientId;

    // DynamicCounters are automatically deleted when all registries referencing
    // them are deleted
    IMetricsRegistryPtr LocalMetricsRegistry;
    IMetricsRegistryPtr AggregatableMetricsRegistry;
    IModuleStatsPtr ModuleStats;
};

////////////////////////////////////////////////////////////////////////////////

class TModuleStatsRegistry final: public IModuleStatsRegistry
{
private:
    ITimerPtr Timer;
    IFsCountersProviderPtr FsCountersProvider;
    IMainMetricsRegistryPtr TotalMetricsRegistry;

    TAdaptiveLock Lock;

    // Map: Fs -> local metrics registry + ref count
    THashMap<TFileSystemKey, TFileSystemMetricsRegistryEntry>
        FileSystemMetricsRegistries;

    // Map: SessionId -> registered module metrics per session
    THashMap<TString, TVector<TModuleStatsEntry>> StatsPerSession;

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

        TGuard guard(Lock);

        // Update all module stats (recalculate IMetricPtr values)
        for (const auto& [_, moduleStatsVector]: StatsPerSession) {
            for (const auto& moduleStatsEntry: moduleStatsVector) {
                moduleStatsEntry.ModuleStats->UpdateStats(now);
            }
        }

        // Propagate updated values to TDynamicCounters
        for (const auto& [_, fileSystemEntry]: FileSystemMetricsRegistries) {
            fileSystemEntry.FileSystemMetricsRegistry->Update(now);
        }

        TotalMetricsRegistry->Update(now);
    }

    void Register(TModuleStatsRegisterArgs args) override
    {
        // Module can be registered multiple times for the same filesystem.
        // This may happen when migration within the same node occurs.

        auto moduleName = TString(args.ModuleStats->GetName());

        TGuard guard(Lock);

        auto fileSystemMetricsRegistry =
            RegisterFileSystemCountersAndGetMetricsRegistry(
                args.FileSystemId,
                args.ClientId,
                args.CloudId,
                args.FolderId);

        auto localMetricsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("module", moduleName)},
            fileSystemMetricsRegistry);

        auto totalMetricsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("module", moduleName)},
            TotalMetricsRegistry);

        auto aggregatableMetricsRegistry = CreateScopedMetricsRegistry(
            {},
            {std::move(totalMetricsRegistry), localMetricsRegistry});

        args.ModuleStats->RegisterCounters(
            *localMetricsRegistry,
            *aggregatableMetricsRegistry);

        StatsPerSession[args.SessionId].push_back(
            {.FileSystemId = args.FileSystemId,
             .ClientId = args.ClientId,
             .LocalMetricsRegistry = std::move(localMetricsRegistry),
             .AggregatableMetricsRegistry =
                 std::move(aggregatableMetricsRegistry),
             .ModuleStats = std::move(args.ModuleStats)});
    }

    void Unregister(const TString& sessionId) override
    {
        TGuard guard(Lock);

        auto it = StatsPerSession.find(sessionId);
        if (it == StatsPerSession.end()) {
            return;
        }

        for (const auto& moduleStatsEntry: it->second) {
            UnregisterFileSystemCounters(
                moduleStatsEntry.FileSystemId,
                moduleStatsEntry.ClientId);
        }

        StatsPerSession.erase(it);
    }

private:
    IMainMetricsRegistryPtr RegisterFileSystemCountersAndGetMetricsRegistry(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId)
    {
        TFileSystemKey key{fileSystemId, clientId};

        auto it = FileSystemMetricsRegistries.find(key);
        if (it == FileSystemMetricsRegistries.end()) {
            auto counters = FsCountersProvider->Register(
                fileSystemId,
                clientId,
                cloudId,
                folderId);
            it = FileSystemMetricsRegistries.emplace(key, counters).first;
        }
        it->second.RefCount++;
        return it->second.FileSystemMetricsRegistry;
    }

    void UnregisterFileSystemCounters(
        const TString& fileSystemId,
        const TString& clientId)
    {
        TFileSystemKey key{fileSystemId, clientId};

        auto it = FileSystemMetricsRegistries.find(key);
        if (it == FileSystemMetricsRegistries.end()) {
            return;
        }

        auto& fileSystemStatsEntry = it->second;
        fileSystemStatsEntry.RefCount--;

        if (fileSystemStatsEntry.RefCount == 0) {
            FsCountersProvider->Unregister(fileSystemId, clientId);
            FileSystemMetricsRegistries.erase(it);
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

    void Register(TModuleStatsRegisterArgs args) override
    {
        Y_UNUSED(args);
    }

    void Unregister(const TString& sessionId) override
    {
        Y_UNUSED(sessionId);
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
