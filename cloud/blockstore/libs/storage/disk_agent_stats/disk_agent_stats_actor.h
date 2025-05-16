#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/actors/public.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Actor that fetches CPU wait stats periodically and updates the corresponding
// counter
class TDiskAgentStatsActor final
    : public NActors::TActorBootstrapped<TDiskAgentStatsActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const NCloud::NStorage::IStatsFetcherPtr StatsFetcher;

    NMonitoring::TDynamicCounters::TCounterPtr CpuWait;
    NMonitoring::TDynamicCounters::TCounterPtr CpuWaitFailure;

    TInstant LastCpuWaitQuery;

public:
    TDiskAgentStatsActor(
        TStorageConfigPtr storageConfig,
        NCloud::NStorage::IStatsFetcherPtr statsFetcher);

    ~TDiskAgentStatsActor() override = default;

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void RegisterCounters(const NActors::TActorContext& ctx);

    STFUNC(StateWork);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateDiskAgentStatsActor(
    TStorageConfigPtr storageConfig,
    NCloud::NStorage::IStatsFetcherPtr statsFetcher);

}   // namespace NCloud::NBlockStore::NStorage
