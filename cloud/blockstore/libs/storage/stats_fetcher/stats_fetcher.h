#pragma once

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/diagnostics/stats_fetcher.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TStatsFetcherActor final
    : public NActors::TActorBootstrapped<TStatsFetcherActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const NCloud::NStorage::IStatsFetcherPtr StatsFetcher;

    NMonitoring::TDynamicCounters::TCounterPtr CpuWaitCounter;

    TMonotonic LastCpuWaitTs;

public:
    TStatsFetcherActor(
        TStorageConfigPtr storageConfig,
        NCloud::NStorage::IStatsFetcherPtr statsFetcher);

    TStatsFetcherActor() = default;

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void RegisterCounters(const NActors::TActorContext& ctx);

    STFUNC(StateWork);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateStatsFetcherActor(
    TStorageConfigPtr storageConfig,
    NCloud::NStorage::IStatsFetcherPtr statFetcher);

}   // namespace NCloud::NBlockStore::NStorage
