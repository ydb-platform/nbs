#pragma once

#include "public.h"

#include "cloud/blockstore/libs/storage/core/public.h"
#include "contrib/ydb/library/actors/core/actor_bootstrapped.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Actor that fetches CPU wait stats periodically
// and updates the corresponding counter.
class TCpuStatsFetcherActor final
    : public NActors::TActorBootstrapped<TCpuStatsFetcherActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const NCloud::NStorage::IStatsFetcherPtr StatsFetcher;

    NMonitoring::TDynamicCounters::TCounterPtr CpuWait;
    NMonitoring::TDynamicCounters::TCounterPtr CpuWaitFailure;

    TInstant LastCpuWaitQuery;

public:
    TCpuStatsFetcherActor(
        TStorageConfigPtr storageConfig,
        NCloud::NStorage::IStatsFetcherPtr statsFetcher);

    ~TCpuStatsFetcherActor() override = default;

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void RegisterCounters(const NActors::TActorContext& ctx);

    STFUNC(StateWork);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
