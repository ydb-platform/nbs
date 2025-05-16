#include "disk_agent_stats_actor.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/core/base/appdata_fwd.h>

namespace {

////////////////////////////////////////////////////////////////////////////////

// Timeout for fetching CPU wait stats
constexpr TDuration CpuFetchPeriod = TDuration::Seconds(15);

}   // namespace

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TDiskAgentStatsActor::TDiskAgentStatsActor(
    TStorageConfigPtr storageConfig,
    NCloud::NStorage::IStatsFetcherPtr statsFetcher)
    : StorageConfig(std::move(storageConfig))
    , StatsFetcher(std::move(statsFetcher))
{}

void TDiskAgentStatsActor::Bootstrap(const TActorContext& ctx)
{
    RegisterCounters(ctx);
    Become(&TThis::StateWork);
}

void TDiskAgentStatsActor::RegisterCounters(const TActorContext& ctx)
{
    auto counters = AppData(ctx)->Counters;
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    auto serverCounters = rootGroup->GetSubgroup("component", "server");

    CpuWait = serverCounters->GetCounter("CpuWait", false);
    CpuWaitFailure = serverCounters->GetCounter("CpuWaitFailure", false);

    ctx.Schedule(CpuFetchPeriod, new TEvents::TEvWakeup);
}

void TDiskAgentStatsActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto now = ctx.Now();

    auto interval = (now - LastCpuWaitQuery).MicroSeconds();
    auto [cpuWait, error] = StatsFetcher->GetCpuWait();
    if (HasError(error)) {
        *CpuWaitFailure = 1;
        LOG_WARN_S(
            ctx,
            TBlockStoreComponents::STATS_SERVICE,
            "Failed to get CpuWait stats: " << error);
    } else {
        *CpuWaitFailure = 0;
    }

    auto cpuLack = 100 * cpuWait.MicroSeconds();
    cpuLack /= interval;
    *CpuWait = cpuLack;

    LastCpuWaitQuery = now;

    if (cpuLack >= StorageConfig->GetCpuLackThreshold()) {
        LOG_WARN_S(
            ctx,
            TBlockStoreComponents::STATS_SERVICE,
            "Cpu wait is " << cpuLack);
    }

    ctx.Schedule(CpuFetchPeriod, new TEvents::TEvWakeup());
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDiskAgentStatsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::STATS_SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateDiskAgentStatsActor(
    TStorageConfigPtr storageConfig,
    NCloud::NStorage::IStatsFetcherPtr statsFetcher)
{
    return std::make_unique<TDiskAgentStatsActor>(
        std::move(storageConfig),
        std::move(statsFetcher));
}

}   // namespace NCloud::NBlockStore::NStorage
