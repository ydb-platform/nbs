#include "cpu_stat_actor.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/core/base/appdata_fwd.h>

namespace {

////////////////////////////////////////////////////////////////////////////////

// Timeout for fetching CPU wait stats
constexpr TDuration CpuFetchWakeupTimeout = TDuration::Seconds(15);
// Mulitiplier for converting CPU wait time to percents
constexpr ui32 CpuLackPercentsMultiplier = 100;

}   // namespace

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TCpuStatsFetcherActor::TCpuStatsFetcherActor(
    TStorageConfigPtr storageConfig,
    NCloud::NStorage::IStatsFetcherPtr statsFetcher)
    : StorageConfig(std::move(storageConfig))
    , StatsFetcher(std::move(statsFetcher))
{}

// @brief Initializes the actor
// @param ctx Actor context
void TCpuStatsFetcherActor::Bootstrap(const NKikimr::TActorContext& ctx)
{
    RegisterCounters(ctx);
    Become(&TThis::StateWork);
}

// @brief Registers counters in the monitoring system and schedules the first
// wakeup
// @param ctx Actor context
void TCpuStatsFetcherActor::RegisterCounters(const NKikimr::TActorContext& ctx)
{
    auto counters = NKikimr::AppData(ctx)->Counters;
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    auto serverCounters = rootGroup->GetSubgroup("component", "server");

    CpuWait = serverCounters->GetCounter("CpuWait", false);
    CpuWaitFailure = serverCounters->GetCounter("CpuWaitFailure", false);

    // Schedule the first wakeup event
    ctx.Schedule(CpuFetchWakeupTimeout, new NKikimr::TEvents::TEvWakeup);
}

// @brief Handles the wakeup event
// @param ev Event pointer
// @param ctx Actor context
// @details This method is called periodically to fetch CPU wait stats
void TCpuStatsFetcherActor::HandleWakeup(
    const NKikimr::TEvents::TEvWakeup::TPtr& ev,
    const NKikimr::TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto now = ctx.Now();

    auto interval = (now - LastCpuWaitQuery).MicroSeconds();
    auto [cpuWait, error] = StatsFetcher->GetCpuWait();
    if (HasError(error)) {
        *CpuWaitFailure = 1;
        LOG_TRACE_S(
            ctx,
            TBlockStoreComponents::STATS_SERVICE,
            "Failed to get CpuWait stats: " << error);
    } else {
        *CpuWaitFailure = 0;
    }
    auto cpuLack = CpuLackPercentsMultiplier * cpuWait.MicroSeconds();
    cpuLack /= interval;
    *CpuWait = cpuLack;

    LastCpuWaitQuery = now;

    if (cpuLack >= StorageConfig->GetCpuLackThreshold()) {
        LOG_WARN_S(
            ctx,
            TBlockStoreComponents::STATS_SERVICE,
            "Cpu wait is " << cpuLack);
    }

    // Schedule the next wakeup event
    ctx.Schedule(CpuFetchWakeupTimeout, new NKikimr::TEvents::TEvWakeup());
}

////////////////////////////////////////////////////////////////////////////////

// Bind events to handlers
STFUNC(TCpuStatsFetcherActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(NKikimr::TEvents::TEvWakeup, HandleWakeup);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::STATS_SERVICE);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
