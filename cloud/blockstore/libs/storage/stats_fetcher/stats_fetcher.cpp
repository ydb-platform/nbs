#include "stats_fetcher.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <contrib/ydb/core/base/appdata.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using namespace NKikimr;

namespace {

constexpr TDuration FetchStatsPeriod = TDuration::Seconds(15);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TStatsFetcherActor::TStatsFetcherActor(
        TStorageConfigPtr storageConfig,
        NCloud::NStorage::IStatsFetcherPtr statsFetcher)
    : StorageConfig(std::move(storageConfig))
    , StatsFetcher(std::move(statsFetcher))
{}

void TStatsFetcherActor::Bootstrap(const TActorContext& ctx)
{
    RegisterCounters(ctx);
    Become(&TThis::StateWork);

    ctx.Schedule(FetchStatsPeriod, new TEvents::TEvWakeup);
}

void TStatsFetcherActor::RegisterCounters(const TActorContext& ctx)
{
    auto counters = AppData(ctx)->Counters;
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    auto serverCounters = rootGroup->GetSubgroup("component", "server");

    CpuWaitCounter = serverCounters->GetCounter("CpuWait", false);
}

void TStatsFetcherActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto [cpuWait, error] = StatsFetcher->GetCpuWait();
    if (HasError(error)) {
        auto errorMessage = ReportCpuWaitCounterReadError(error.GetMessage());
        LOG_WARN_S(
            ctx,
            TBlockStoreComponents::STATS_SERVICE,
            "Failed to get CpuWait stats: " << errorMessage);
    }

    auto now = ctx.Now();
    auto intervalUs = (now - LastCpuWaitTs).MicroSeconds();
    auto cpuLack = 100 * cpuWait.MicroSeconds();
    cpuLack /= intervalUs;
    *CpuWaitCounter = cpuLack;

    LastCpuWaitTs = now;

    if (cpuLack >= StorageConfig->GetCpuLackThreshold()) {
        LOG_WARN_S(
            ctx,
            TBlockStoreComponents::STATS_SERVICE,
            "Cpu wait is " << cpuLack);
    }

    ctx.Schedule(FetchStatsPeriod, new TEvents::TEvWakeup());
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TStatsFetcherActor::StateWork)
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

IActorPtr CreateStatsFetcherActor(
    TStorageConfigPtr storageConfig,
    NCloud::NStorage::IStatsFetcherPtr statFetcher)
{
    return std::make_unique<TStatsFetcherActor>(
        std::move(storageConfig),
        std::move(statFetcher));
}

}   // namespace NCloud::NBlockStore::NStorage
