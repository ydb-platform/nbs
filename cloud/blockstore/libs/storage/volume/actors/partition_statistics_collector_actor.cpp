#include "partition_statistics_collector_actor.h"

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TPartitionStatisticsCollectorActor::TPartitionStatisticsCollectorActor(
        const TActorId& owner,
        const TPartitionInfoList& partitions)
    : Owner(owner)
    , Partitions(partitions)
{}

void TPartitionStatisticsCollectorActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    for (const auto& partition: Partitions) {
        auto request =
            std::make_unique<TEvStatsService::TEvGetPartCountersRequest>();
        NCloud::Send(ctx, partition.GetTopActorId(), std::move(request));
    }

    ctx.Schedule(TimeoutUpdateCountersInterval, new TEvents::TEvWakeup());
}

void TPartitionStatisticsCollectorActor::SendStatToVolume(
    const TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvStatsService::TEvUpdatedAllPartCounters>(
            std::move(PartCounters)));

    Die(ctx);
}

void TPartitionStatisticsCollectorActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvStatsService::TEvUpdatedAllPartCounters>(
            MakeError(E_TIMEOUT, "Failed to update partition statistics"),
            std::move(PartCounters)));

    Die(ctx);
}

void TPartitionStatisticsCollectorActor::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    Die(ctx);
}

void TPartitionStatisticsCollectorActor::HandleGetPartCountersResponse(
    TEvStatsService::TEvGetPartCountersResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* record = ev->Get();

    PartCounters.emplace_back(
        record->VolumeSystemCpu,
        record->VolumeUserCpu,
        std::move(record->DiskCounters),
        std::move(record->BlobLoadMetrics),
        std::move(record->TabletMetrics),
        record->PartActorId);

    if (Partitions.size() == PartCounters.size()) {
        SendStatToVolume(ctx);
    }
}

STFUNC(TPartitionStatisticsCollectorActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleTimeout);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(
            TEvStatsService::TEvGetPartCountersResponse,
            HandleGetPartCountersResponse)

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
