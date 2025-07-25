#include "partition_statistics_collector_actor.h"

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <contrib/ydb/library/actors/core/hfunc.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TPartitionStatisticsCollectorActor::TPartitionStatisticsCollectorActor(
        const TActorId& owner,
        TVector<TActorId> partitions)
    : Owner(owner)
    , Partitions(std::move(partitions))
{}

void TPartitionStatisticsCollectorActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    for (const auto& partition: Partitions) {
        auto request = std::make_unique<
            TEvPartitionCommonPrivate::TEvGetPartCountersRequest>();
        NCloud::Send(ctx, partition, std::move(request));
    }

    ctx.Schedule(UpdateCountersInterval, new TEvents::TEvWakeup());
}

void TPartitionStatisticsCollectorActor::SendStatToVolume(
    const TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvPartitionCommonPrivate::TEvPartCountersCombined>(
            std::move(Error),
            std::move(Response)));

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
        std::make_unique<TEvPartitionCommonPrivate::TEvPartCountersCombined>(
            MakeError(E_TIMEOUT, "Failed to update partition statistics"),
            std::move(Response)));

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
    TEvPartitionCommonPrivate::TEvGetPartCountersResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* record = ev->Get();

    if (HasError(record->Error)) {
        Error = record->Error;
        ++NumberResponsesWithError;
    } else {
        Response.PartCounters.push_back(std::move(*record));
    }

    if (Partitions.size() ==
        Response.PartCounters.size() + NumberResponsesWithError)
    {
        SendStatToVolume(ctx);
    }
}

STFUNC(TPartitionStatisticsCollectorActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleTimeout);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(
            TEvPartitionCommonPrivate::TEvGetPartCountersResponse,
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
