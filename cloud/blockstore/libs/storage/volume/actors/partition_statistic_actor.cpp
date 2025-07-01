#include "partition_statistic_actor.h"

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsForAllPartitionsStatUpdate(
    const THashMap<TActorId, bool>& isUpdatedPartitionCounters)
{
    for (auto [_, isUpdate]: isUpdatedPartitionCounters) {
        if (!isUpdate) {
            return false;
        }
    }
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TPartitionStatisticActor::TPartitionStatisticActor(
        const TActorId& owner,
        const TPartitionInfoList& partitions)
    : Owner(owner)
    , Partitions(partitions)
{}

void TPartitionStatisticActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    for (const auto& partition: Partitions) {
        auto partActorId = partition.GetTopActorId();
        IsUpdatedPartitionCounters[partActorId] = false;
        auto request =
            std::make_unique<TEvStatsService::TEvUpdatePartCountersRequest>();
        NCloud::Send(ctx, partActorId, std::move(request));
    }

    ctx.Schedule(UpdateCountersInterval, new TEvents::TEvWakeup());
}

void TPartitionStatisticActor::SendStatToVolume(const TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvStatsService::TEvUpdatedAllPartCounters>(
            std::move(PartCounters)));

    Die(ctx);
}

void TPartitionStatisticActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    TStringBuilder builder;

    builder << "Failed to update partition statistics. Can't get response from "
               "partitions: ";

    for (const auto& [partActorId, isGetResponse]: IsUpdatedPartitionCounters) {
        if (!isGetResponse) {
            builder << partActorId << " ";
        }
    }

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvStatsService::TEvUpdatedAllPartCounters>(
            MakeError(E_TIMEOUT, builder.c_str()),
            std::move(PartCounters)));

    Die(ctx);
}

void TPartitionStatisticActor::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    Die(ctx);
}

void TPartitionStatisticActor::HandleUpdatePartCountersResponse(
    TEvStatsService::TEvUpdatePartCountersResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto* record = ev->Get();

    PartCounters.emplace_back(
        record->VolumeSystemCpu,
        record->VolumeUserCpu,
        std::move(record->DiskCounters),
        std::move(record->BlobLoadMetrics),
        std::move(record->TabletMetrics),
        record->PartActorId);

    IsUpdatedPartitionCounters[ev->Sender] = true;
    if (IsForAllPartitionsStatUpdate(IsUpdatedPartitionCounters)) {
        SendStatToVolume(ctx);
    }
}

STFUNC(TPartitionStatisticActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleTimeout);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(
            TEvStatsService::TEvUpdatePartCountersResponse,
            HandleUpdatePartCountersResponse)

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
