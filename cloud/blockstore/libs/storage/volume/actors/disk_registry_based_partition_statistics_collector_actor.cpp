#include "disk_registry_based_partition_statistics_collector_actor.h"

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TDiskRegistryBasedPartitionStatisticsCollectorActor::
    TDiskRegistryBasedPartitionStatisticsCollectorActor(
        const TActorId& owner,
        TVector<TActorId> statActorIds)
    : Owner(owner)
    , StatActorIds(std::move(statActorIds))
    , Error(MakeError(S_OK))
{}

void TDiskRegistryBasedPartitionStatisticsCollectorActor::Bootstrap(
    const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    for (const auto& statActorId: StatActorIds) {
        NCloud::Send(
            ctx,
            statActorId,
            std::make_unique<TEvNonreplPartitionPrivate::
                                 TEvGetDiskRegistryBasedPartCountersRequest>());
    }

    ctx.Schedule(UpdateCountersInterval, new TEvents::TEvWakeup());
}

void TDiskRegistryBasedPartitionStatisticsCollectorActor::SendStatistics(
    const TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvNonreplPartitionPrivate::
                             TEvDiskRegistryBasedPartCountersCombined>(
            std::move(Error),
            std::move(Counters)));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryBasedPartitionStatisticsCollectorActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvNonreplPartitionPrivate::
                             TEvDiskRegistryBasedPartCountersCombined>(
            MakeError(
                E_TIMEOUT,
                "Failed to update disk registry based partition counters."),
            std::move(Counters)));

    Die(ctx);
}

void TDiskRegistryBasedPartitionStatisticsCollectorActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Die(ctx);
}

void TDiskRegistryBasedPartitionStatisticsCollectorActor::
    HandleGetDiskRegistryBasedPartCountersResponse(
        TEvNonreplPartitionPrivate::
            TEvGetDiskRegistryBasedPartCountersResponse::TPtr& ev,
        const TActorContext& ctx)
{
    auto* record = ev->Get();

    Counters.emplace_back(
        std::move(record->DiskCounters),
        record->NetworkBytes,
        record->CpuUsage,
        record->SelfId,
        record->DiskId);

    if (HasError(record->Error)) {
        Error = record->Error;
    }

    if (Counters.size() == StatActorIds.size()) {
        SendStatistics(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDiskRegistryBasedPartitionStatisticsCollectorActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleTimeout);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(
            TEvNonreplPartitionPrivate::
                TEvGetDiskRegistryBasedPartCountersResponse,
            HandleGetDiskRegistryBasedPartCountersResponse)

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
