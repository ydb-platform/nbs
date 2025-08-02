#include "disk_registry_based_partition_statistics_collector_actor.h"

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <contrib/ydb/library/actors/core/hfunc.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TDiskRegistryBasedPartitionStatisticsCollectorActor::
    TDiskRegistryBasedPartitionStatisticsCollectorActor(
        const TActorId& owner,
        TVector<TActorId> statActorIds)
    : Owner(owner)
    , StatActorIds(std::move(statActorIds))
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
            std::move(Response)));

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
            std::move(Response)));

    Die(ctx);
}

void TDiskRegistryBasedPartitionStatisticsCollectorActor::
    HandleGetDiskRegistryBasedPartCountersResponse(
        TEvNonreplPartitionPrivate::
            TEvGetDiskRegistryBasedPartCountersResponse::TPtr& ev,
        const TActorContext& ctx)
{
    auto* record = ev->Get();

    if (HasError(record->Error)) {
        Error = record->Error;
    }

    Response.Counters.push_back(std::move(*record));

    if (Response.Counters.size() == StatActorIds.size()) {
        SendStatistics(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDiskRegistryBasedPartitionStatisticsCollectorActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleTimeout);
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
