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
        auto request =
            std::make_unique<TEvNonreplPartitionPrivate::
                                 TEvGetDiskRegistryBasedPartCountersRequest>();

        auto event = std::make_unique<IEventHandle>(
            statActorId,
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            0,  // cookie
            &ctx.SelfID   // forwardOnNondelivery
        );

        ctx.Send(event.release());
    }

    ctx.Schedule(UpdateCountersInterval, new TEvents::TEvWakeup());
}

void TDiskRegistryBasedPartitionStatisticsCollectorActor::ReplyAndDie(
    const TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvNonreplPartitionPrivate::
                             TEvDiskRegistryBasedPartCountersCombined>(
            std::move(LastError),
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
    auto* msg = ev->Get();

    if (HasError(msg->Error)) {
        LastError = msg->Error;
    }

    Response.Counters.push_back(std::move(*msg));

    ++ResponsesCount;
    if (ResponsesCount == StatActorIds.size()) {
        ReplyAndDie(ctx);
    }
}

void TDiskRegistryBasedPartitionStatisticsCollectorActor::
    HandleGetDiskRegitryBasedPartCountersUndelivery(
        TEvNonreplPartitionPrivate::TEvGetDiskRegistryBasedPartCountersRequest::
            TPtr& ev,
        const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ++ResponsesCount;
    if (ResponsesCount == StatActorIds.size()) {
        ReplyAndDie(ctx);
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
            HandleGetDiskRegistryBasedPartCountersResponse);
        HFunc(
            TEvNonreplPartitionPrivate::
                TEvGetDiskRegistryBasedPartCountersRequest,
            HandleGetDiskRegitryBasedPartCountersUndelivery);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
