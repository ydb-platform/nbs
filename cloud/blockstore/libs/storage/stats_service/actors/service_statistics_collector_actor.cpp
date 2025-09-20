#include "service_statistics_collector_actor.h"

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <contrib/ydb/library/actors/core/hfunc.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TServiceStatisticsCollectorActor::TServiceStatisticsCollectorActor(
        const TActorId& owner,
        TVector<TActorId> volumeActorIds)
    : Owner(owner)
    , VolumeActorIds(std::move(volumeActorIds))
{}

void TServiceStatisticsCollectorActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    for (const auto& volumeActorId: VolumeActorIds) {
        NCloud::Send(
            ctx,
            volumeActorId,
            std::make_unique<
                TEvStatsService::TEvGetServiceStatisticsRequest>());
    }

    ctx.Schedule(UpdateCountersInterval, new TEvents::TEvWakeup());
}

void TServiceStatisticsCollectorActor::SendStatistics(const TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvStatsService::TEvServiceStatisticsCombined>(
            std::move(LastError),
            std::move(Response)));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TServiceStatisticsCollectorActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvStatsService::TEvServiceStatisticsCombined>(
            MakeError(E_TIMEOUT, "Failed to update service statistics."),
            std::move(Response)));

    Die(ctx);
}

void TServiceStatisticsCollectorActor::HandleGetServiceStatisticsResponse(
    TEvStatsService::TEvGetServiceStatisticsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->Error)) {
        LastError = msg->Error;
    }

    Response.Counters.push_back(std::move(*msg));

    if (Response.Counters.size() == VolumeActorIds.size()) {
        SendStatistics(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TServiceStatisticsCollectorActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleTimeout);
        HFunc(
            TEvStatsService::TEvGetServiceStatisticsResponse,
            HandleGetServiceStatisticsResponse)

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
