#include "stats_service_actor_pull_scheme.h"

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
        auto request =
            std::make_unique<TEvStatsService::TEvGetServiceStatisticsRequest>();

        auto event = std::make_unique<IEventHandle>(
            volumeActorId,
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

void TServiceStatisticsCollectorActor::ReplyAndDie(const TActorContext& ctx)
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

    ++ResponsesCount;
    if (ResponsesCount == VolumeActorIds.size()) {
        ReplyAndDie(ctx);
    }
}

void TServiceStatisticsCollectorActor::HandleGetServiceStatisticsUndelivery(
    TEvStatsService::TEvGetServiceStatisticsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ++ResponsesCount;
    if (ResponsesCount == VolumeActorIds.size()) {
        ReplyAndDie(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TServiceStatisticsCollectorActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleTimeout);

        HFunc(
            TEvStatsService::TEvGetServiceStatisticsResponse,
            HandleGetServiceStatisticsResponse);

        HFunc(
            TEvStatsService::TEvGetServiceStatisticsRequest,
            HandleGetServiceStatisticsUndelivery);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::STATS_SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
