#include "stats_service_actor.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TStatsServiceActor::HandleRegisterTrafficSource(
    const TEvStatsServicePrivate::TEvRegisterTrafficSourceRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    BackgroundBandwidth[msg->SourceId].LastRegistrationAt = ctx.Now();
    BackgroundBandwidth[msg->SourceId].DesiredBandwidthMiBs =
        msg->BandwidthMiBs;

    auto response = std::make_unique<
        TEvStatsServicePrivate::TEvRegisterTrafficSourceResponse>();
    response->LimitedBandwidthMiBs = CalcBandwidthLimit(msg->SourceId);

    NCloud::Reply(ctx, *ev, std::move(response));
}

ui32 TStatsServiceActor::CalcBandwidthLimit(const TString& sourceId) const
{
    ui32 totalDesiredBandwidth = 0;
    for (const auto& [_, info]: BackgroundBandwidth) {
        totalDesiredBandwidth += info.DesiredBandwidthMiBs;
    };

    if (totalDesiredBandwidth == 0) {
        return 0;
    }

    double multiplier =
        static_cast<double>(Config->GetBackgroundOperationsTotalBandwidth()) /
        totalDesiredBandwidth;
    multiplier = Min(multiplier, 1.0);
    if (const auto* info = BackgroundBandwidth.FindPtr(sourceId)) {
        const ui32 limitedBandwidth =
            static_cast<ui32>(info->DesiredBandwidthMiBs * multiplier);
        return Max<ui32>(limitedBandwidth, 1);
    }
    return 0;
}

void TStatsServiceActor::ScheduleCleanupBackgroundSources(
    const NActors::TActorContext& ctx) const
{
    ctx.Schedule(
        RegisterBackgroundTrafficDuration,
        new TEvStatsServicePrivate::TEvCleanupBackgroundSources());
}

void TStatsServiceActor::HandleCleanupBackgroundSources(
    const TEvStatsServicePrivate::TEvCleanupBackgroundSources::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    ScheduleCleanupBackgroundSources(ctx);

    auto deadline = ctx.Now() - RegisterBackgroundTrafficDuration * 3;
    EraseNodesIf(
        BackgroundBandwidth,
        [deadline](const auto& item)
        { return item.second.LastRegistrationAt < deadline; });
}

}   // namespace NCloud::NBlockStore::NStorage
