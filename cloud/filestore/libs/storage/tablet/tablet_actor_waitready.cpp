#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleWaitReady(
    const TEvIndexTablet::TEvWaitReadyRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (CurrentState != STATE_WORK) {
        LOG_TRACE(
            ctx,
            TFileStoreComponents::TABLET,
            "%s WaitReady request delayed until partition is ready",
            LogTag.c_str());

        WaitReadyRequests.emplace_back(ev.Release());
        return;
    }

    LOG_TRACE(
        ctx,
        TFileStoreComponents::TABLET,
        "%s Received WaitReady request",
        LogTag.c_str());

    auto response = std::make_unique<TEvIndexTablet::TEvWaitReadyResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
