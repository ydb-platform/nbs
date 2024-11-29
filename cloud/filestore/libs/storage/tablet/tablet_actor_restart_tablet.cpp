#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleRestartTablet(
    const TEvIndexTablet::TEvRestartTabletRequest::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_TRACE(ctx, TFileStoreComponents::TABLET,
        "%s Received RestartTablet request, restarting",
        LogTag.c_str());

    auto response = std::make_unique<TEvIndexTablet::TEvRestartTabletResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));

    Suicide(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
