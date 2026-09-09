#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleUnlinkNode(
    const TEvService::TEvUnlinkNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* session =
        GetAndValidateSession<TEvService::TUnlinkNodeMethod>(ctx, ev);
    if (!session) {
        return;
    }

    if (TryHandleControlNamespaceUnlinkNode(ctx, ev, session)) {
        return;
    }

    ForwardRequestToShard<TEvService::TUnlinkNodeMethod>(
        ctx,
        ev,
        false /* forceBehaveAsShard */,
        ev->Get()->Record.GetNodeId(),
        session);
}

}   // namespace NCloud::NFileStore::NStorage
