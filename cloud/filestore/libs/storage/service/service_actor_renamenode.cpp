#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleRenameNode(
    const TEvService::TEvRenameNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* session =
        GetAndValidateSession<TEvService::TRenameNodeMethod>(ctx, ev);
    if (!session) {
        return;
    }

    if (TryHandleControlNamespaceRenameNode(ctx, ev, session)) {
        return;
    }

    ForwardRequestToShard<TEvService::TRenameNodeMethod>(
        ctx,
        ev,
        false /* forceBehaveAsShard */,
        ev->Get()->Record.GetNodeId());
}

}   // namespace NCloud::NFileStore::NStorage
