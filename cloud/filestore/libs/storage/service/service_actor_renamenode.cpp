#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleRenameNode(
    const TEvService::TEvRenameNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (TryHandleControlNamespaceRenameNode(ctx, ev)) {
        return;
    }

    ForwardRequestToShard<TEvService::TRenameNodeMethod>(
        ctx,
        ev,
        false /* forceBehaveAsShard */,
        ev->Get()->Record.GetNodeId());
}

}   // namespace NCloud::NFileStore::NStorage
