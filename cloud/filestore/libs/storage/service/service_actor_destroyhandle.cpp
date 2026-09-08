#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleDestroyHandle(
    const TEvService::TEvDestroyHandleRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* session =
        GetAndValidateSession<TEvService::TDestroyHandleMethod>(ctx, ev);
    if (!session) {
        return;
    }

    if (TryHandleControlNamespaceDestroyHandle(ctx, ev, session)) {
        return;
    }

    ForwardRequestToShard<TEvService::TDestroyHandleMethod>(
        ctx,
        ev,
        false /* forceBehaveAsShard */,
        ev->Get()->Record.GetHandle(),
        session);
}

}   // namespace NCloud::NFileStore::NStorage
