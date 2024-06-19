#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleListNodes(
    const TEvService::TEvListNodesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO: impl follower forwarding

    ForwardRequest<TEvService::TListNodesMethod>(ctx, ev);
}

}   // namespace NCloud::NFileStore::NStorage
