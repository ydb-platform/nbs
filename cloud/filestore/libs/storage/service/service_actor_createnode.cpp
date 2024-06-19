#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleCreateNode(
    const TEvService::TEvCreateNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO: impl follower forwarding

    ForwardRequest<TEvService::TCreateNodeMethod>(ctx, ev);
}

}   // namespace NCloud::NFileStore::NStorage
