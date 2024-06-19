#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleGetNodeAttr(
    const TEvService::TEvGetNodeAttrRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO: impl follower forwarding

    ForwardRequest<TEvService::TGetNodeAttrMethod>(ctx, ev);
}

}   // namespace NCloud::NFileStore::NStorage
