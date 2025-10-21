#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandlePing(
    const TEvService::TEvPingRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvPingResponse>();

    response->Record.SetServiceState(ServiceState);

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
