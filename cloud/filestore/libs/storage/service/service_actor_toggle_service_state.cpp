#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleToggleServiceState(
    const TEvService::TEvToggleServiceStateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    ServiceState = msg->Record.GetDesiredServiceState();

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "Service state changed to %s",
        EServiceState_Name(ServiceState).Quote().c_str()
    );

    auto response =
        std::make_unique<TEvService::TEvToggleServiceStateResponse>();

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
