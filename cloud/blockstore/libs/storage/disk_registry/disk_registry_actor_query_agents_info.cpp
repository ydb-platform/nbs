#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleQueryAgentsInfo(
    const TEvService::TEvQueryAgentsInfoRequest::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received QueryAgentsInfo request",
        TabletID());

    auto response = std::make_unique<TEvService::TEvQueryAgentsInfoResponse>(
        MakeError(S_OK));
    for (auto& agentInfo: State->QueryAgentsInfo()) {
        *response->Record.AddAgents() = std::move(agentInfo);
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
