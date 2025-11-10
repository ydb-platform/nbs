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
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received QueryAgentsInfo request: %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.ShortDebugString().c_str());

    auto response = std::make_unique<TEvService::TEvQueryAgentsInfoResponse>(
        MakeError(S_OK));
    for (auto& agentInfo: State->QueryAgentsInfo()) {
        *response->Record.AddAgents() = std::move(agentInfo);
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
