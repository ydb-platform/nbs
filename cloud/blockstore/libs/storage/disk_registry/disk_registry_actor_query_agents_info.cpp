#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/common/safe_debug_print.h>

#include <algorithm>

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
        SafeDebugPrint(msg->Record).c_str());

    auto response = std::make_unique<TEvService::TEvQueryAgentsInfoResponse>(
        MakeError(S_OK));

    const auto& filterState = msg->Record.GetFilterState();
    for (auto& agentInfo: State->QueryAgentsInfo()) {
        if(filterState.size() > 0) {
            if(std::ranges::find(filterState, agentInfo.GetState()) == filterState.end()) {
                continue;
            }
        }

        *response->Record.AddAgents() = std::move(agentInfo);
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
