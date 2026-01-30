#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/common/safe_debug_print.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleListDiskStates(
    const TEvService::TEvListDiskStatesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received ListDiskStates request: %s",
        LogTitle.GetWithTime().c_str(),
        SafeDebugPrint(msg->Record).c_str());

    auto states = State->ListDiskStates();
    for (auto& s: states) {
        s = OverrideDiskState(std::move(s));
    }

    auto response = std::make_unique<TEvService::TEvListDiskStatesResponse>();
    response->Record.MutableDiskStates()->Assign(
        std::make_move_iterator(states.begin()),
        std::make_move_iterator(states.end()));

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
