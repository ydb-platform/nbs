#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleListDiskStates(
    const TEvService::TEvListDiskStatesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received ListDiskStates request",
        TabletID());

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
