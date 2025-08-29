#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleListDisksStates(
    const TEvService::TEvListDisksStatesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received ListDisksStates request",
        TabletID());

    auto states = State->ListDisksStates();
    for (auto& s: states) {
        s = OverrideDiskState(std::move(s));
    }

    auto response = std::make_unique<TEvService::TEvListDisksStatesResponse>();
    response->Record.MutableDisksStates()->Assign(
        std::make_move_iterator(states.begin()),
        std::make_move_iterator(states.end()));

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
