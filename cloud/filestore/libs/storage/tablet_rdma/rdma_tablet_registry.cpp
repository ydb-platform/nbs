#include "rdma_tablet_registry.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

// TODO after #7890 we don't need find for every request, but it shouldn't be a
// big problem anyway
TActorId TRdmaTabletRegistry::FindTablet(const TActorId& pipeServerId) const
{
    auto guard = Guard(Lock);
    const auto* tabletActorId = Pipes.FindPtr(pipeServerId);
    return tabletActorId ? *tabletActorId : TActorId();
}

void TRdmaTabletRegistry::RegisterPipe(
    const TActorId& pipeServerId,
    const TActorId& tabletActorId)
{
    auto guard = Guard(Lock);
    Pipes[pipeServerId] = tabletActorId;
    TabletPipes[tabletActorId].insert(pipeServerId);
}

void TRdmaTabletRegistry::UnregisterPipe(const TActorId& pipeServerId)
{
    auto guard = Guard(Lock);
    auto pipe = Pipes.find(pipeServerId);
    if (pipe == Pipes.end()) {
        return;
    }

    auto tablet = TabletPipes.find(pipe->second);
    if (tablet != TabletPipes.end()) {
        tablet->second.erase(pipeServerId);
        if (tablet->second.empty()) {
            TabletPipes.erase(tablet);
        }
    }

    Pipes.erase(pipe);
}

void TRdmaTabletRegistry::UnregisterTablet(const TActorId& tabletActorId)
{
    auto guard = Guard(Lock);
    auto tablet = TabletPipes.find(tabletActorId);
    if (tablet == TabletPipes.end()) {
        return;
    }

    for (const auto& pipeServerId: tablet->second) {
        Pipes.erase(pipeServerId);
    }
    TabletPipes.erase(tablet);
}

}   // namespace NCloud::NFileStore::NStorage
