#include "rdma_tablet_registry.h"

#include "rdma_private.h"

#include <util/generic/algorithm.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TRdmaTabletRegistry::AttachForwarder(const TActorId& forwarderId)
{
    auto guard = Guard(Lock);
    ForwarderId = forwarderId;
}

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
}

void TRdmaTabletRegistry::UnregisterPipe(const TActorId& pipeServerId)
{
    auto guard = Guard(Lock);
    Pipes.erase(pipeServerId);
}

void TRdmaTabletRegistry::UnregisterTablet(
    const TActorContext& ctx,
    const TActorId& tabletActorId)
{
    TActorId forwarderId;
    with_lock (Lock) {
        EraseNodesIf(
            Pipes,
            [&](const auto& pipe) { return pipe.second == tabletActorId; });
        forwarderId = ForwarderId;
    }

    if (forwarderId) {
        ctx.Send(
            forwarderId,
            new TEvRdmaTabletPrivate::TEvTabletGone(tabletActorId));
    }
}

}   // namespace NCloud::NFileStore::NStorage
