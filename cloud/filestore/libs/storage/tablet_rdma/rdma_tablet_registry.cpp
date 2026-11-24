#include "rdma_tablet_registry.h"

#include "rdma_private.h"

#include <contrib/ydb/library/actors/core/actor.h>

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
TActorId TRdmaTabletRegistry::FindTablet(
    const TActorId& pipeServerId) const noexcept
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
    TabletPipes[tabletActorId].insert(pipeServerId);

    auto [pipe, inserted] = Pipes.emplace(pipeServerId, tabletActorId);
    if (!inserted && pipe->second != tabletActorId) {
        EraseTabletPipe(pipe->second, pipeServerId);
        pipe->second = tabletActorId;
    }
}

void TRdmaTabletRegistry::UnregisterPipe(const TActorId& pipeServerId) noexcept
{
    auto guard = Guard(Lock);
    auto pipe = Pipes.find(pipeServerId);
    if (pipe == Pipes.end()) {
        return;
    }

    EraseTabletPipe(pipe->second, pipeServerId);
    Pipes.erase(pipe);
}

void TRdmaTabletRegistry::UnregisterTablet(
    const TActorId& tabletActorId) noexcept
{
    TActorId forwarderId;
    {
        auto guard = Guard(Lock);
        auto tablet = TabletPipes.find(tabletActorId);
        if (tablet != TabletPipes.end()) {
            for (const auto& pipeServerId: tablet->second) {
                Pipes.erase(pipeServerId);
            }
            TabletPipes.erase(tablet);
        }
        forwarderId = ForwarderId;
    }

    if (forwarderId) {
        TActivationContext::Send(
            forwarderId,
            std::make_unique<TEvRdmaTabletPrivate::TEvTabletGone>(
                tabletActorId));
    }
}

void TRdmaTabletRegistry::EraseTabletPipe(
    const TActorId& tabletActorId,
    const TActorId& pipeServerId) noexcept
{
    auto tablet = TabletPipes.find(tabletActorId);
    if (tablet == TabletPipes.end()) {
        return;
    }

    tablet->second.erase(pipeServerId);
    if (tablet->second.empty()) {
        TabletPipes.erase(tablet);
    }
}

}   // namespace NCloud::NFileStore::NStorage
