#include "replica_actors.h"

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TReplicaActors::TReplicaActors() = default;
TReplicaActors::~TReplicaActors() = default;

ui32 TReplicaActors::Size() const
{
    Y_ABORT_UNLESS(PartActorIds.size() == LaggingProxyActorIds.size());
    return PartActorIds.size();
}

void TReplicaActors::AddReplicaActor(const TActorId& actorId)
{
    PartActorIds.push_back(actorId);
    LaggingProxyActorIds.push_back(actorId);
}

const TActorId& TReplicaActors::GetReplicaActor(ui32 index) const
{
    Y_ABORT_UNLESS(index < LaggingProxyActorIds.size());
    return LaggingProxyActorIds[index];
}

const TVector<TActorId>& TReplicaActors::GetReplicaActors() const
{
    return LaggingProxyActorIds;
}

const TVector<TActorId>&
TReplicaActors::GetReplicaActorsBypassingProxies() const
{
    return PartActorIds;
}

bool TReplicaActors::IsReplicaActor(TActorId actorId) const
{
    const ui32 index = GetReplicaIndex(actorId);
    return index < Size();
}

ui32 TReplicaActors::GetReplicaIndex(TActorId actorId) const
{
    size_t index = FindIndex(PartActorIds, actorId);
    if (index != NPOS) {
        return index;
    }

    return FindIndex(LaggingProxyActorIds, actorId);
}

void TReplicaActors::SetLaggingReplicaProxy(
    ui32 replicaIndex,
    const TActorId& actorId)
{
    Y_ABORT_UNLESS(replicaIndex < Size());
    LaggingProxyActorIds[replicaIndex] = actorId;
}

void TReplicaActors::ResetLaggingReplicaProxy(ui32 replicaIndex)
{
    Y_ABORT_UNLESS(replicaIndex < Size());
    LaggingProxyActorIds[replicaIndex] = PartActorIds[replicaIndex];
}

bool TReplicaActors::IsLaggingProxySet(ui32 replicaIndex) const
{
    if (replicaIndex >= Size()) {
        return false;
    }

    return LaggingProxyActorIds[replicaIndex] != PartActorIds[replicaIndex];
}

ui32 TReplicaActors::LaggingReplicaCount() const
{
    ui32 count = 0;
    for (ui32 i = 0; i < Size(); ++i) {
        if (IsLaggingProxySet(i)) {
            ++count;
        }
    }
    return count;
}

}   // namespace NCloud::NBlockStore::NStorage
