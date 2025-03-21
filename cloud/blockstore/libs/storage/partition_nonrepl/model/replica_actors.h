#pragma once

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TReplicaActors
{
private:
    TVector<NActors::TActorId> LaggingProxyActorIds;
    TVector<NActors::TActorId> PartActorIds;

public:
    TReplicaActors();
    ~TReplicaActors();

public:
    [[nodiscard]] ui32 Size() const;

    void AddReplicaActor(const NActors::TActorId& actor);
    [[nodiscard]] const NActors::TActorId& GetReplicaActor(ui32 index) const;
    [[nodiscard]] const TVector<NActors::TActorId>& GetReplicaActors() const;
    [[nodiscard]] const TVector<NActors::TActorId>&
    GetReplicaActorsBypassingProxies() const;
    [[nodiscard]] bool IsReplicaActor(NActors::TActorId actorId) const;
    [[nodiscard]] ui32 GetReplicaIndex(NActors::TActorId actorId) const;

    void SetLaggingReplicaProxy(
        ui32 replicaIndex,
        const NActors::TActorId& actorId);
    void ResetLaggingReplicaProxy(ui32 replicaIndex);
    [[nodiscard]] bool IsLaggingProxySet(ui32 replicaIndex) const;
    [[nodiscard]] ui32 LaggingReplicaCount() const;
};

}   // namespace NCloud::NBlockStore::NStorage
