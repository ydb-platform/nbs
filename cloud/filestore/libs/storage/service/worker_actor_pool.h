#pragma once

#include <contrib/ydb/library/actors/core/actor.h>

#include <util/generic/vector.h>

#include <functional>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Pool bookkeeping is confined to the storage service actor's mailbox.
// The actor system owns the actors; the pool only tracks their IDs.
class TWorkerActorPool
{
public:
    // Creates an actor. The pool registers it with the actor system.
    // \param parentActorid - id of pool owner. Should be used by the actor
    //                        to signal that it has completed and should be
    //                        returned to the pool.
    using TActorFactory =
        std::function<NActors::IActor*(NActors::TActorId parentActorId)>;

private:
    const TActorFactory ActorFactory;
    NActors::TActorSystem* ActorSystem = nullptr;

    TVector<NActors::TActorId> AllActors;
    TVector<NActors::TActorId> FreeActors;

public:
    explicit TWorkerActorPool(TActorFactory actorFactory);
    ~TWorkerActorPool();

    TWorkerActorPool(const TWorkerActorPool&) = delete;
    TWorkerActorPool& operator=(const TWorkerActorPool&) = delete;

    size_t GetSize() const
    {
        return AllActors.size();
    }

    void Initialize(const NActors::TActorContext& ctx, ui32 initialSize);

    NActors::TActorId GetOrCreateActor(const NActors::TActorContext& ctx);
    void ReleaseActor(const NActors::TActorId& actorId);

private:
    void CreateActor(const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NFileStore::NStorage
