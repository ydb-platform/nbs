#pragma once

#include "service_state.h"

#include <cloud/filestore/libs/diagnostics/trace_serializer.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Pool bookkeeping is confined to the storage service actor's mailbox.
// The actor system owns the actors; the pool only tracks their IDs.
class TReadDataActorPool
{
private:
    const IProfileLogPtr ProfileLog;
    const ITraceSerializerPtr TraceSerializer;
    const TInFlightRequestStoragePtr InFlightRequests;

    NActors::TActorSystem* ActorSystem = nullptr;

    TVector<NActors::TActorId> AllActors;
    TVector<NActors::TActorId> FreeActors;

public:
    TReadDataActorPool(
        IProfileLogPtr profileLog,
        ITraceSerializerPtr traceSerializer,
        TInFlightRequestStoragePtr inFlightRequests);
    ~TReadDataActorPool();

    size_t GetSize() const
    {
        return AllActors.size();
    }

    TReadDataActorPool(const TReadDataActorPool&) = delete;
    TReadDataActorPool& operator=(const TReadDataActorPool&) = delete;

    void Initialize(const NActors::TActorContext& ctx, ui32 initialSize);

    NActors::TActorId GetOrCreateActor(const NActors::TActorContext& ctx);
    void ReleaseActor(const NActors::TActorId& actorId);

private:
    void CreateActor(const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NFileStore::NStorage
