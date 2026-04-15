#include "mirror_request_actor.h"

#include <cloud/storage/core/libs/actors/actor_pool.h>
#include <cloud/storage/core/libs/kikimr/actor_system_adapter.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

template <>
TPooledActorHolder<TMirrorRequestActor<TEvService::TWriteBlocksMethod>>
GetMirrorRequestActor()
{
    static auto actorPool = MakeIntrusive<
        TActorPool<TMirrorRequestActor<TEvService::TWriteBlocksMethod>>>(
        MakeIntrusive<TActorSystemAdapter>(TActivationContext::ActorSystem()),
        1000u);
    return actorPool->GetPooledActor();
}

template <>
TPooledActorHolder<TMirrorRequestActor<TEvService::TWriteBlocksLocalMethod>>
GetMirrorRequestActor()
{
    static auto actorPool = MakeIntrusive<
        TActorPool<TMirrorRequestActor<TEvService::TWriteBlocksLocalMethod>>>(
        MakeIntrusive<TActorSystemAdapter>(TActivationContext::ActorSystem()),
        1000u);
    return actorPool->GetPooledActor();
}

template <>
TPooledActorHolder<TMirrorRequestActor<TEvService::TZeroBlocksMethod>>
GetMirrorRequestActor()
{
    static auto actorPool = MakeIntrusive<
        TActorPool<TMirrorRequestActor<TEvService::TZeroBlocksMethod>>>(
        MakeIntrusive<TActorSystemAdapter>(TActivationContext::ActorSystem()),
        1000u);
    return actorPool->GetPooledActor();
}

void ProcessMirrorActorError(NProto::TError& error)
{
    if (HasError(error) && error.GetCode() != E_REJECTED) {
        // We believe that all errors of the mirrored disk can be fixed by
        // repeating the request.
        error = MakeError(E_REJECTED, FormatError(error));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
