#include "pooled_actor.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TPooledActor::TPooledActor(TReceiveFunc stateFunc)
    : TBase(stateFunc)
{}

TPooledActor::~TPooledActor() = default;

void TPooledActor::SetActorSystem(IActorSystemPtr actorSystem)
{
    ActorSystem = std::move(actorSystem);
}

void TPooledActor::SetSelfId(NActors::TActorId selfId)
{
    SelfId = selfId;
}

void TPooledActor::SetWorkFinishedCallback(
    std::function<void()> workFinishedCallback)
{
    WorkFinishedCallback = std::move(workFinishedCallback);
}

void TPooledActor::WorkFinished(const NActors::TActorContext& ctx)
{
    if (WorkFinishedCallback) {
        WorkFinishedCallback();
    } else {
        Die(ctx);
    }
}

IActorSystem* TPooledActor::GetActorSystem()
{
    return ActorSystem.Get();
}

}   // namespace NCloud
