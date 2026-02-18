#pragma once

#include <cloud/storage/core/libs/actors/public.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud {

class TPooledActor: public NActors::TActor<TPooledActor>
{
    using TBase = NActors::TActor<TPooledActor>;

private:
    IActorSystemPtr ActorSystem;
    NActors::TActorId SelfId;
    std::function<void()> WorkFinishedCallback;

protected:
    explicit TPooledActor(TReceiveFunc stateFunc);

public:
    ~TPooledActor() override;

    void SetActorSystem(IActorSystemPtr actorSystem);
    void SetSelfId(NActors::TActorId selfId);
    void SetWorkFinishedCallback(std::function<void()> workFinishedCallback);
    void WorkFinished(const NActors::TActorContext& ctx);

    IActorSystem* GetActorSystem();
    NActors::TActorId GetSelfId() const;
};

}   // namespace NCloud
