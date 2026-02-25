#pragma once

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

template <typename TActor>
class TActorPool;

// Base class for actors that are pooled by TActorPool. After the derived class
// is done with its work, it should call WorkFinished() to return the actor to
// the pool.
template <typename TDerived>
class IPooledActor: public NActors::TActor<TDerived>
{
private:
    using TBase = NActors::TActor<TDerived>;
    friend class TActorPool<TDerived>;

    IActorSystemPtr ActorSystem;
    NActors::TActorId SelfId;
    std::function<void()> WorkFinishedCallback;

protected:
    explicit IPooledActor(
        void (TDerived::*stateFunc)(TAutoPtr<NActors::IEventHandle>&))
        : TBase(static_cast<typename TBase::TReceiveFunc>(stateFunc))
    {}

    virtual void Reset() = 0;

public:
    ~IPooledActor() override = default;

    void WorkFinished(const NActors::TActorContext& ctx)
    {
        if (!WorkFinishedCallback) {
            TBase::Die(ctx);
            return;
        }

        Reset();
        WorkFinishedCallback();
    }

    IActorSystem* GetActorSystem()
    {
        return ActorSystem.Get();
    }

    NActors::TActorId GetSelfId() const
    {
        return SelfId;
    }

private:
    void SetActorSystem(IActorSystemPtr actorSystem)
    {
        ActorSystem = std::move(actorSystem);
    }

    void SetSelfId(NActors::TActorId selfId)
    {
        SelfId = selfId;
    }

    // Optional callback that will be called on WorkFinished(). If not set, the
    // actor is for one use only.
    void SetWorkFinishedCallback(std::function<void()> workFinishedCallback)
    {
        WorkFinishedCallback = std::move(workFinishedCallback);
    }
};

}   // namespace NCloud
