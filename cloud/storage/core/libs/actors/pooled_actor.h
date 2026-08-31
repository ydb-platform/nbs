#pragma once

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <util/generic/ptr.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
class IPooledActor;

// Interface for returning a pooled actor after its work is done. Implemented
// by TActorPool to put actors back into the reuse queue.
template <typename TActor>
class IActorReturnQueue: public TThrRefBase
{
public:
    ~IActorReturnQueue() override = default;

    virtual void OnWorkFinished(IPooledActor<TActor>* actor) = 0;
};

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
    TIntrusivePtr<IActorReturnQueue<TDerived>> ReturnQueue;

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
        if (!ReturnQueue) {
            TBase::Die(ctx);
            return;
        }

        Reset();
        ReturnQueue->OnWorkFinished(this);
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

    void SetReturnQueue(TIntrusivePtr<IActorReturnQueue<TDerived>> returnQueue)
    {
        ReturnQueue = std::move(returnQueue);
    }
};

}   // namespace NCloud
