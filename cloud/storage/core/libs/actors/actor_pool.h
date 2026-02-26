#pragma once

#include <cloud/storage/core/libs/actors/pooled_actor.h>
#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <util/thread/lfqueue.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// Thread-safe bounded pool of reusable actors. Actors are created on demand up
// to MaxActors and returned to the pool automatically when their work is done
// (via IPooledActor::WorkFinished). Actors beyond the limit are one-shot: they
// receive no return queue and Die() after completing their work.
// On shutdown, all tracked actors are sent a PoisonPill.
template <typename TActor>
class TActorPool final: public IActorReturnQueue<TActor>
{
private:
    const IActorSystemPtr ActorSystem;
    const ui32 MaxActors;

    struct TActorEntry
    {
        NActors::TActorId ActorId;
        IPooledActor<TActor>* Actor = nullptr;
    };

    struct TCounter: TAtomicCounter
    {
        void IncCount(const NActors::TActorId& /*unused*/)
        {
            Inc();
        }

        void DecCount(const NActors::TActorId& /*unused*/)
        {
            Dec();
        }
    };

    TLockFreeQueue<TActorEntry> FreePooledActors;
    TLockFreeQueue<NActors::TActorId, TCounter> PooledActors;
    std::atomic_flag Stopped = ATOMIC_FLAG_INIT;

public:
    TActorPool(IActorSystemPtr actorSystem, ui32 maxActors)
        : ActorSystem(std::move(actorSystem))
        , MaxActors(maxActors)
    {}

    ~TActorPool() override = default;

    void OnBeforeDestroy()
    {
        Stopped.test_and_set();

        TVector<NActors::TActorId> actors;
        actors.reserve(MaxActors);
        PooledActors.DequeueAll(&actors);
        for (const auto& actorId: actors) {
            ActorSystem->Send(
                actorId,
                std::make_unique<NActors::TEvents::TEvPoisonPill>());
        }

        TVector<TActorEntry> freeActors;
        freeActors.reserve(actors.size());
        FreePooledActors.DequeueAll(&freeActors);
    }

    template <typename TRequestActor>
        requires std::derived_from<TRequestActor, IPooledActor<TRequestActor>>
    TRequestActor* GetPooledActor()
    {
        if (Stopped.test()) {
            return nullptr;
        }

        TActorEntry entry;
        if (FreePooledActors.Dequeue(&entry)) {
            return static_cast<TRequestActor*>(entry.Actor);
        }

        TRequestActor* actorRawPtr;
        NActors::TActorId actorId;
        {
            auto actorPtr = std::make_unique<TRequestActor>();
            actorRawPtr = actorPtr.get();
            actorId = ActorSystem->Register(std::move(actorPtr));
            actorRawPtr->SetSelfId(actorId);
            actorRawPtr->SetActorSystem(ActorSystem);
        }

        if (PooledActors.GetCounter().Val() < MaxActors) {
            actorRawPtr->SetReturnQueue(this);
            PooledActors.Enqueue(actorId);
        }

        return actorRawPtr;
    }

    void OnWorkFinished(IPooledActor<TActor>* actor) override
    {
        PutActorToQueue(actor, actor->GetSelfId());
    }

private:
    void PutActorToQueue(
        IPooledActor<TActor>* actor,
        NActors::TActorId actorId)
    {
        if (!Stopped.test()) {
            FreePooledActors.Enqueue({.ActorId = actorId, .Actor = actor});
        }
    }
};

}   // namespace NCloud
