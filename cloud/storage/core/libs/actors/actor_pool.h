#pragma once

#include <cloud/storage/core/libs/actors/pooled_actor.h>
#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <util/system/spin_wait.h>
#include <util/thread/lfqueue.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// RAII holder for a pooled actor. Prevents OnBeforeDestroy() from sending
// PoisonPill while the caller is still using the actor (e.g. inside
// SendRequest). The in-flight counter is decremented when the holder is
// destroyed or moved-from.
template <typename TActor>
class TPooledActorHolder

{
    template <typename>
    friend class TActorPool;

private:
    TActor* Actor = nullptr;
    std::atomic<int>* InFlightSends = nullptr;

    TPooledActorHolder(TActor* actor, std::atomic<int>* counter)
        : Actor(actor)
        , InFlightSends(counter)
    {}

public:
    TPooledActorHolder() = default;

    ~TPooledActorHolder()
    {
        Release();
    }

    TPooledActorHolder(TPooledActorHolder&& other) noexcept
        : Actor(std::exchange(other.Actor, nullptr))
        , InFlightSends(std::exchange(other.InFlightSends, nullptr))
    {}

    TPooledActorHolder& operator=(TPooledActorHolder&& other) noexcept
    {
        if (this != &other) {
            Release();
            Actor = std::exchange(other.Actor, nullptr);
            InFlightSends = std::exchange(other.InFlightSends, nullptr);
        }
        return *this;
    }

    TPooledActorHolder(const TPooledActorHolder&) = delete;
    TPooledActorHolder& operator=(const TPooledActorHolder&) = delete;

    explicit operator bool() const
    {
        return Actor != nullptr;
    }

    TActor* operator->() const
    {
        return Actor;
    }

    TActor* Get() const
    {
        return Actor;
    }

private:
    void Release()
    {
        if (InFlightSends) {
            InFlightSends->fetch_sub(1, std::memory_order_seq_cst);
            InFlightSends = nullptr;
        }
        Actor = nullptr;
    }
};

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

    TLockFreeQueue<TActorEntry> FreePooledActors;
    TLockFreeQueue<NActors::TActorId> PooledActors;
    std::atomic<ui32> PooledActorsCount = 0;
    std::atomic_flag Stopped = ATOMIC_FLAG_INIT;
    std::atomic<int> InFlightSends{0};

public:
    TActorPool(IActorSystemPtr actorSystem, ui32 maxActors)
        : ActorSystem(std::move(actorSystem))
        , MaxActors(maxActors)
    {}

    ~TActorPool() override = default;

    void OnBeforeDestroy()
    {
        Stopped.test_and_set(std::memory_order_seq_cst);

        TSpinWait sw;
        while (InFlightSends.load(std::memory_order_seq_cst) > 0) {
            sw.Sleep();
        }

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

    TPooledActorHolder<TActor> GetPooledActor()
    {
        InFlightSends.fetch_add(1, std::memory_order_seq_cst);

        if (Stopped.test()) {
            InFlightSends.fetch_sub(1, std::memory_order_seq_cst);
            return {};
        }

        TActorEntry entry;
        if (FreePooledActors.Dequeue(&entry)) {
            return {static_cast<TActor*>(entry.Actor), &InFlightSends};
        }

        TActor* actorRawPtr;
        NActors::TActorId actorId;
        {
            auto actorPtr = std::make_unique<TActor>();
            actorRawPtr = actorPtr.get();
            actorId = ActorSystem->Register(std::move(actorPtr));
            actorRawPtr->SetSelfId(actorId);
            actorRawPtr->SetActorSystem(ActorSystem);
        }

        ui32 count = PooledActorsCount.load(std::memory_order_relaxed);
        while (count < MaxActors) {
            if (PooledActorsCount.compare_exchange_weak(
                    count,
                    count + 1,
                    std::memory_order_relaxed))
            {
                actorRawPtr->SetReturnQueue(this);
                PooledActors.Enqueue(actorId);
                break;
            }
        }

        return {actorRawPtr, &InFlightSends};
    }

    void OnWorkFinished(IPooledActor<TActor>* actor) override
    {
        PutActorToQueue(actor, actor->GetSelfId());
    }

private:
    void PutActorToQueue(IPooledActor<TActor>* actor, NActors::TActorId actorId)
    {
        if (!Stopped.test()) {
            FreePooledActors.Enqueue({.ActorId = actorId, .Actor = actor});
        }
    }
};

}   // namespace NCloud
