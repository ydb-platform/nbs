#pragma once

#include <cloud/storage/core/libs/actors/pooled_actor.h>
#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <util/generic/deque.h>
#include <util/system/guard.h>
#include <util/system/spin_wait.h>
#include <util/system/spinlock.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// RAII holder for a pooled actor. Prevents OnBeforeDestroy() from sending
// PoisonPill while the caller is still using the actor (e.g. inside
// SendRequest). The in-flight counter is decremented when the holder is
// destroyed or moved-from.
template <typename TActor>
class TPooledActorHolder

{
private:
    template <typename>
    friend class TActorPool;

    TActor* Actor = nullptr;
    std::atomic<int>* InFlightSends = nullptr;

    explicit TPooledActorHolder(std::atomic<int>* counter)
        : InFlightSends(counter)
    {
        InFlightSends->fetch_add(1);
    }

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
            InFlightSends->fetch_sub(1);
            InFlightSends = nullptr;
        }
        Actor = nullptr;
    }

    void SetActor(TActor* actor)
    {
        Actor = actor;
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

    TDeque<TActorEntry> FreePooledActors;
    TAdaptiveLock FreePooledActorsLock;
    TDeque<NActors::TActorId> PooledActors;
    TAdaptiveLock PooledActorsLock;

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
        Stopped.test_and_set();

        TSpinWait sw;
        while (InFlightSends.load() > 0) {
            sw.Sleep();
        }

        TDeque<NActors::TActorId> actors;
        with_lock (PooledActorsLock) {
            actors.swap(PooledActors);
        }

        for (const auto& actorId: actors) {
            ActorSystem->Send(
                actorId,
                std::make_unique<NActors::TEvents::TEvPoisonPill>());
        }

        with_lock (FreePooledActorsLock) {
            FreePooledActors.clear();
        }
    }

    TPooledActorHolder<TActor> GetPooledActor()
    {
        TPooledActorHolder<TActor> holder(&InFlightSends);
        if (Stopped.test()) {
            return holder;
        }

        TActorEntry entry;
        with_lock (FreePooledActorsLock) {
            if (!FreePooledActors.empty()) {
                entry = std::move(FreePooledActors.front());
                FreePooledActors.pop_front();
                holder.SetActor(static_cast<TActor*>(entry.Actor));
                return holder;
            }
        }

        TActor* actorRawPtr;
        NActors::TActorId actorId;
        {
            auto actorPtr = std::make_unique<TActor>();
            actorRawPtr = actorPtr.get();
            holder.SetActor(actorRawPtr);
            actorId = ActorSystem->Register(std::move(actorPtr));
            actorRawPtr->SetSelfId(actorId);
            actorRawPtr->SetActorSystem(ActorSystem);
        }

        with_lock (PooledActorsLock) {
            if (PooledActors.size() < MaxActors) {
                actorRawPtr->SetReturnQueue(this);
                PooledActors.push_back(actorId);
            }
        }

        return holder;
    }

    void OnWorkFinished(IPooledActor<TActor>* actor) override
    {
        PutActorToQueue(actor, actor->GetSelfId());
    }

private:
    void PutActorToQueue(IPooledActor<TActor>* actor, NActors::TActorId actorId)
    {
        if (!Stopped.test()) {
            auto g = Guard(FreePooledActorsLock);
            FreePooledActors.push_back({.ActorId = actorId, .Actor = actor});
        }
    }
};

}   // namespace NCloud
