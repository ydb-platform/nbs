#pragma once

#include <cloud/storage/core/libs/actors/pooled_actor.h>
#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <util/thread/lfqueue.h>

namespace NCloud {

class TActorPool final: TAtomicRefCount<TActorPool>
{
private:
    ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const IActorSystemPtr ActorSystem;

    const ui32 MaxActors;

    struct TRequestActorEntry
    {
        NActors::TActorId ActorId;
        TPooledActor* Actor = nullptr;
    };
    TLockFreeQueue<TRequestActorEntry> FreeActors;
    TLockFreeQueue<NActors::TActorId> AllActors;
    std::atomic<ui32> PooledActorCount = 0;
    std::atomic_flag Stopped = ATOMIC_FLAG_INIT;

public:
    TActorPool(
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        IActorSystemPtr actorSystem,
        ui32 maxActors);

    ~TActorPool();

    void OnBeforeDestroy();

    void PutActorToQueue(NActors::TActorId actorId);

    template <typename TRequestActor>
        requires std::derived_from<TRequestActor, TPooledActor>
    TRequestActor* GetPooledActor()
    {
        if (Stopped.test()) {
            return NActors::TActorId();
        }

        TRequestActorEntry entry;
        if (FreeActors.Dequeue(&entry)) {
            return entry.Actor;
        }

        std::function<void()> putBackCallback;
        if (PooledActorCount < MaxActors) {
            PooledActorCount++;
            putBackCallback = GetActorPutBackCallback(actorId);
        }

        auto actorPtr = std::make_unique<TRequestActor>();
        if (putBackCallback) {
            actorPtr->SetPutBackCallback(std::move(putBackCallback));
            AllActors.Enqueue(actorId);
        }

        auto actorId = ActorSystem->Register(std::move(actorPtr));
        FreeActors.Enqueue({actorId, actorPtr.get()});
        return actorId;
    }

private:
    std::function<void()> GetActorPutBackCallback(NActors::TActorId actorId);
};

}   // namespace NCloud
