#include "actor_pool.h"

#include <contrib/ydb/library/actors/core/log.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

namespace NCloud {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TActorPool::TActorPool(ISchedulerPtr scheduler,
    ITimerPtr timer,
    IActorSystemPtr actorSystem,
    ui32 maxActors)
    : Scheduler(std::move(scheduler))
    , Timer(std::move(timer))
    , ActorSystem(std::move(actorSystem))
    , MaxActors(maxActors)
{
    // Scheduler->Schedule(Timer->Now() + TDuration::MilliSeconds(500), [] {

    // });
}

TActorPool::~TActorPool() = default;

void TActorPool::OnBeforeDestroy() {
    Stopped.test_and_set();

    TVector<TActorId> actors;
    actors.reserve(MaxActors * 2);
    AllActors.DequeueAll(&actors);
    for (const auto& actorId: actors) {
        ActorSystem->Send(actorId, std::make_unique<TEvents::TEvPoisonPill>());
    }
    FreeActors.DequeueAll(&actors);
}

std::function<void()> TActorPool::GetActorPutBackCallback(
    NActors::TActorId actorId)
{
    TIntrusivePtr<TActorPool> selfPtr{this};
    return [selfPtr = std::move(selfPtr), actorId]()
    {
        selfPtr->PutActorToQueue(actorId);
    };
}

void TActorPool::PutActorToQueue(NActors::TActorId actorId)
{
    FreeActors.Enqueue(actorId);
}

}   // namespace NCloud
