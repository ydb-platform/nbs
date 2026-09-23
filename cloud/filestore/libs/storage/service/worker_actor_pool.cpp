#include "worker_actor_pool.h"

#include <contrib/ydb/library/actors/core/actorsystem.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <utility>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TWorkerActorPool::TWorkerActorPool(TActorFactory actorFactory)
    : ActorFactory(std::move(actorFactory))
{}

TWorkerActorPool::~TWorkerActorPool()
{
    if (!ActorSystem) {
        return;
    }

    for (const auto& actorId: AllActors) {
        ActorSystem->Send(actorId, new TEvents::TEvPoisonPill());
    }
}

void TWorkerActorPool::Initialize(const TActorContext& ctx, ui32 initialSize)
{
    Y_ABORT_UNLESS(!ActorSystem);
    ActorSystem = ctx.ActorSystem();
    AllActors.reserve(initialSize);
    FreeActors.reserve(initialSize);
    for (ui32 i = 0; i < initialSize; ++i) {
        CreateActor(ctx);
    }
}

void TWorkerActorPool::CreateActor(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(ActorSystem == ctx.ActorSystem());
    const auto actorId = ctx.Register(ActorFactory(ctx.SelfID));

    AllActors.push_back(actorId);
    FreeActors.push_back(actorId);
}

TActorId TWorkerActorPool::GetOrCreateActor(const TActorContext& ctx)
{
    if (FreeActors.empty()) {
        CreateActor(ctx);
    }

    const auto actorId = FreeActors.back();
    FreeActors.pop_back();
    return actorId;
}

void TWorkerActorPool::ReleaseActor(const TActorId& actorId)
{
    // Reuse the most recently released actor for better cache locality.
    FreeActors.push_back(actorId);
}

}   // namespace NCloud::NFileStore::NStorage
