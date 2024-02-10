#include "poison_pill_helper.h"

#include <cloud/storage/core/libs/actors/helpers.h>

using namespace NActors;

namespace NCloud {
namespace {

////////////////////////////////////////////////////////////////////////////////

// Helps to call protected methods of IActor.
class IActorHelper: public IActor
{
public:
    static void CallDie(IActor* actor, const TActorContext& ctx)
    {
        Y_DEBUG_ABORT_UNLESS(actor);
        static_cast<IActorHelper*>(actor)->Die(ctx);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TPoisonPillHelper::TPoisonPillHelper(NActors::IActor* owner)
    : Owner(owner)
{}

TPoisonPillHelper::~TPoisonPillHelper() = default;

void TPoisonPillHelper::TakeOwnership(
    const TActorContext& ctx,
    NActors::TActorId actor)
{
    if (actor == TActorId()) {
        return;
    }
    OwnedActors.insert(actor);
    if (Poisoner) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, actor);
    }
}

void TPoisonPillHelper::ReleaseOwnership(
    const NActors::TActorContext& ctx,
    NActors::TActorId actor)
{
    OwnedActors.erase(actor);
    ReplyAndDie(ctx);
}

void TPoisonPillHelper::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(!Poisoner);

    Poisoner = TPoisoner{ev->Sender, ev->Cookie};
    KillActors(ctx);
    ReplyAndDie(ctx);
}

void TPoisonPillHelper::HandlePoisonTaken(
    const NActors::TEvents::TEvPoisonTaken::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    OwnedActors.erase(ev->Sender);
    ReplyAndDie(ctx);
}

void TPoisonPillHelper::KillActors(const TActorContext& ctx)
{
    for (auto actor: OwnedActors) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, actor);
    }
}

void TPoisonPillHelper::ReplyAndDie(const TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(Poisoner);

    if (!OwnedActors.empty()) {
        return;
    }

    ctx.Send(
        Poisoner->Sender,
        std::make_unique<TEvents::TEvPoisonTaken>(),
        0,   // flags
        Poisoner->Cookie);

    IActorHelper::CallDie(Owner, ctx);
}

}   // namespace NCloud
