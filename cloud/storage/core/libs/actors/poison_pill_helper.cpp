#include "poison_pill_helper.h"

#include <cloud/storage/core/libs/actors/helpers.h>

#include <util/string/join.h>

using namespace std::chrono_literals;

using namespace NActors;

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TPoisonPillHelper::TPoisonPillHelper(IMortalActor* owner)
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
    auto* actorId = PoisonPillCookieToOwnedActorId.FindPtr(ev->Cookie);

    if (actorId && ev->Sender == ctx.SelfID) {
        ReleaseOwnership(ctx, *actorId);
        PoisonPillCookieToOwnedActorId.erase(ev->Cookie);

        return;
    }

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
        auto cookie = GetNextPoisonCookie();

        PoisonPillCookieToOwnedActorId[cookie] = actor;

        SendWithUndeliveryTracking(
            ctx,
            actor,
            std::make_unique<TEvents::TEvPoisonPill>(),
            cookie);
    }
}

void TPoisonPillHelper::ReplyAndDie(const TActorContext& ctx)
{
    if (!Poisoner || !OwnedActors.empty()) {
        return;
    }

    ctx.Send(
        Poisoner->Sender,
        std::make_unique<TEvents::TEvPoisonTaken>(),
        0,   // flags
        Poisoner->Cookie);
    Owner->Poison(ctx);
}

ui64 TPoisonPillHelper::GetNextPoisonCookie()
{
    constexpr ui64 PoisonPillHelperCookiesFlag = ui64{1} << 63;
    auto cookieCounter = CookieCounter++;

    return cookieCounter | PoisonPillHelperCookiesFlag;
}

}   // namespace NCloud
