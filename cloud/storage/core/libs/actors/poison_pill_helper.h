#pragma once

#include "public.h"

#include "mortal_actor.h"

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/event.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <util/generic/set.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// Helps to handle the TEvPoisonPill for actor who owns other actors. The helper
// sends TEvPoisonPill to all owned actors and waits for a response
// TEvPoisonTaken from everyone. After that, it responds with the TEvPoisonTaken
// message.

// Don't send a poison pill to yourself with a cookie in which the most
// significant bit is set. This can lead to an actor leak.
class TPoisonPillHelper
{
private:
    struct TPoisoner
    {
        NActors::TActorId Sender;
        ui64 Cookie = 0;
    };

    IMortalActor* Owner;
    TSet<NActors::TActorId> OwnedActors;
    std::optional<TPoisoner> Poisoner;

    THashMap<ui64, NActors::TActorId> PoisonPillCookieToOwnedActorId;

    ui64 CookieCounter = 0;

public:
    explicit TPoisonPillHelper(IMortalActor* owner);
    virtual ~TPoisonPillHelper();

    void TakeOwnership(
        const NActors::TActorContext& ctx,
        NActors::TActorId actor);
    void ReleaseOwnership(
        const NActors::TActorContext& ctx,
        NActors::TActorId actor);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonTaken(
        const NActors::TEvents::TEvPoisonTaken::TPtr& ev,
        const NActors::TActorContext& ctx);

private:
    void KillActors(const NActors::TActorContext& ctx);
    void ReplyAndDie(const NActors::TActorContext& ctx);

    ui64 GetNextPoisonCookie();
};

}   // namespace NCloud
