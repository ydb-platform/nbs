#pragma once

#include "public.h"

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// Helps to handle the TEvPoisonPill for actor who owns other actors. The helper
// sends TEvPoisonPill to all owned actors and waits for a response
// TEvPoisonTaken from everyone. After that, it responds with the TEvPoisonTaken
// message.
class TPoisonPillHelper
{
private:
    struct TPoisoner
    {
        NActors::TActorId Sender;
        ui64 Cookie = 0;
    };

    NActors::IActor* Owner;
    TSet<NActors::TActorId> OwnedActors;
    std::optional<TPoisoner> Poisoner;

public:
    explicit TPoisonPillHelper(NActors::IActor* owner);
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
};

}   // namespace NCloud
