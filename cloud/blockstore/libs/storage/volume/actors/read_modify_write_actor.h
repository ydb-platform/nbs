#pragma once

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

class TReadModifyWriteActor final
    : public NActors::TActorBootstrapped<TReadModifyWriteActor>
{
private:
    const NActors::TActorId Owner;
    const TString DiskId;
    const TString ClientId;

public:
    TReadModifyWriteActor(
        const NActors::TActorId& owner,
        TString diskId,
        TString clientId);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void ReplyAndDie(const NActors::TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleTimeout(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
