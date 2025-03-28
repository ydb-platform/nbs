#pragma once

#include <cloud/blockstore/libs/storage/api/partition.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class ICopyRangeOwner
{
public:
    virtual void ReadyToCopy(const NActors::TActorContext& ctx) = 0;
    virtual bool OnMessage(
        const NActors::TActorContext& ctx,
        TAutoPtr<NActors::IEventHandle>& ev) = 0;

    virtual void BeforeDie(
        const NActors::TActorContext& ctx,
        NProto::TError error) = 0;

    virtual ~ICopyRangeOwner() = default;
};

class TCopyRangeActorCommon: public NActors::TActorBootstrapped<TCopyRangeActorCommon>
{
    ICopyRangeOwner* Owner;
    const NActors::TActorId ActorToBlockRangeAndDrain;

protected:
    const TBlockRange64 Range;

public:
    TCopyRangeActorCommon(
            ICopyRangeOwner* owner,
            NActors::TActorId actorToBlockRangeAndDrain,
            TBlockRange64 range)
        : Owner(owner)
        , ActorToBlockRangeAndDrain(actorToBlockRangeAndDrain)
        , Range(range)
    {}

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void BlockRangeAndDrain(const NActors::TActorContext& ctx);

protected:
    void Done(const NActors::TActorContext& ctx, NProto::TError error);

private:
    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleBlockRangeAndDrain(
        const NPartition::TEvPartition::TEvBlockRangeAndDrainResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    STFUNC(StateWork);
};

}   // namespace NCloud::NBlockStore::NStorage
