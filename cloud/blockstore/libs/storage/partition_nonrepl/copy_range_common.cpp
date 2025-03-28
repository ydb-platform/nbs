#include "copy_range_common.h"

#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/hfunc.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TCopyRangeActorCommon::Bootstrap(const NActors::TActorContext& ctx) {
    Become(&TThis::StateWork);

    if (ActorToBlockRangeAndDrain) {
        BlockRangeAndDrain(ctx);
        return;
    }

    Owner->ReadyToCopy(ctx);
}

void TCopyRangeActorCommon::BlockRangeAndDrain(
    const NActors::TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        ActorToBlockRangeAndDrain,
        std::make_unique<
            NPartition::TEvPartition::TEvBlockRangeAndDrainRequest>(Range));
}

void TCopyRangeActorCommon::Done(const NActors::TActorContext& ctx, NProto::TError error)
{
    if (ActorToBlockRangeAndDrain) {
        NCloud::Send(
            ctx,
            ActorToBlockRangeAndDrain,
            std::make_unique<NPartition::TEvPartition::TEvReleaseRange>(Range));
    }

    Owner->BeforeDie(ctx, std::move(error));

    Die(ctx);
}

void TCopyRangeActorCommon::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx) {
    Y_UNUSED(ev);
    Done(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

void TCopyRangeActorCommon::HandleBlockRangeAndDrain(
    const NPartition::TEvPartition::TEvBlockRangeAndDrainResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    Owner->ReadyToCopy(ctx);
}

STFUNC(TCopyRangeActorCommon::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(
            NPartition::TEvPartition::TEvBlockRangeAndDrainResponse,
            HandleBlockRangeAndDrain);

        default:
            if (!Owner->OnMessage(this->ActorContext(), ev)) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION_WORKER);
            }
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
