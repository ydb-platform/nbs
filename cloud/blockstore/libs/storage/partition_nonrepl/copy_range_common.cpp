#include "copy_range_common.h"

#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/hfunc.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NPartition;

////////////////////////////////////////////////////////////////////////////////

void TCopyRangeActorCommon::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    if (ActorToBlockAndDrainRange) {
        BlockAndDrainRange(ctx);
        return;
    }

    Owner->ReadyToCopy(ctx);
}

void TCopyRangeActorCommon::BlockAndDrainRange(const TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        ActorToBlockAndDrainRange,
        std::make_unique<TEvPartition::TEvBlockAndDrainRangeRequest>(Range));
}

void TCopyRangeActorCommon::Done(const TActorContext& ctx, NProto::TError error)
{
    if (NeedToReleaseRange && ActorToBlockAndDrainRange) {
        NCloud::Send(
            ctx,
            ActorToBlockAndDrainRange,
            std::make_unique<TEvPartition::TEvReleaseRange>(Range));
    }

    Owner->BeforeDie(ctx, std::move(error));

    Die(ctx);
}

void TCopyRangeActorCommon::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Done(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

void TCopyRangeActorCommon::HandleBlockAndDrainRange(
    const TEvPartition::TEvBlockAndDrainRangeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        Done(ctx, msg->GetError());
        return;
    }
    NeedToReleaseRange = true;
    Owner->ReadyToCopy(ctx);
}

STFUNC(TCopyRangeActorCommon::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(
            TEvPartition::TEvBlockAndDrainRangeResponse,
            HandleBlockAndDrainRange);

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
