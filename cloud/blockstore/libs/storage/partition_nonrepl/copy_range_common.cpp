#include "copy_range_common.h"

#include <cloud/storage/core/libs/actors/helpers.h>


#include <contrib/ydb/library/actors/core/hfunc.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TCopyRangeActorCommon::Bootstrap(const NActors::TActorContext& ctx) {
    Become(&TThis::StateWork);

    if (VolumeActorId) {
        GetVolumeRequestId(ctx);
        return;
    }

    Owner->ReadyToCopy(ctx, 0);
}

void TCopyRangeActorCommon::GetVolumeRequestId(
    const NActors::TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        VolumeActorId,
        std::make_unique<TEvVolumePrivate::TEvTakeVolumeRequestIdRequest>());
}

void TCopyRangeActorCommon::Done(const NActors::TActorContext& ctx, NProto::TError error)
{
    Owner->BeforeDie(ctx, std::move(error));

    Die(ctx);
}

void TCopyRangeActorCommon::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx) {
    Y_UNUSED(ev);
    Done(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

void TCopyRangeActorCommon::HandleVolumeRequestId(
    const TEvVolumePrivate::TEvTakeVolumeRequestIdResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        Done(ctx, msg->GetError());
        return;
    }

    Owner->ReadyToCopy(ctx, msg->ReqId);
}

STFUNC(TCopyRangeActorCommon::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(
            TEvVolumePrivate::TEvTakeVolumeRequestIdResponse,
            HandleVolumeRequestId);

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
