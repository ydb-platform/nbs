#include "reliable_destroy_volume_actor.h"

#include <cloud/storage/core/libs/common/format.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TReliableDestroyVolumeActor::TReliableDestroyVolumeActor(
    TChildLogTitle logTitle,
    TRequestInfoPtr requestInfo,
    TString diskId)
    : LogTitle(std::move(logTitle))
    , RequestInfo(std::move(requestInfo))
    , DiskId(std::move(diskId))
{}

void TReliableDestroyVolumeActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    DestroyDisk(ctx);
}

void TReliableDestroyVolumeActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    NCloud::Reply(
        ctx,
        *RequestInfo,
        std::make_unique<TEvService::TEvDestroyVolumeResponse>(
            std::move(error)));
    Die(ctx);
}

void TReliableDestroyVolumeActor::DestroyDisk(const TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Destroy volume DiskId=%s, try# %lu",
        LogTitle.GetWithTime().c_str(),
        DiskId.Quote().c_str(),
        ++TryCount);

    auto request = std::make_unique<TEvService::TEvDestroyVolumeRequest>();
    request->Record.SetDiskId(DiskId);
    request->Record.SetUseStrictDiskId(true);
    NCloud::Send(ctx, MakeStorageServiceId(), std::move(request));
}

STFUNC(TReliableDestroyVolumeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvService::TEvDestroyVolumeResponse,
            HandleDestroyVolumeResponse);
        HFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TReliableDestroyVolumeActor::HandleDestroyVolumeResponse(
    const TEvService::TEvDestroyVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (HasError(ev->Get()->Record)) {
        auto timeout = DelayProvider.GetDelayAndIncrease();
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Volume destruction failed: DiskId=%s, Error=%s, Will retry "
            "after %s",
            LogTitle.GetWithTime().c_str(),
            DiskId.Quote().c_str(),
            FormatError(ev->Get()->Record.GetError()).c_str(),
            FormatDuration(timeout).c_str());

        ctx.Schedule(timeout, new NActors::TEvents::TEvWakeup());
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Volume destruction succeeded: DiskId=%s",
        LogTitle.GetWithTime().c_str(),
        DiskId.Quote().c_str());

    ReplyAndDie(ctx, MakeError(S_OK));
}

void TReliableDestroyVolumeActor::HandleWakeup(
    const NActors::TEvents::TEvWakeup::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    DestroyDisk(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
