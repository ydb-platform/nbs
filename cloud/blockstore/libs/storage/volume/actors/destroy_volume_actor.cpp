#include "destroy_volume_actor.h"

#include "cloud/storage/core/libs/common/format.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TDestroyVolumeActor::TDestroyVolumeActor(
    TChildLogTitle logTitle,
    TRequestInfoPtr requestInfo,
    TString diskId)
    : LogTitle(std::move(logTitle))
    , RequestInfo(std::move(requestInfo))
    , DiskId(std::move(diskId))
{}

void TDestroyVolumeActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    DestroyDisk(ctx);
}

void TDestroyVolumeActor::ReplyAndDie(
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

void TDestroyVolumeActor::DestroyDisk(const TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Destroy volume DiskId=%s, try# %lu",
        LogTitle.GetWithTime().c_str(),
        DiskId.Quote().c_str(),
        ++TryCount);

    auto request = std::make_unique<TEvService::TEvDestroyVolumeRequest>();
    request->Record.SetDiskId(DiskId);
    request->Record.SetSync(false);
    NCloud::Send(ctx, MakeStorageServiceId(), std::move(request));
}

STFUNC(TDestroyVolumeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvService::TEvDestroyVolumeResponse,
            HandleDestroyVolumeResponse);
        HFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TDestroyVolumeActor::HandleDestroyVolumeResponse(
    const TEvService::TEvDestroyVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (HasError(ev->Get()->Record)) {
        auto timeout = DelayProvider.GetDelayAndIncrease();
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
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
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Volume destruction succeeded: DiskId=%s",
        LogTitle.GetWithTime().c_str(),
        DiskId.Quote().c_str());

    ReplyAndDie(ctx, MakeError(S_OK));
}

void TDestroyVolumeActor::HandleWakeup(
    const NActors::TEvents::TEvWakeup::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    DestroyDisk(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
