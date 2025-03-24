#include "volume_session_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TVolumeSessionActor::HandleStopVolumeRequest(
    const TEvServicePrivate::TEvStopVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{

    LOG_WARN(ctx, TBlockStoreComponents::SERVICE,
        "HandleStopVolumeRequest $#");

    const auto& diskId = VolumeInfo->DiskId;

    if (!StartVolumeActor) {
        auto response = std::make_unique<TEvServicePrivate::TEvStopVolumeResponse>(
            MakeError(S_ALREADY, TStringBuilder()
                << "Volume " << diskId << " is already stopped"));
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            FormatError(response->GetError()));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (VolumeInfo->State == TVolumeInfo::STOPPING) {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s is already being stopped",
            diskId.Quote().data());
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Shutting down start volume actor for volume %s",
        diskId.Quote().data());

    VolumeInfo->State = TVolumeInfo::STOPPING;
    CurrentRequest = STOP_REQUEST;
    NCloud::Send<TEvents::TEvPoisonPill>(ctx, StartVolumeActor);
}

void TVolumeSessionActor::HandleStartVolumeActorStopped(
    const TEvServicePrivate::TEvStartVolumeActorStopped::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Start volume actor stopped for volume %s",
        VolumeInfo->TabletId,
        VolumeInfo->DiskId.Quote().data());

    VolumeInfo->VolumeActor = {};
    StartVolumeActor = {};
    VolumeInfo->State = TVolumeInfo::INITIAL;
    VolumeInfo->Error.Clear();

    if (MountRequestActor) {
        if (CurrentRequest == START_REQUEST) {
            auto response = std::make_unique<TEvServicePrivate::TEvStartVolumeResponse>(
                msg->Error);
            NCloud::Send(ctx, MountRequestActor, std::move(response));
        } else {
            auto response = std::make_unique<TEvServicePrivate::TEvStopVolumeResponse>();
            NCloud::Send(ctx, MountRequestActor, std::move(response));
        }
    } else if (UnmountRequestActor) {
        auto response = std::make_unique<TEvServicePrivate::TEvStopVolumeResponse>();
        NCloud::Send(ctx, UnmountRequestActor, std::move(response));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
