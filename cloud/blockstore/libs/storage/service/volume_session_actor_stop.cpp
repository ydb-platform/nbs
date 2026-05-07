#include "volume_session_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TVolumeSessionActor::HandleStopVolumeRequest(
    const TEvServicePrivate::TEvStopVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& diskId = VolumeInfo->DiskId;

    if (!StartVolumeActor) {
        auto response = std::make_unique<TEvServicePrivate::TEvStopVolumeResponse>(
            MakeError(S_ALREADY, TStringBuilder()
                << "Volume " << diskId << " is already stopped"));
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(response->GetError()).c_str());
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (VolumeInfo->State == TVolumeInfo::STOPPING) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Volume is already being stopped",
            LogTitle.GetWithTime().c_str());
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Shutting down StartVolumeActor",
        LogTitle.GetWithTime().c_str());

    VolumeInfo->State = TVolumeInfo::STOPPING;
    CurrentRequest = STOP_REQUEST;
    VolumeRequestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext);
    NCloud::Send<TEvents::TEvPoisonPill>(ctx, StartVolumeActor);
}

void TVolumeSessionActor::HandleStartVolumeActorStopped(
    const TEvServicePrivate::TEvStartVolumeActorStopped::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s StartVolumeActor stopped",
        LogTitle.GetWithTime().c_str());

    VolumeInfo->VolumeActor = {};
    StartVolumeActor = {};
    VolumeInfo->State = TVolumeInfo::INITIAL;
    VolumeInfo->Error.Clear();

    if (VolumeRequestInfo) {
        if (CurrentRequest == START_REQUEST) {
            NCloud::Reply(
                ctx,
                *VolumeRequestInfo,
                std::make_unique<TEvServicePrivate::TEvStartVolumeResponse>(
                    msg->Error));
        } else {
            NCloud::Reply(
                ctx,
                *VolumeRequestInfo,
                std::make_unique<TEvServicePrivate::TEvStopVolumeResponse>());
        }
        CurrentRequest = NONE;
        VolumeRequestInfo = {};
    }
}

}   // namespace NCloud::NBlockStore::NStorage
