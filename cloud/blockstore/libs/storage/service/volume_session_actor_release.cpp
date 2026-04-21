#include "volume_session_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TVolumeSessionActor::HandleReleaseVolumeToHiveRequest(
    const TEvServicePrivate::TEvReleaseVolumeToHiveRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto& diskId = VolumeInfo->DiskId;

    if (!StartVolumeActor) {
        auto response =
            std::make_unique<TEvServicePrivate::TEvReleaseVolumeToHiveResponse>(
                MakeError(
                    S_ALREADY,
                    TStringBuilder()
                        << "Volume " << diskId << " is not running locally"));
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(response->GetError()).c_str());
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (VolumeInfo->State != TVolumeInfo::STARTED) {
        auto response =
            std::make_unique<TEvServicePrivate::TEvReleaseVolumeToHiveResponse>(
                MakeError(
                    E_INVALID_STATE,
                    TStringBuilder()
                        << "Volume " << diskId << " is not is running state"));
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(response->GetError()).c_str());
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    VolumeInfo->State = TVolumeInfo::STOPPING;
    CurrentRequest = RELEASE_TO_HIVE_REQUEST;
    VolumeRequestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext);
}

void TVolumeSessionActor::HandleUnlockTabletResponse(
    const NCloud::NStorage::TEvHiveProxy::TEvUnlockTabletResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (VolumeRequestInfo && CurrentRequest == RELEASE_TO_HIVE_REQUEST) {
        const auto* msg = ev->Get();

        const auto& error = msg->GetError();
        if (FAILED(error.GetCode())) {
            LOG_ERROR(
                ctx,
                TBlockStoreComponents::SERVICE,
                "%s UnlockTablet failed with error: %s",
                LogTitle.GetWithTime().c_str(),
                FormatError(error).c_str());

            auto response = std::make_unique<
                TEvServicePrivate::TEvReleaseVolumeToHiveResponse>(msg->Error);
            NCloud::Reply(ctx, *VolumeRequestInfo, std::move(response));
            VolumeRequestInfo = {};
            CurrentRequest = NONE;
            return;
        }

        if (error.GetCode() == S_ALREADY) {
            auto response = std::make_unique<
                TEvServicePrivate::TEvReleaseVolumeToHiveResponse>(msg->Error);
            NCloud::Reply(ctx, *VolumeRequestInfo, std::move(response));
            VolumeRequestInfo = {};
            CurrentRequest = NONE;
        }

        LOG_INFO(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Unlocked volume for ReleaseVolumeToHive",
            LogTitle.GetWithTime().c_str());

        // The hive is to boot a remote volume tablet and demote the local one.
        // TEvReleaseVolumeToHiveRequest's processing continues in
        // TVolumeSessionActor::HandleStartVolumeActorStopped.
    }
}

}   // namespace NCloud::NBlockStore::NStorage
