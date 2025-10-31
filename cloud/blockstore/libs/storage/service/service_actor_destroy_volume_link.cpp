#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDestroyVolumeLinkActor final
    : public TActorBootstrapped<TDestroyVolumeLinkActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const TString LeaderDiskId;
    const TString FollowerDiskId;

public:
    TDestroyVolumeLinkActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString leaderDiskId,
        TString followerDiskId);

    void Bootstrap(const TActorContext& ctx);

private:
    void UnlinkLeaderVolumeFromFollower(const NActors::TActorContext& ctx);
    void RemoveLinkOnFollower(const NActors::TActorContext& ctx);

    void HandleUnlinkLeaderVolumeFromFollowerResponse(
        const TEvVolume::TEvUnlinkLeaderVolumeFromFollowerResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleLinkRemovedOnFollower(
        const TEvVolume::TEvUpdateLinkOnFollowerResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvDestroyVolumeLinkResponse> response);

private:
    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

TDestroyVolumeLinkActor::TDestroyVolumeLinkActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString leaderDiskId,
        TString followerDiskId)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , LeaderDiskId(std::move(leaderDiskId))
    , FollowerDiskId(std::move(followerDiskId))
{}

void TDestroyVolumeLinkActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    UnlinkLeaderVolumeFromFollower(ctx);
}

void TDestroyVolumeLinkActor::UnlinkLeaderVolumeFromFollower(
    const NActors::TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvVolume::TEvUnlinkLeaderVolumeFromFollowerRequest>(
            RequestInfo->CallContext);
    request->Record.SetDiskId(LeaderDiskId);
    request->Record.SetFollowerDiskId(FollowerDiskId);

    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
}

void TDestroyVolumeLinkActor::RemoveLinkOnFollower(
    const NActors::TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
    request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
    request->Record.SetDiskId(FollowerDiskId);
    request->Record.SetFollowerShardId({});
    request->Record.SetLeaderDiskId(LeaderDiskId);
    request->Record.SetLeaderShardId({});
    request->Record.SetAction(NProto::ELinkAction::LINK_ACTION_DESTROY);

    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
}

void TDestroyVolumeLinkActor::HandleUnlinkLeaderVolumeFromFollowerResponse(
    const TEvVolume::TEvUnlinkLeaderVolumeFromFollowerResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* message = ev->Get();
    const auto& error = message->GetError();

    if (FAILED(error.GetCode())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Remove link on leader %s failed: %s",
            LeaderDiskId.Quote().data(),
            FormatError(error).data());

        if (FACILITY_FROM_CODE(error.GetCode()) == FACILITY_SCHEMESHARD) {
            auto status = static_cast<NKikimrScheme::EStatus>(
                STATUS_FROM_CODE(error.GetCode()));
            // TODO: return E_NOT_FOUND instead of StatusPathDoesNotExist
            if (status == NKikimrScheme::StatusPathDoesNotExist) {
                // Leader disk not found. Try to remove on follower.
                RemoveLinkOnFollower(ctx);
                return;
            }
        }
    }

    ReplyAndDie(
        ctx,
        std::make_unique<TEvService::TEvDestroyVolumeLinkResponse>(
            message->GetError()));
}

void TDestroyVolumeLinkActor::HandleLinkRemovedOnFollower(
    const TEvVolume::TEvUpdateLinkOnFollowerResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* message = ev->Get();
    const auto& error = message->GetError();

    if (FAILED(error.GetCode())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Remove link on follower %s failed: %s",
            FollowerDiskId.Quote().data(),
            FormatError(error).data());
    }

    ReplyAndDie(
        ctx,
        std::make_unique<TEvService::TEvDestroyVolumeLinkResponse>(
            message->GetError()));
}

void TDestroyVolumeLinkActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvDestroyVolumeLinkResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDestroyVolumeLinkActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvVolume::TEvUnlinkLeaderVolumeFromFollowerResponse,
            HandleUnlinkLeaderVolumeFromFollowerResponse);

        HFunc(
            TEvVolume::TEvUpdateLinkOnFollowerResponse,
            HandleLinkRemovedOnFollower);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleDestroyVolumeLink(
    const TEvService::TEvDestroyVolumeLinkRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    const auto& request = msg->Record;

    if (request.GetFollowerDiskId().empty()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Empty Leader DiskId in DestroyVolumeLink");

        auto response =
            std::make_unique<TEvService::TEvDestroyVolumeLinkResponse>(
                MakeError(E_ARGUMENT, "Leader volume name cannot be empty"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
    if (request.GetFollowerDiskId().empty()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Empty Follower DiskId in DestroyVolumeLink");

        auto response =
            std::make_unique<TEvService::TEvDestroyVolumeLinkResponse>(
                MakeError(E_ARGUMENT, "Follower volume name cannot be empty"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "DestroyVolumeLink leader: %s, follower: %s",
        request.GetLeaderDiskId().Quote().data(),
        request.GetFollowerDiskId().Quote().data());

    NCloud::Register<TDestroyVolumeLinkActor>(
        ctx,
        std::move(requestInfo),
        Config,
        request.GetLeaderDiskId(),
        request.GetFollowerDiskId());
}

}   // namespace NCloud::NBlockStore::NStorage
