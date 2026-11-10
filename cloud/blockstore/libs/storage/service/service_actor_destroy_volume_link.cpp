#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Cancel the link on the follower before unlinking the source. The follower
// may already own the disk while the source is waiting for its acknowledgment.
// If the follower rejects cancellation, keep the source link so source I/O
// stays blocked. Unlink the source only after cancellation succeeds or the
// follower no longer exists.
class TDestroyVolumeLinkActor final
    : public TActorBootstrapped<TDestroyVolumeLinkActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigConstPtr Config;
    const TString LeaderDiskId;
    const TString LeaderCellId;
    const TString FollowerDiskId;
    const TString FollowerCellId;

    NProto::TError FollowerUnlinkResult;

public:
    TDestroyVolumeLinkActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigConstPtr config,
        TString leaderDiskId,
        TString leaderCellId,
        TString followerDiskId,
        TString followerCellId);

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
        TStorageConfigConstPtr config,
        TString leaderDiskId,
        TString leaderCellId,
        TString followerDiskId,
        TString followerCellId)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , LeaderDiskId(std::move(leaderDiskId))
    , LeaderCellId(std::move(leaderCellId))
    , FollowerDiskId(std::move(followerDiskId))
    , FollowerCellId(std::move(followerCellId))
{}

void TDestroyVolumeLinkActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    RemoveLinkOnFollower(ctx);
}

void TDestroyVolumeLinkActor::UnlinkLeaderVolumeFromFollower(
    const NActors::TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvVolume::TEvUnlinkLeaderVolumeFromFollowerRequest>(
            RequestInfo->CallContext);
    request->Record.SetDiskId(LeaderDiskId);
    request->Record.SetLeaderCellId(LeaderCellId);
    request->Record.SetFollowerDiskId(FollowerDiskId);
    request->Record.SetFollowerCellId(FollowerCellId);

    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
}

void TDestroyVolumeLinkActor::RemoveLinkOnFollower(
    const NActors::TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
    request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
    request->Record.SetDiskId(FollowerDiskId);
    request->Record.SetFollowerCellId(FollowerCellId);
    request->Record.SetLeaderDiskId(LeaderDiskId);
    request->Record.SetLeaderCellId(LeaderCellId);
    request->Record.SetAction(NProto::ELinkAction::LINK_ACTION_DESTROY);

    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
}

void TDestroyVolumeLinkActor::HandleUnlinkLeaderVolumeFromFollowerResponse(
    const TEvVolume::TEvUnlinkLeaderVolumeFromFollowerResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* message = ev->Get();
    auto error = message->GetError();

    if (IsNotFoundSchemeShardError(error)) {
        error = FollowerUnlinkResult;
    }

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Remove link on leader %s failed: %s",
            LeaderDiskId.Quote().data(),
            FormatError(error).data());
    }

    ReplyAndDie(
        ctx,
        std::make_unique<TEvService::TEvDestroyVolumeLinkResponse>(
            std::move(error)));
}

void TDestroyVolumeLinkActor::HandleLinkRemovedOnFollower(
    const TEvVolume::TEvUpdateLinkOnFollowerResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* message = ev->Get();
    const auto& error = message->GetError();
    FollowerUnlinkResult = error;

    if (HasError(error) && !IsNotFoundSchemeShardError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Remove link on follower %s failed: %s",
            FollowerDiskId.Quote().data(),
            FormatError(error).data());
        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvDestroyVolumeLinkResponse>(error));
        return;
    }

    UnlinkLeaderVolumeFromFollower(ctx);
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

    const auto& followerCellId =
        request.GetFollowerCellId() ? request.GetFollowerCellId() : CellId;

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "DestroyVolumeLink leader: %s/%s, follower: %s/%s",
        CellId.Quote().data(),
        request.GetLeaderDiskId().Quote().data(),
        followerCellId.Quote().data(),
        request.GetFollowerDiskId().Quote().data());

    NCloud::Register<TDestroyVolumeLinkActor>(
        ctx,
        std::move(requestInfo),
        Config,
        request.GetLeaderDiskId(),
        CellId,
        request.GetFollowerDiskId(),
        followerCellId);
}

}   // namespace NCloud::NBlockStore::NStorage
