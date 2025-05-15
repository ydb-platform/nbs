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

class TDestroyVolumeLinkActor final
    : public TActorBootstrapped<TDestroyVolumeLinkActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const TString LeaderDiskId;
    const TString FollowerDiskId;

    NProto::TVolume LeaderVolume;

public:
    TDestroyVolumeLinkActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString leaderDiskId,
        TString followerDiskId);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeVolume(const TActorContext& ctx);
    void RemoveLink(const NActors::TActorContext& ctx);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleUnlinkLeaderVolumeFromFollowerResponse(
        const TEvVolume::TEvUnlinkLeaderVolumeFromFollowerResponse::TPtr& ev,
        const TActorContext& ctx);

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
    DescribeVolume(ctx);
}

void TDestroyVolumeLinkActor::DescribeVolume(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(LeaderDiskId),
        0);
}

void TDestroyVolumeLinkActor::RemoveLink(const NActors::TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvVolume::TEvUnlinkLeaderVolumeFromFollowerRequest>(
            RequestInfo->CallContext);
    request->Record.SetDiskId(LeaderDiskId);
    request->Record.SetFollowerDiskId(FollowerDiskId);

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TDestroyVolumeLinkActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Volume %s: describe failed: %s",
            LeaderDiskId.Quote().data(),
            FormatError(error).data());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvDestroyVolumeLinkResponse>(error));
        return;
    }

    const auto& pathDescription = msg->PathDescription;
    const auto& volumeDescription =
        pathDescription.GetBlockStoreVolumeDescription();
    const auto& volumeConfig = volumeDescription.GetVolumeConfig();

    VolumeConfigToVolume(volumeConfig, LeaderVolume);
    LeaderVolume.SetTokenVersion(volumeDescription.GetTokenVersion());

    RemoveLink(ctx);
}

void TDestroyVolumeLinkActor::HandleUnlinkLeaderVolumeFromFollowerResponse(
    const TEvVolume::TEvUnlinkLeaderVolumeFromFollowerResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* message = ev->Get();
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
            TEvSSProxy::TEvDescribeVolumeResponse,
            HandleDescribeVolumeResponse);

        HFunc(
            TEvVolume::TEvUnlinkLeaderVolumeFromFollowerResponse,
            HandleUnlinkLeaderVolumeFromFollowerResponse);

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
