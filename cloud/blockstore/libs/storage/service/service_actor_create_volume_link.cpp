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

enum EDescribeKind : ui64
{
    DESCRIBE_KIND_LEADER = 0,
    DESCRIBE_KIND_FOLLOWER = 1
};

class TCreateVolumeLinkActor final
    : public TActorBootstrapped<TCreateVolumeLinkActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const TString LeaderDiskId;
    const TString FollowerDiskId;

    NProto::TVolume LeaderVolume;
    NProto::TVolume FollowerVolume;

public:
    TCreateVolumeLinkActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString leaderDiskId,
        TString followerDiskId);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeVolume(const TActorContext& ctx);
    void LinkVolumes(const TActorContext& ctx);
    void AddLink(const NActors::TActorContext& ctx);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleLinkLeaderVolumeToFollowerResponse(
        const TEvVolume::TEvLinkLeaderVolumeToFollowerResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvCreateVolumeLinkResponse> response);

private:
    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

TCreateVolumeLinkActor::TCreateVolumeLinkActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString leaderDiskId,
        TString followerDiskId)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , LeaderDiskId(std::move(leaderDiskId))
    , FollowerDiskId(std::move(followerDiskId))
{}

void TCreateVolumeLinkActor::Bootstrap(const TActorContext& ctx)
{
    DescribeVolume(ctx);
}

void TCreateVolumeLinkActor::DescribeVolume(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(LeaderDiskId),
        DESCRIBE_KIND_LEADER);
    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(FollowerDiskId),
        DESCRIBE_KIND_FOLLOWER);
}

void TCreateVolumeLinkActor::LinkVolumes(const TActorContext& ctx)
{
    if (!LeaderVolume.GetDiskId() || !FollowerVolume.GetDiskId()) {
        return;
    }

    const auto sourceSize =
        LeaderVolume.GetBlocksCount() * LeaderVolume.GetBlockSize();
    const auto targetSize =
        FollowerVolume.GetBlocksCount() * FollowerVolume.GetBlockSize();

    if (sourceSize > targetSize) {
        auto errorMessage =
            TStringBuilder()
            << "The size of the leader disk " << LeaderDiskId.Quote()
            << " is larger than the follower disk " << FollowerDiskId.Quote()
            << " " << sourceSize << " > " << targetSize;
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE, errorMessage.c_str());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(
                MakeError(E_INVALID_STATE, errorMessage)));
        return;
    }

    AddLink(ctx);
}

void TCreateVolumeLinkActor::AddLink(const NActors::TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvVolume::TEvLinkLeaderVolumeToFollowerRequest>(
            RequestInfo->CallContext);
    request->Record.SetDiskId(LeaderDiskId);
    request->Record.SetFollowerDiskId(FollowerDiskId);

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TCreateVolumeLinkActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& diskId =
        (ev->Cookie == DESCRIBE_KIND_LEADER) ? LeaderDiskId : FollowerDiskId;
    auto& volume =
        (ev->Cookie == DESCRIBE_KIND_LEADER) ? LeaderVolume : FollowerVolume;

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Volume %s: describe failed: %s",
            diskId.Quote().data(),
            FormatError(error).data());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(error));
        return;
    }

    const auto& pathDescription = msg->PathDescription;
    const auto& volumeDescription =
        pathDescription.GetBlockStoreVolumeDescription();
    const auto& volumeConfig = volumeDescription.GetVolumeConfig();

    VolumeConfigToVolume(volumeConfig, volume);
    volume.SetTokenVersion(volumeDescription.GetTokenVersion());

    LinkVolumes(ctx);
}

void TCreateVolumeLinkActor::HandleLinkLeaderVolumeToFollowerResponse(
    const TEvVolume::TEvLinkLeaderVolumeToFollowerResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* message = ev->Get();
    ReplyAndDie(
        ctx,
        std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(
            message->GetError()));
}

void TCreateVolumeLinkActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvCreateVolumeLinkResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCreateVolumeLinkActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvSSProxy::TEvDescribeVolumeResponse,
            HandleDescribeVolumeResponse);

        HFunc(
            TEvVolume::TEvLinkLeaderVolumeToFollowerResponse,
            HandleLinkLeaderVolumeToFollowerResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleCreateVolumeLink(
    const TEvService::TEvCreateVolumeLinkRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    const auto& request = msg->Record;

    if (request.GetLeaderDiskId().empty()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Empty Leader DiskId in CreateVolumeLink");

        auto response =
            std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(
                MakeError(E_ARGUMENT, "Leader volume name cannot be empty"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
    if (request.GetFollowerDiskId().empty()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Empty Follower DiskId in CreateVolumeLink");

        auto response =
            std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(
                MakeError(E_ARGUMENT, "Follower volume name cannot be empty"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
    if (request.GetLeaderDiskId() == request.GetFollowerDiskId()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Source and Target should be different in CreateVolumeLink");

        auto response =
            std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(MakeError(
                E_ARGUMENT,
                "Leader and Follower should be different"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "CreateVolumeLink leader: %s, follower: %s",
        request.GetLeaderDiskId().Quote().data(),
        request.GetFollowerDiskId().Quote().data());

    NCloud::Register<TCreateVolumeLinkActor>(
        ctx,
        std::move(requestInfo),
        Config,
        request.GetLeaderDiskId(),
        request.GetFollowerDiskId());
}

}   // namespace NCloud::NBlockStore::NStorage
