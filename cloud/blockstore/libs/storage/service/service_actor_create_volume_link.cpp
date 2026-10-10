#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCreateVolumeLinkActor final
    : public TActorBootstrapped<TCreateVolumeLinkActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString LeaderDiskId;
    const TString FollowerDiskId;
    const TString LeaderShardId;
    const TString FollowerShardId;

public:
    TCreateVolumeLinkActor(TRequestInfoPtr requestInfo, TString leaderDiskId,
                           TString followerDiskId, TString leaderShardId,
                           TString followerShardId);

    void Bootstrap(const TActorContext& ctx);

private:
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
    TRequestInfoPtr requestInfo, TString leaderDiskId, TString followerDiskId,
    TString leaderShardId, TString followerShardId)
    : RequestInfo(std::move(requestInfo))
    , LeaderDiskId(std::move(leaderDiskId))
    , FollowerDiskId(std::move(followerDiskId))
    , LeaderShardId(std::move(leaderShardId))
    , FollowerShardId(std::move(followerShardId))
{}

void TCreateVolumeLinkActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    auto request =
        std::make_unique<TEvVolume::TEvLinkLeaderVolumeToFollowerRequest>(
            RequestInfo->CallContext);
    request->Record.SetDiskId(LeaderDiskId);
    request->Record.SetFollowerDiskId(FollowerDiskId);
    request->Record.SetLeaderShardId(LeaderShardId);
    request->Record.SetFollowerShardId(FollowerShardId);
    request->Record.MutableHeaders()->SetShardId(LeaderShardId);
    request->Record.MutableHeaders()->SetExactDiskIdMatch(true);

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
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
            TEvVolume::TEvLinkLeaderVolumeToFollowerResponse,
            HandleLinkLeaderVolumeToFollowerResponse);

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

void TServiceActor::HandleCreateVolumeLink(
    const TEvService::TEvCreateVolumeLinkRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    const auto& request = msg->Record;

    const auto leaderDirectory =
        Config->GetSchemeShardDirForShard(request.GetLeaderShardId());
    const auto followerDirectory =
        Config->GetSchemeShardDirForShard(request.GetFollowerShardId());
    if (!leaderDirectory || !followerDirectory) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(
                MakeError(E_ARGUMENT, "Unknown or invalid storage shard")));
        return;
    }
    if (*leaderDirectory != *followerDirectory &&
        (request.GetLeaderShardId().empty() ||
         request.GetFollowerShardId().empty()))
    {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(MakeError(
                E_ARGUMENT,
                "Both shard identifiers are required for a cross-shard link")));
        return;
    }

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
        ctx, std::move(requestInfo), request.GetLeaderDiskId(),
        request.GetFollowerDiskId(), request.GetLeaderShardId(),
        request.GetFollowerShardId());
}

}   // namespace NCloud::NBlockStore::NStorage
